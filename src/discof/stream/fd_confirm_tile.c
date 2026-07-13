#include "fd_confirm_tile.h"
#include "fd_stream_msg.h"

#include "../../disco/fd_disco_base.h"
#include "../../disco/fd_txn_p.h"
#include "../../disco/metrics/fd_metrics.h"
#include "../../disco/topo/fd_topo.h"
#include "../../discof/replay/fd_replay_tile.h"
#include "../../choreo/tower/fd_tower_serdes.h"
#include "../../flamenco/runtime/program/vote/fd_vote_codec.h"
#include "../../flamenco/runtime/fd_bank.h"
#include "../../flamenco/stakes/fd_top_votes.h"
#include "../../util/pod/fd_pod.h"
#include "../../ballet/txn/fd_txn.h"
#include "../../flamenco/gossip/fd_gossip_message.h"

#include "generated/fd_stream_tile_seccomp.h"

/* Confirm tile — mirrors Agave's cluster_info_vote_listener.

   Architecture (matching Agave):
   - Voter stake map: pubkey → stake (loaded from STAKE_TABLE, static within epoch)
   - Per-slot trackers: each slot has a running stake counter + dedup bitset
   - O(1) per gossip vote: lookup stake, check dedup, add stake, check threshold
   - Fires CONFIRMED the instant 2/3 stake is crossed

   Inputs:
     gossip_out — gossip vote CRDs (fast path, pre-replay)
     stake_out  — STAKE_TABLE batches from replay (voter stake mapping)

   Output:
     confirm_out — CONFIRMED slot signals */

#define CONFIRM_IN_GOSSIP (0)
#define CONFIRM_IN_REPLAY (1)

#define CONFIRM_THRESHOLD_NUM (2UL)
#define CONFIRM_THRESHOLD_DEN (3UL)

/* Voter stake map — maps vote_account pubkey → delegated stake.
   Open addressing, linear probing.  Loaded from STAKE_TABLE. */
#define VOTER_MAP_CAP  (4096UL)
#define VOTER_MAP_MASK (VOTER_MAP_CAP - 1UL)

typedef struct {
  ulong key;       /* first 8 bytes of pubkey, 0 = empty */
  ulong stake;
  uchar pubkey[32];
} voter_entry_t;

/* Per-slot vote tracker — matches Agave's SlotVoteTracker + VoteStakeTracker.
   Uses a bitset for dedup (indexed by voter map position).
   Only need to track recent slots (ring buffer of 64). */
#define SLOT_RING_CNT  (64UL)
#define SLOT_RING_MASK (SLOT_RING_CNT - 1UL)
#define DEDUP_WORDS    (VOTER_MAP_CAP / 64UL)  /* 64 ulong = 4096 bits */

typedef struct {
  ulong slot;
  ulong stake;           /* running total of accumulated stake */
  int   confirmed;
  ulong voted[ DEDUP_WORDS ]; /* bitset: bit i = voters[i] already counted */
  ulong gossip_cnt;      /* number of gossip votes counted */
  ulong replay_cnt;      /* number of replay votes counted */
  long  first_vote_ts;   /* wallclock of first vote */
  long  first_gossip_ts; /* wallclock of first gossip vote */
  long  first_replay_ts; /* wallclock of first replay vote */
} slot_tracker_t;

typedef struct {
  fd_wksp_t * mem;
  ulong       chunk0;
  ulong       wmark;
  ulong       mtu;
  int         link_type;
} confirm_in_ctx_t;

struct fd_confirm_tile {
  confirm_in_ctx_t in[ 16UL ];
  ulong            in_cnt;

  struct {
    ulong       idx;
    fd_wksp_t * mem;
    ulong       chunk0;
    ulong       wmark;
    ulong       chunk;
  } out;

  /* Direct bank access for stake table (no inter-tile link needed) */
  fd_banks_t *  banks;
  ulong         last_refresh_slot;  /* slot of last voter map refresh */
  ulong         confirmed_high;     /* highest confirmed slot — prevents stale confirmations */

  /* Voter stake map */
  voter_entry_t voters[ VOTER_MAP_CAP ];
  ulong         voter_cnt;
  ulong         total_stake;

  /* Per-slot trackers (ring buffer) */
  slot_tracker_t trackers[ SLOT_RING_CNT ];

  /* Deser scratch */
  fd_compact_tower_sync_serde_t tower_serde[1];

  /* Staging buffer */
  uchar msg_buf[ USHORT_MAX ];
  ulong last_sig;
  int   last_link_type;
  int   frag_valid;  /* set by during_frag, checked by after_frag */
};
typedef struct fd_confirm_tile fd_confirm_tile_t;

FD_FN_CONST static inline ulong
scratch_align( void ) { return 128UL; }

FD_FN_PURE static inline ulong
scratch_footprint( fd_topo_tile_t const * tile ) {
  (void)tile;
  ulong l = FD_LAYOUT_INIT;
  l = FD_LAYOUT_APPEND( l, alignof(fd_confirm_tile_t), sizeof(fd_confirm_tile_t) );
  return FD_LAYOUT_FINI( l, scratch_align() );
}

static inline void metrics_write( fd_confirm_tile_t * ctx ) { (void)ctx; }

/* Diagnostic counters */
static ulong diag_gossip_vote_cnt;
static ulong diag_gossip_vote_matched;
static ulong diag_gossip_vote_no_stake;
static ulong diag_gossip_vote_dedup;
/* diag_stake_batch_cnt removed — no longer using stake_out link */
static ulong diag_confirmed_cnt;

/* ---- Voter stake map ---- */

/* Lookup voter stake and return map index.  O(1) amortized. */
static inline ulong
voter_lookup( fd_confirm_tile_t * ctx, uchar const * pubkey, ulong * out_idx ) {
  ulong key = fd_ulong_load_8( pubkey );
  if( FD_UNLIKELY( !key ) ) key = 1UL;
  ulong idx = key & VOTER_MAP_MASK;
  for( ulong i=0; i<64UL; i++ ) {
    ulong probe = (idx + i) & VOTER_MAP_MASK;
    if( FD_UNLIKELY( ctx->voters[probe].key == 0 ) ) { *out_idx = ULONG_MAX; return 0; }
    if( FD_LIKELY( ctx->voters[probe].key == key &&
                   !memcmp( ctx->voters[probe].pubkey, pubkey, 32UL ) ) ) {
      *out_idx = probe;
      return ctx->voters[probe].stake;
    }
  }
  *out_idx = ULONG_MAX;
  return 0;
}

/* Upsert voter in stake map.  Only updates stake, no vote tracking. */
static inline void
voter_upsert( fd_confirm_tile_t * ctx, uchar const * pubkey, ulong stake ) {
  ulong key = fd_ulong_load_8( pubkey );
  if( FD_UNLIKELY( !key ) ) key = 1UL;
  ulong idx = key & VOTER_MAP_MASK;
  for( ulong i=0; i<VOTER_MAP_CAP; i++ ) {
    ulong probe = (idx + i) & VOTER_MAP_MASK;
    if( FD_LIKELY( ctx->voters[probe].key == 0 ) ) {
      ctx->voters[probe].key   = key;
      ctx->voters[probe].stake = stake;
      fd_memcpy( ctx->voters[probe].pubkey, pubkey, 32UL );
      ctx->voter_cnt++;
      return;
    }
    if( FD_LIKELY( ctx->voters[probe].key == key &&
                   !memcmp( ctx->voters[probe].pubkey, pubkey, 32UL ) ) ) {
      ctx->voters[probe].stake = stake;
      return;
    }
  }
}

/* ---- Per-slot tracker ---- */

static inline slot_tracker_t *
get_tracker( fd_confirm_tile_t * ctx, ulong slot ) {
  ulong idx = slot & SLOT_RING_MASK;
  slot_tracker_t * t = &ctx->trackers[idx];
  if( FD_UNLIKELY( t->slot != slot ) ) {
    t->slot          = slot;
    t->stake         = 0;
    t->confirmed     = 0;
    t->gossip_cnt    = 0;
    t->replay_cnt    = 0;
    t->first_vote_ts = 0;
    t->first_gossip_ts = 0;
    t->first_replay_ts = 0;
    fd_memset( t->voted, 0, sizeof(t->voted) );
  }
  return t;
}

/* ---- Core: emit CONFIRMED ---- */

static inline void
emit_confirmed( fd_confirm_tile_t * ctx,
                fd_stem_context_t * stem,
                ulong               slot ) {
  if( FD_UNLIKELY( ctx->out.idx == ULONG_MAX ) ) return;
  fd_stream_slot_msg_t * msg = (fd_stream_slot_msg_t *)fd_chunk_to_laddr( ctx->out.mem, ctx->out.chunk );
  msg->msg_type    = FD_STREAM_MSG_TYPE_SLOT;
  msg->status      = FD_STREAM_SLOT_CONFIRMED;
  memset( msg->_pad, 0, sizeof(msg->_pad) );
  msg->slot        = slot;
  msg->parent_slot = 0;
  fd_stem_publish( stem, ctx->out.idx, 0UL, ctx->out.chunk, sizeof(fd_stream_slot_msg_t), 0UL, 0UL, fd_frag_meta_ts_comp( fd_tickcount() ) );
  ctx->out.chunk = fd_dcache_compact_next( ctx->out.chunk, sizeof(fd_stream_slot_msg_t), ctx->out.chunk0, ctx->out.wmark );
}

/* ---- Core: count a vote — O(1) per call, matches Agave exactly ---- */

static inline void
count_vote( fd_confirm_tile_t * ctx,
            fd_stem_context_t * stem,
            ulong               voted_slot,
            uchar const *       vote_acc,
            int                 is_gossip ) {
  diag_gossip_vote_cnt++;
  static ulong last_voted_slot = 0;
  if( voted_slot > last_voted_slot ) last_voted_slot = voted_slot;

  /* 1. Lookup voter stake */
  ulong map_idx = ULONG_MAX;
  ulong stake = voter_lookup( ctx, vote_acc, &map_idx );
  if( FD_UNLIKELY( !stake || map_idx == ULONG_MAX ) ) { diag_gossip_vote_no_stake++; return; }
  diag_gossip_vote_matched++;

  /* 2. Get/create per-slot tracker */
  slot_tracker_t * t = get_tracker( ctx, voted_slot );
  if( FD_UNLIKELY( t->confirmed ) ) return;

  /* Track first vote timestamp for C-P gap analysis */
  long now_wc = fd_log_wallclock();
  if( FD_UNLIKELY( t->first_vote_ts == 0 ) ) {
    t->first_vote_ts = now_wc;
  }

  /* 3. Dedup check — one bit per voter map index */
  ulong word = map_idx >> 6;
  ulong bit  = 1UL << (map_idx & 63UL);
  if( FD_UNLIKELY( t->voted[word] & bit ) ) { diag_gossip_vote_dedup++; return; } /* already counted */
  t->voted[word] |= bit;

  /* Track source */
  if( is_gossip ) {
    t->gossip_cnt++;
    if( FD_UNLIKELY( !t->first_gossip_ts ) ) t->first_gossip_ts = now_wc;
  } else {
    t->replay_cnt++;
    if( FD_UNLIKELY( !t->first_replay_ts ) ) t->first_replay_ts = now_wc;
  }

  /* 4. Accumulate stake and check threshold */
  t->stake += stake;
  if( FD_UNLIKELY( ctx->total_stake > 0 &&
                    t->stake * CONFIRM_THRESHOLD_DEN >
                    ctx->total_stake * CONFIRM_THRESHOLD_NUM ) ) {
    t->confirmed = 1;
    diag_confirmed_cnt++;

    /* Only emit if this slot advances the confirmed frontier.
       Tower-wide vote counting pushes many old slots over threshold
       simultaneously; only the advancing frontier should emit. */
    if( FD_UNLIKELY( voted_slot <= ctx->confirmed_high ) ) {
      return; /* already confirmed or behind frontier */
    }
    ctx->confirmed_high = voted_slot;

    long confirm_latency_us = (now_wc - t->first_vote_ts) / 1000;
    long gossip_lead_us = 0;
    if( t->first_gossip_ts && t->first_replay_ts ) {
      gossip_lead_us = (t->first_replay_ts - t->first_gossip_ts) / 1000;
    }
    FD_LOG_WARNING(( "confm: CONFIRMED slot=%lu ratio=%.4f g=%lu r=%lu lat=%ldus g_lead=%ldus g1=%ld r1=%ld",
                     voted_slot,
                     (double)t->stake/(double)ctx->total_stake,
                     t->gossip_cnt, t->replay_cnt,
                     confirm_latency_us, gossip_lead_us,
                     t->first_gossip_ts ? (t->first_gossip_ts - t->first_vote_ts) / 1000 : -1L,
                     t->first_replay_ts ? (t->first_replay_ts - t->first_vote_ts) / 1000 : -1L ));
    emit_confirmed( ctx, stem, voted_slot );
  }
}

/* ---- Vote transaction parsing ---- */

static void
process_vote_txn( fd_confirm_tile_t * ctx,
                  fd_stem_context_t * stem,
                  fd_txn_t const *    txn,
                  uchar const *       payload,
                  int                 is_gossip ) {
  if( FD_UNLIKELY( !fd_txn_is_simple_vote_transaction( txn, payload ) ) ) return;

  fd_txn_instr_t const * instr = &txn->instr[0];
  uchar const * data = payload + instr->data_off;
  if( FD_UNLIKELY( instr->data_sz < 4 ) ) return;

  uint kind = fd_uint_load_4_fast( data );
  if( FD_UNLIKELY( kind != FD_VOTE_IX_KIND_TOWER_SYNC &&
                   kind != FD_VOTE_IX_KIND_TOWER_SYNC_SWITCH ) ) return;

  int err = fd_compact_tower_sync_de( ctx->tower_serde, data + sizeof(uint), instr->data_sz - sizeof(uint) );
  if( FD_UNLIKELY( err == -1 ) ) return;

  /* Vote account address */
  fd_pubkey_t const * accs = (fd_pubkey_t const *)fd_type_pun_const( payload + txn->acct_addr_off );
  uchar const * vote_acc;
  if( FD_UNLIKELY( txn->signature_cnt == 1 ) ) vote_acc = accs[1].uc;
  else                                         vote_acc = accs[2].uc;

  /* Count votes for ALL slots in the tower, not just the tip.
     This matches Agave's process_vote_state_update which processes
     the full tower update.  Each lockout offset represents a voted slot.
     Only count recent slots (within SLOT_RING_CNT of the tip). */
  ulong tip_slot = ctx->tower_serde->root;
  for( ulong i = 0; i < ctx->tower_serde->lockouts_cnt; i++ ) {
    tip_slot += ctx->tower_serde->lockouts[i].offset;
  }
  if( FD_UNLIKELY( !tip_slot ) ) return;

  ulong voted_slot = ctx->tower_serde->root;
  for( ulong i = 0; i < ctx->tower_serde->lockouts_cnt; i++ ) {
    voted_slot += ctx->tower_serde->lockouts[i].offset;
    /* Only count votes for recent slots to avoid wasting work on old ones */
    if( FD_LIKELY( tip_slot - voted_slot < SLOT_RING_CNT ) ) {
      count_vote( ctx, stem, voted_slot, vote_acc, is_gossip );
    }
  }
}

/* ---- Stem callbacks ---- */

static void
before_credit( fd_confirm_tile_t * ctx,
               fd_stem_context_t * stem FD_PARAM_UNUSED,
               int *               charge_busy FD_PARAM_UNUSED ) {
  if( FD_LIKELY( ctx->banks ) ) {
    static int joined = 0;
    if( FD_UNLIKELY( !joined ) ) {
      fd_banks_t * b = fd_banks_join( ctx->banks );
      if( !b ) return;
      ctx->banks = b;
      joined = 1;
      FD_LOG_NOTICE(( "confm: banks joined successfully" ));
    }
    if( FD_UNLIKELY( ctx->banks->root_idx == ULONG_MAX ) ) return;

    /* Refresh voter map from root bank (for gossip vote lookups).
       Only when root advances — stake changes slowly. */
    fd_bank_t * root = fd_banks_root( ctx->banks );
    if( root && root->f.slot > ctx->last_refresh_slot ) {
      ctx->last_refresh_slot = root->f.slot;
      fd_top_votes_t const * top_votes = fd_bank_top_votes_t_2_query( root );
      if( top_votes ) {
        fd_memset( ctx->voters, 0, sizeof(ctx->voters) );
        ctx->voter_cnt = 0;
        ctx->total_stake = 0;

        uchar __attribute__((aligned(FD_TOP_VOTES_ITER_ALIGN))) iter_mem[ FD_TOP_VOTES_ITER_FOOTPRINT ];
        for( fd_top_votes_iter_t * iter = fd_top_votes_iter_init( top_votes, iter_mem );
             !fd_top_votes_iter_done( top_votes, iter );
             fd_top_votes_iter_next( top_votes, iter ) ) {
          fd_pubkey_t pubkey;
          ulong stake = 0;
          int is_valid = fd_top_votes_iter_ele( top_votes, iter, &pubkey, NULL, &stake, NULL, NULL, NULL );
          if( FD_UNLIKELY( !is_valid || !stake ) ) continue;

          ulong key = fd_ulong_load_8( pubkey.uc );
          if( FD_UNLIKELY( !key ) ) key = 1UL;
          ulong idx = key & VOTER_MAP_MASK;
          for( ulong j=0; j<VOTER_MAP_CAP; j++ ) {
            ulong probe = (idx + j) & VOTER_MAP_MASK;
            if( FD_LIKELY( ctx->voters[probe].key == 0 ) ) {
              ctx->voters[probe].key   = key;
              ctx->voters[probe].stake = stake;
              fd_memcpy( ctx->voters[probe].pubkey, pubkey.uc, 32UL );
              ctx->voter_cnt++;
              break;
            }
            if( FD_LIKELY( ctx->voters[probe].key == key &&
                           !memcmp( ctx->voters[probe].pubkey, pubkey.uc, 32UL ) ) ) {
              ctx->voters[probe].stake = stake;
              break;
            }
          }
          ctx->total_stake += stake;
        }
      }
    }

    /* Bank diagnostic removed — confirmed 79-95% stake available */

  }
  static ulong tick = 0;
  if( FD_UNLIKELY( (++tick) % 20000000UL == 1 ) ) {
    ulong best_slot = 0; ulong best_stake = 0; (void)best_slot;
    for( ulong i=0; i<SLOT_RING_CNT; i++ ) {
      if( ctx->trackers[i].slot > 0 && !ctx->trackers[i].confirmed && ctx->trackers[i].stake > best_stake ) {
        best_slot = ctx->trackers[i].slot;
        best_stake = ctx->trackers[i].stake;
      }
    }
    double ratio = ctx->total_stake > 0 ? (double)best_stake / (double)ctx->total_stake : 0.0;
    FD_LOG_WARNING(( "confm: voter_cnt=%lu total_stake=%lu gossip=%lu confirmed=%lu best_ratio=%.4f refresh_slot=%lu",
                     ctx->voter_cnt, ctx->total_stake, diag_gossip_vote_cnt, diag_confirmed_cnt,
                     ratio, ctx->last_refresh_slot ));
  }
}

static inline void
during_frag( fd_confirm_tile_t * ctx,
             ulong               in_idx,
             ulong               seq FD_PARAM_UNUSED,
             ulong               sig,
             ulong               chunk,
             ulong               sz,
             ulong               ctl FD_PARAM_UNUSED ) {
  ctx->frag_valid = 0;
  if( FD_UNLIKELY( chunk < ctx->in[in_idx].chunk0 || chunk > ctx->in[in_idx].wmark || sz > ctx->in[in_idx].mtu ) ) {
    FD_LOG_WARNING(( "confm: chunk %lu %lu out of range [%lu,%lu] mtu=%lu in_idx=%lu — skipping",
                     chunk, sz, ctx->in[in_idx].chunk0, ctx->in[in_idx].wmark, ctx->in[in_idx].mtu, in_idx ));
    return;
  }

  uchar const * src = (uchar const *)fd_chunk_to_laddr_const( ctx->in[in_idx].mem, chunk );
  fd_memcpy( ctx->msg_buf, src, sz );
  ctx->last_sig       = sig;
  ctx->last_link_type = ctx->in[in_idx].link_type;
  ctx->frag_valid     = 1;

  /* Diagnostic: track message sources */
  static ulong frag_cnt[5] = {0};
  if( in_idx < 5 ) frag_cnt[in_idx]++;
  static ulong frag_diag_tick = 0;
  if( FD_UNLIKELY( (++frag_diag_tick) % 500000 == 1 ) ) {
    FD_LOG_WARNING(( "confm: during_frag in[0]=%lu in[1]=%lu in[2]=%lu in[3]=%lu type=%d sig=%lu",
                     frag_cnt[0], frag_cnt[1], frag_cnt[2], frag_cnt[3],
                     ctx->last_link_type, sig ));
  }
}

static inline void
after_frag( fd_confirm_tile_t * ctx,
            ulong               in_idx FD_PARAM_UNUSED,
            ulong               seq    FD_PARAM_UNUSED,
            ulong               sig    FD_PARAM_UNUSED,
            ulong               sz     FD_PARAM_UNUSED,
            ulong               tsorig FD_PARAM_UNUSED,
            ulong               tspub  FD_PARAM_UNUSED,
            fd_stem_context_t * stem ) {

  if( FD_UNLIKELY( !ctx->frag_valid ) ) return;

  if( ctx->last_link_type == CONFIRM_IN_GOSSIP ) {
    /* Gossip vote CRD */
    if( FD_LIKELY( ctx->last_sig == FD_GOSSIP_UPDATE_TAG_VOTE ) ) {
      fd_gossip_update_message_t const * msg = (fd_gossip_update_message_t const *)ctx->msg_buf;
      /* Track gossip vote freshness and inter-arrival timing */
      static long last_vote_ns = 0;
      static ulong vote_age_sum_ms = 0;
      static ulong vote_gap_sum_us = 0;
      static ulong vote_age_cnt = 0;
      long now_ns = fd_log_wallclock();
      long now_ms = now_ns / 1000000;
      long age_ms = now_ms - (long)msg->wallclock;
      long gap_us = last_vote_ns > 0 ? (now_ns - last_vote_ns) / 1000 : 0;
      last_vote_ns = now_ns;
      vote_age_sum_ms += (ulong)(age_ms > 0 ? age_ms : 0);
      vote_gap_sum_us += (ulong)(gap_us > 0 ? gap_us : 0);
      vote_age_cnt++;
      if( FD_UNLIKELY( vote_age_cnt % 10000 == 1 ) ) {
        FD_LOG_WARNING(( "confm: vote age avg=%lums this=%ldms gap_avg=%luus (cnt=%lu)",
                         vote_age_cnt > 0 ? vote_age_sum_ms / vote_age_cnt : 0,
                         age_ms,
                         vote_age_cnt > 1 ? vote_gap_sum_us / (vote_age_cnt-1) : 0,
                         vote_age_cnt ));
      }
      fd_gossip_vote_t const * vote = msg->vote->value;
      if( FD_LIKELY( vote->transaction_len > 0 && vote->transaction_len <= 1232UL ) ) {
        uchar txn_buf[ FD_TXN_MAX_SZ ] __attribute__((aligned(2UL)));
        ulong parsed_sz = fd_txn_parse( vote->transaction, vote->transaction_len, txn_buf, NULL );
        fd_txn_t * parsed = parsed_sz ? (fd_txn_t *)txn_buf : NULL;
        if( FD_LIKELY( parsed ) ) {
          process_vote_txn( ctx, stem, parsed, vote->transaction, 1 /* gossip */ );
        }
      }
    }

  } else if( ctx->last_link_type == CONFIRM_IN_REPLAY ) {

    if( ctx->last_sig == REPLAY_SIG_STAKE_TABLE ) {
      /* Stake table now loaded from bank in before_credit — ignore */
    } else if( ctx->last_sig == REPLAY_SIG_TXN_EXECUTED ) {
      /* Executed vote transaction from replay — count toward confirmation.
         This is the equivalent of Agave's ReplayVoteReceiver path. */
      static ulong replay_vote_cnt = 0;
      fd_replay_txn_executed_t * te = (fd_replay_txn_executed_t *)ctx->msg_buf;
      if( FD_LIKELY( te->is_committable ) ) {
        fd_txn_p_t * txn_p = te->txn;
        fd_txn_t const * txn = (fd_txn_t const *)TXN( txn_p );
        replay_vote_cnt++;
        if( FD_UNLIKELY( replay_vote_cnt % 10000 == 1 ) ) {
          FD_LOG_WARNING(( "confm: REPLAY_VOTE cnt=%lu", replay_vote_cnt ));
        }
        process_vote_txn( ctx, stem, txn, txn_p->payload, 0 /* replay */ );
      }
    }
  }
}

/* ---- Init ---- */

static void
unprivileged_init( fd_topo_t *      topo,
                   fd_topo_tile_t * tile ) {
  void * scratch = fd_topo_obj_laddr( topo, tile->tile_obj_id );

  FD_SCRATCH_ALLOC_INIT( l, scratch );
  fd_confirm_tile_t * ctx = FD_SCRATCH_ALLOC_APPEND( l, alignof(fd_confirm_tile_t), sizeof(fd_confirm_tile_t) );
  FD_SCRATCH_ALLOC_FINI( l, scratch_align() );

  memset( ctx, 0, sizeof(*ctx) );

  ctx->in_cnt = 0UL;
  for( ulong i = 0; i < tile->in_cnt; i++ ) {
    if( FD_UNLIKELY( !tile->in_link_poll[i] ) ) continue;
    fd_topo_link_t * link     = &topo->links[ tile->in_link_id[i] ];
    fd_topo_wksp_t * link_wksp = &topo->workspaces[ topo->objs[ link->dcache_obj_id ].wksp_id ];

    ctx->in[ctx->in_cnt].mem    = link_wksp->wksp;
    ctx->in[ctx->in_cnt].mtu    = link->mtu;
    ctx->in[ctx->in_cnt].chunk0 = fd_dcache_compact_chunk0( ctx->in[ctx->in_cnt].mem, link->dcache );
    ctx->in[ctx->in_cnt].wmark  = fd_dcache_compact_wmark( ctx->in[ctx->in_cnt].mem, link->dcache, link->mtu );

    if( !strcmp( link->name, "gossip_out" ) || !strcmp( link->name, "vote_out" ) )
      ctx->in[ctx->in_cnt].link_type = CONFIRM_IN_GOSSIP;
    else
      ctx->in[ctx->in_cnt].link_type = CONFIRM_IN_REPLAY;
    ctx->in_cnt++;
  }

  ctx->out.idx = fd_topo_find_tile_out_link( topo, tile, "confirm_out", 0UL );
  if( ctx->out.idx != ULONG_MAX ) {
    fd_topo_link_t * out_link = &topo->links[ tile->out_link_id[ ctx->out.idx ] ];
    ctx->out.mem    = topo->workspaces[ topo->objs[ out_link->dcache_obj_id ].wksp_id ].wksp;
    ctx->out.chunk0 = fd_dcache_compact_chunk0( ctx->out.mem, out_link->dcache );
    ctx->out.wmark  = fd_dcache_compact_wmark( ctx->out.mem, out_link->dcache, out_link->mtu );
    ctx->out.chunk  = ctx->out.chunk0;
  }

  /* Save topo for deferred banks join in before_credit (banks not ready at boot) */
  ulong banks_obj_id = fd_pod_query_ulong( topo->props, "banks", ULONG_MAX );
  if( FD_LIKELY( banks_obj_id != ULONG_MAX ) ) {
    ctx->banks = (fd_banks_t *)fd_topo_obj_laddr( topo, banks_obj_id );
    /* Note: we store the raw address. fd_banks_join is called later
       in before_credit after the replay tile initializes the banks. */
  } else {
    ctx->banks = NULL;
  }
  FD_LOG_NOTICE(( "confm tile: %lu inputs, out_idx=%lu, banks_ptr=%p",
                  ctx->in_cnt, ctx->out.idx, (void*)ctx->banks ));
}

static ulong
populate_allowed_seccomp( fd_topo_t const *      topo FD_PARAM_UNUSED,
                          fd_topo_tile_t const * tile FD_PARAM_UNUSED,
                          ulong                  out_cnt,
                          struct sock_filter *   out ) {
  populate_sock_filter_policy_fd_stream_tile( out_cnt, out, (uint)fd_log_private_logfile_fd() );
  return sock_filter_policy_fd_stream_tile_instr_cnt;
}

static ulong
populate_allowed_fds( fd_topo_t const *      topo FD_PARAM_UNUSED,
                      fd_topo_tile_t const * tile FD_PARAM_UNUSED,
                      ulong                  out_fds_cnt,
                      int *                  out_fds ) {
  if( FD_UNLIKELY( out_fds_cnt < 2UL ) ) FD_LOG_ERR(( "out_fds_cnt %lu", out_fds_cnt ));
  ulong out_cnt = 0UL;
  out_fds[ out_cnt++ ] = 2;
  if( FD_LIKELY( -1 != fd_log_private_logfile_fd() ) )
    out_fds[ out_cnt++ ] = fd_log_private_logfile_fd();
  return out_cnt;
}

#define STEM_BURST (64UL)

#define STEM_CALLBACK_CONTEXT_TYPE  fd_confirm_tile_t
#define STEM_CALLBACK_CONTEXT_ALIGN alignof(fd_confirm_tile_t)

#define STEM_CALLBACK_METRICS_WRITE metrics_write
#define STEM_CALLBACK_BEFORE_CREDIT before_credit
#define STEM_CALLBACK_DURING_FRAG   during_frag
#define STEM_CALLBACK_AFTER_FRAG    after_frag

#include "../../disco/stem/fd_stem.c"

fd_topo_run_tile_t fd_tile_confm = {
  .name                     = "confm",
  .populate_allowed_seccomp = populate_allowed_seccomp,
  .populate_allowed_fds     = populate_allowed_fds,
  .scratch_align            = scratch_align,
  .scratch_footprint        = scratch_footprint,
  .unprivileged_init        = unprivileged_init,
  .run                      = stem_run,
};
