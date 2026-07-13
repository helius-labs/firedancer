#include "../execle/fd_execle_err.h"
#include "../../util/pod/fd_pod_format.h"
#include "../../disco/metrics/fd_metrics.h"

#include "../../choreo/tower/fd_tower_serdes.h"
#include "../../discof/fd_startup.h"
#include "../../discof/replay/fd_execrp.h"
#include "../../discof/stream/fd_stream_msg.h"
#include "../../discof/stream/fd_stream_token.h"
#include "../../flamenco/capture/fd_capture_ctx.h"
#include "../../flamenco/runtime/fd_bank.h"
#include "../../flamenco/runtime/fd_txncache.h"
#include "../../flamenco/runtime/fd_cost_tracker.h"
#include "../../flamenco/runtime/fd_runtime.h"
#include "../../flamenco/runtime/fd_executor.h"
#include "../../flamenco/runtime/tests/fd_dump_pb.h"
#include "../../flamenco/progcache/fd_progcache_user.h"
#include "../../flamenco/log_collector/fd_log_collector_base.h"
#include "../../disco/metrics/fd_metrics.h"
#include "../../disco/events/generated/fd_event_gen.h"
#include "../../flamenco/events/fd_event_runtime.h"

#include <time.h>
#include "generated/fd_execrp_tile_seccomp.h"

/* The exec tile is responsible for executing single transactions.  The
   tile receives a parsed transaction (fd_txn_p_t) and an identifier to
   which bank to execute against (index into the bank pool).  With this,
   the exec tile is able to identify the correct bank and accounts
   database fork to execute the transaction against.  The exec tile then
   commits the results of the transaction to the accounts db and makes
   any necessary updates to the bank. */

typedef struct link_ctx {
  ulong       idx;
  fd_wksp_t * mem;
  ulong       chunk;
  ulong       chunk0;
  ulong       wmark;
} link_ctx_t;

struct fd_execrp_tile {
  ulong tile_idx;

  /* link-related data structures. */
  link_ctx_t            replay_in[ 1 ];
  link_ctx_t            execrp_replay_out[ 1 ]; /* TODO: Remove with solcap v2 */
  link_ctx_t            stream_out[ 1 ];        /* Output link to stream tile (optional, may be absent) */
  int                   stream_enabled;         /* Whether the stream output link was found */
  ulong                 write_version_seq;      /* Monotonic counter for account write_version */

  fd_sha512_t           sha_mem[ FD_TXN_ACTUAL_SIG_MAX ];
  fd_sha512_t *         sha_lj[ FD_TXN_ACTUAL_SIG_MAX ];

  /* Capture context for debugging runtime execution. */
  fd_capture_ctx_t *    capture_ctx;
  fd_capture_link_buf_t cap_execrp_out[1];

  /* Protobuf dumping context for debugging runtime execution and
     collecting seed corpora. */
  fd_dump_proto_ctx_t * dump_proto_ctx;
  fd_txn_dump_ctx_t *   txn_dump_ctx;

  fd_banks_t *    banks;
  fd_bank_t *     bank;
  fd_accdb_t *    accdb;
  fd_txncache_t * txncache;
  fd_progcache_t  progcache[1];

  ulong txn_idx;
  ulong slot;
  ulong dispatch_time_comp;

  fd_log_collector_t log_collector;

  fd_txn_in_t  txn_in;
  fd_txn_out_t txn_out;

  fd_runtime_t runtime[1];

  struct {
    ulong sigverify_cnt;
    ulong poh_hash_cnt;

    /* Ticks spent loading txn accounts */
    ulong txn_load_cum_ticks;

    /* Ticks spent validating txn invariants (e.g. status cache, fee payer) */
    ulong txn_check_cum_ticks;

    /* Ticks spent executing a txn (includes any VM time) */
    ulong txn_exec_cum_ticks;

    /* Ticks spent committing a txn (database writes) */
    ulong txn_commit_cum_ticks;

    ulong txn_result[ FD_METRICS_ENUM_TRANSACTION_RESULT_CNT ];
  } metrics;

  /* If non-zero, emit one runtime_txn event per dispatched txn */
  int report_transaction_diffs;
};

typedef struct fd_execrp_tile fd_execrp_tile_t;

FD_FN_CONST static inline ulong
scratch_align( void ) {
  return 128UL;
}

FD_FN_PURE static inline ulong
scratch_footprint( fd_topo_tile_t const * tile ) {
  ulong l = FD_LAYOUT_INIT;
  l = FD_LAYOUT_APPEND(   l, alignof(fd_execrp_tile_t),    sizeof(fd_execrp_tile_t)                             );
  l = FD_LAYOUT_APPEND(   l, fd_txncache_align(),          fd_txncache_footprint( tile->execrp.max_live_slots ) );
  l = FD_LAYOUT_APPEND(   l, fd_accdb_align(),             fd_accdb_footprint( tile->execrp.max_live_slots )    );
  l = FD_LAYOUT_APPEND(   l, FD_PROGCACHE_SCRATCH_ALIGN,   FD_PROGCACHE_SCRATCH_FOOTPRINT                       );

  if( FD_UNLIKELY( strlen( tile->execrp.solcap_capture ) ) ) {
    l = FD_LAYOUT_APPEND( l, fd_capture_ctx_align(),       fd_capture_ctx_footprint()                           );
  }

  if( FD_UNLIKELY( strlen( tile->execrp.dump_proto_dir ) ) ) {
    l = FD_LAYOUT_APPEND( l, alignof(fd_dump_proto_ctx_t), sizeof(fd_dump_proto_ctx_t)                          );
    l = FD_LAYOUT_APPEND( l, fd_txn_dump_context_align(),  fd_txn_dump_context_footprint()                      );
    if( FD_UNLIKELY( tile->execrp.dump_instr_to_pb || tile->execrp.dump_syscall_to_pb || tile->execrp.dump_txn_to_pb ) ) {
      l = FD_LAYOUT_APPEND( l, FD_SPAD_ALIGN,              FD_SPAD_FOOTPRINT( 1UL<<28UL )                       );
    }
  }

  return FD_LAYOUT_FINI(  l, scratch_align() );
}

static void
metrics_write( fd_execrp_tile_t * ctx ) {
  FD_MCNT_SET      ( EXECRP, SIGNATURE_VERIFIED,    ctx->metrics.sigverify_cnt );
  FD_MCNT_SET      ( EXECRP, POH_HASHED,     ctx->metrics.poh_hash_cnt  );
  FD_MCNT_ENUM_COPY( EXECRP, TXN_RESULT,   ctx->metrics.txn_result    );

  fd_progcache_metrics_t * pm = ctx->progcache->metrics;
  FD_MCNT_SET( EXECRP, PROGCACHE_LOOKUP,                 pm->lookup_cnt     );
  FD_MCNT_SET( EXECRP, PROGCACHE_HIT,                    pm->hit_cnt        );
  FD_MCNT_SET( EXECRP, PROGCACHE_MISS,                   pm->miss_cnt       );
  FD_MCNT_SET( EXECRP, PROGCACHE_OOM_HEAP,               pm->oom_heap_cnt   );
  FD_MCNT_SET( EXECRP, PROGCACHE_OOM_DESC,               pm->oom_desc_cnt   );
  FD_MCNT_SET( EXECRP, PROGCACHE_FILL,                   pm->fill_cnt       );
  FD_MCNT_SET( EXECRP, PROGCACHE_FILL_BYTES,             pm->fill_tot_sz    );
  FD_MCNT_SET( EXECRP, PROGCACHE_SPILL,                  pm->spill_cnt      );
  FD_MCNT_SET( EXECRP, PROGCACHE_SPILL_BYTES,            pm->spill_tot_sz   );
  FD_MCNT_SET( EXECRP, PROGCACHE_EVICTION,               pm->evict_cnt      );
  FD_MCNT_SET( EXECRP, PROGCACHE_EVICTION_BYTES,         pm->evict_tot_sz   );
  FD_MCNT_SET( EXECRP, PROGCACHE_DURATION_SECONDS,       pm->cum_pull_ticks );
  FD_MCNT_SET( EXECRP, PROGCACHE_LOAD_DURATION_SECONDS,  pm->cum_load_ticks );

  FD_MCNT_SET( EXECRP, TXN_REGIME_DURATION_NANOS_SETUP,  ctx->metrics.txn_load_cum_ticks+ctx->metrics.txn_check_cum_ticks );
  FD_MCNT_SET( EXECRP, TXN_REGIME_DURATION_NANOS_EXEC,   ctx->metrics.txn_exec_cum_ticks    );
  FD_MCNT_SET( EXECRP, TXN_REGIME_DURATION_NANOS_COMMIT, ctx->metrics.txn_commit_cum_ticks  );

  fd_runtime_t const * runtime = ctx->runtime;
  ulong cpi_ticks  = runtime->metrics.cpi_setup_cum_ticks +
                     runtime->metrics.cpi_commit_cum_ticks;
  ulong exec_ticks = fd_ulong_sat_sub( runtime->metrics.vm_exec_cum_ticks, cpi_ticks );
  FD_MCNT_SET( EXECRP, VM_REGIME_DURATION_NANOS_SETUP,       runtime->metrics.vm_setup_cum_ticks   );
  FD_MCNT_SET( EXECRP, VM_REGIME_DURATION_NANOS_COMMIT,      runtime->metrics.vm_commit_cum_ticks  );
  FD_MCNT_SET( EXECRP, VM_REGIME_DURATION_NANOS_SETUP_CPI,   runtime->metrics.cpi_setup_cum_ticks  );
  FD_MCNT_SET( EXECRP, VM_REGIME_DURATION_NANOS_COMMIT_CPI,  runtime->metrics.cpi_commit_cum_ticks );
  FD_MCNT_SET( EXECRP, VM_REGIME_DURATION_NANOS_INTERPRETER, exec_ticks                            );

  FD_MCNT_SET( EXECRP, CU_EXECUTED, runtime->metrics.cu_cum );

  FD_ACCDB_METRICS_WRITE( EXECRP, fd_accdb_metrics( ctx->accdb ) );
}


static void
publish_txn_finalized_msg( fd_execrp_tile_t *  ctx,
                           fd_stem_context_t * stem ) {
  fd_execrp_task_done_msg_t * msg  = fd_chunk_to_laddr( ctx->execrp_replay_out->mem, ctx->execrp_replay_out->chunk );
  msg->bank_idx                  = ctx->bank->idx;
  msg->txn_exec->txn_idx         = ctx->txn_idx;
  msg->txn_exec->is_committable  = ctx->txn_out.err.is_committable;
  msg->txn_exec->is_fees_only    = ctx->txn_out.err.is_fees_only;
  msg->txn_exec->txn_err         = ctx->txn_out.err.txn_err;
  msg->txn_exec->slot            = ctx->slot;
  msg->txn_exec->start_shred_idx = ctx->txn_in.txn->start_shred_idx;
  msg->txn_exec->end_shred_idx   = ctx->txn_in.txn->end_shred_idx;

  if( FD_UNLIKELY( !ctx->txn_out.details.is_simple_vote || !fd_txn_parse_simple_vote( TXN( ctx->txn_in.txn ), ctx->txn_in.txn->payload, msg->txn_exec->vote.identity, msg->txn_exec->vote.vote_acct, &msg->txn_exec->vote.slot ) ) ) {
    msg->txn_exec->vote.slot       = ULONG_MAX;
    *msg->txn_exec->vote.identity  = (fd_pubkey_t){ 0 };
    *msg->txn_exec->vote.vote_acct = (fd_pubkey_t){ 0 };
  }

  if( FD_UNLIKELY( !msg->txn_exec->is_committable ) ) {
    uchar * signature = (uchar *)ctx->txn_in.txn->payload + TXN( ctx->txn_in.txn )->signature_off;
    FD_BASE58_ENCODE_64_BYTES( signature, signature_b58 );
    FD_LOG_WARNING(( "block marked dead (slot=%lu) because of invalid transaction (signature=%s) (txn_err=%d)", ctx->slot, signature_b58, ctx->txn_out.err.txn_err ));
  }

  fd_stem_publish( stem, ctx->execrp_replay_out->idx, (FD_EXECRP_TT_TXN_EXEC<<32)|ctx->tile_idx, ctx->execrp_replay_out->chunk, sizeof(*msg), 0UL, ctx->dispatch_time_comp, fd_frag_meta_ts_comp( fd_tickcount() ) );

  ctx->execrp_replay_out->chunk = fd_dcache_compact_next( ctx->execrp_replay_out->chunk, sizeof(*msg), ctx->execrp_replay_out->chunk0, ctx->execrp_replay_out->wmark );
}

/* Look up token decimals by finding the mint account in the
   transaction's account list and reading byte 44 of the mint data. */

static uchar
lookup_token_decimals( fd_runtime_t const * runtime,
                       fd_txn_out_t const * txn_out,
                       uchar const *        mint_key ) {
  /* First try the mint decimals cache populated at account setup time.
     This works even when meta wasn't loaded post-execution. */
  for( ulong i=0; i<txn_out->accounts.cnt; i++ ) {
    if( !runtime->accounts.starting_token[i].is_mint ) continue;
    if( !fd_memeq( txn_out->accounts.keys[i].uc, mint_key, 32UL ) ) continue;
    return runtime->accounts.starting_token[i].decimals;
  }
  /* Fallback: parse from account meta if available (usually not for
     read-only mints, but works if the mint was loaded fully). */
  for( ulong i=0; i<txn_out->accounts.cnt; i++ ) {
    if( FD_UNLIKELY( !txn_out->accounts.account[i].meta ) ) continue;
    if( FD_UNLIKELY( !fd_memeq( txn_out->accounts.keys[i].uc, mint_key, 32UL ) ) ) continue;
    ulong dlen = txn_out->accounts.account[i].meta->dlen;
    if( FD_LIKELY( dlen>=82UL ) ) {
      uchar const * data = (uchar const *)( txn_out->accounts.account[i].meta + 1 );
      if( data[45] ) return data[44];
    }
    break;
  }
  return 0;
}

/* lookup_mint_decimals_via_accdb: for mints not referenced in the txn's
   account list, look them up directly in accdb.  Uses short-lived RO
   refs (open→read→close immediately) so the val_lock read counter
   never accumulates across txns.  Caches the decimals in the per-tile
   mint cache on hit so subsequent txns skip the accdb ref.  Returns 0
   if not found. */
static uchar
lookup_mint_decimals_via_accdb( fd_execrp_tile_t * ctx,
                                uchar const *      mint_key ) {
  fd_accdb_ro_t ro[1];
  fd_pubkey_t const * mk = (fd_pubkey_t const *)mint_key;
  fd_funk_txn_xid_t xid = { .ul = { ctx->bank->f.slot, ctx->bank->idx } };
  if( FD_UNLIKELY( !fd_accdb_open_ro( ctx->runtime->accdb, ro, &xid, mk ) ) ) return 0;
  ulong dlen = fd_accdb_ref_data_sz( ro );
  uchar dec = 0;
  if( FD_LIKELY( dlen>=82UL ) ) {
    uchar const * data = fd_accdb_ref_data_const( ro );
    if( data[45] ) dec = data[44];
  }
  fd_accdb_close_ro( ctx->runtime->accdb, ro );
  /* Cache the result so subsequent txns skip accdb entirely. */
  if( dec ) {
    static uchar const zero_mint[32] = {0};
    ulong h = FD_LOAD( ulong, mint_key ) & (FD_RUNTIME_MINT_CACHE_SZ-1UL);
    for( ulong probe=0UL; probe<FD_RUNTIME_MINT_CACHE_SZ; probe++ ) {
      ulong slot = (h+probe) & (FD_RUNTIME_MINT_CACHE_SZ-1UL);
      uchar * cached_mint = ctx->runtime->accounts.mint_cache[slot].mint;
      if( fd_memeq( cached_mint, mint_key, 32UL ) ) {
        ctx->runtime->accounts.mint_cache[slot].decimals = dec;
        break;
      }
      if( fd_memeq( cached_mint, zero_mint, 32UL ) ) {
        fd_memcpy( cached_mint, mint_key, 32UL );
        ctx->runtime->accounts.mint_cache[slot].decimals = dec;
        break;
      }
    }
  }
  return dec;
}

/* publish_stream_txn_msg packs the full transaction execution result
   into an fd_stream_txn_msg_t and publishes it on the stream_out link.
   Called after execution and before commit releases account handles.
   This is gated behind stream_enabled so it has zero cost when the
   stream tile is not configured. */

static void
publish_stream_txn_msg( fd_execrp_tile_t *  ctx,
                        fd_stem_context_t * stem ) {
  if( FD_LIKELY( !ctx->stream_enabled ) ) return;

  uchar * dst = (uchar *)fd_chunk_to_laddr( ctx->stream_out->mem, ctx->stream_out->chunk );
  fd_stream_txn_msg_t * msg = (fd_stream_txn_msg_t *)dst;

  msg->msg_type = FD_STREAM_MSG_TYPE_TXN;

  /* Raw transaction */
  fd_memcpy( &msg->txn, ctx->txn_in.txn, sizeof(fd_txn_p_t) );
  msg->slot    = ctx->bank->f.slot;
  msg->txn_idx = ctx->txn_idx;

  /* Execution result */
  msg->is_committable = ctx->txn_out.err.is_committable;
  msg->is_fees_only   = ctx->txn_out.err.is_fees_only;
  msg->txn_err        = ctx->txn_out.err.txn_err;
  msg->exec_err       = ctx->txn_out.err.exec_err;
  msg->exec_err_kind  = ctx->txn_out.err.exec_err_kind;
  msg->exec_err_idx   = ctx->txn_out.err.exec_err_idx;
  msg->custom_err     = ctx->txn_out.err.custom_err;
  msg->is_simple_vote = ctx->txn_out.details.is_simple_vote;

  /* Fees and compute */
  msg->fee                    = ctx->txn_out.details.execution_fee + ctx->txn_out.details.priority_fee;
  msg->compute_units_consumed = ctx->txn_out.details.compute_budget.compute_unit_limit -
                                ctx->txn_out.details.compute_budget.compute_meter;

  /* cost_units: total block-level cost charged for this transaction.
     Sum of signature_cost + write_lock_cost + data_bytes_cost +
     programs_execution_cost + loaded_accounts_data_size_cost.  Simple
     votes have a flat usage cost. */
  {
    fd_transaction_cost_t const * txn_cost = &ctx->txn_out.details.txn_cost;
    if( txn_cost->type == FD_TXN_COST_TYPE_SIMPLE_VOTE ) {
      msg->cost_units = FD_SIMPLE_VOTE_USAGE_COST;
    } else {
      fd_usage_cost_details_t const * u = &txn_cost->transaction;
      ulong c = 0UL;
      c = fd_ulong_sat_add( c, u->signature_cost );
      c = fd_ulong_sat_add( c, u->write_lock_cost );
      c = fd_ulong_sat_add( c, u->data_bytes_cost );
      c = fd_ulong_sat_add( c, u->programs_execution_cost );
      c = fd_ulong_sat_add( c, u->loaded_accounts_data_size_cost );
      msg->cost_units = c;
    }
  }

  /* Return data */
  fd_memcpy( msg->return_data_program_id, ctx->txn_out.details.return_data.program_id.uc, 32UL );
  msg->return_data_sz = (ushort)fd_ulong_min( ctx->txn_out.details.return_data.len, 1024UL );

  /* Account counts.  For non-committable transactions the account
     arrays may be partially initialized (e.g. cnt was never set after a
     txn_err=WOULD_EXCEED_MAX_BLOCK_COST_LIMIT before load completed);
     clamp to zero to skip the per-account regions and emit only the
     header fields. */
  ushort account_cnt = ctx->txn_out.err.is_committable
                     ? (ushort)ctx->txn_out.accounts.cnt
                     : (ushort)0;
  msg->account_cnt = account_cnt;

  /* Pack variable-length regions */
  uchar * cursor = dst + sizeof(fd_stream_txn_msg_t);

  /* 1. Pre-balances from runtime->accounts.starting_lamports.
     For the fee payer (account 0), starting_lamports was captured AFTER
     fee deduction (it's used for instruction-level balance accounting).
     Yellowstone reports the pre-fee balance, so add the fee back.

     Note: for read-only accounts that FD's executor did not load, the
     starting_lamports entry is 0 and we will emit 0 — this creates a
     small mismatch with Agave for program-owned accounts referenced
     but not modified.  A precise lookup would require an accdb RO
     open which is not safe here (blows the read-lock counter on hot
     records). */
  ulong * pre_bal = (ulong *)cursor;
  ulong total_fee = ctx->txn_out.details.execution_fee + ctx->txn_out.details.priority_fee;
  for( ushort i=0; i<account_cnt; i++ ) {
    pre_bal[i] = ctx->runtime->accounts.starting_lamports[i];
  }
  if( FD_LIKELY( account_cnt>0 ) ) {
    pre_bal[0] += total_fee;
  }
  cursor += account_cnt * sizeof(ulong);

  /* 2. Post-balances.  For a successful transaction, use the post-execution
     .meta->lamports for writable accounts (the execution engine updates
     these in place, and lamports=0 means the account was closed).  For
     failed transactions, the rollback fee-payer is the correct fee-payer
     post-balance (pre-fee minus fee), and all other accounts revert.
     Read-only accounts are never modified so post = pre. */
  int txn_failed = (ctx->txn_out.err.txn_err!=0 || ctx->txn_out.err.exec_err!=0);
  ulong * post_bal = (ulong *)cursor;
  for( ushort i=0; i<account_cnt; i++ ) {
    if( FD_UNLIKELY( txn_failed ) ) {
      /* Failed txn: only fee-payer's balance changed (fee deducted).
         Other accounts are unchanged from pre-balance. */
      if( i==0 && ctx->txn_out.accounts.rollback_fee_payer ) {
        post_bal[i] = ctx->txn_out.accounts.rollback_fee_payer->lamports;
      } else {
        post_bal[i] = pre_bal[i];
      }
      continue;
    }
    /* Successful txn.
       - Writable accounts: trust meta->lamports (0 = closed, correct).
       - Read-only accounts: pre == post (unchanged). */
    if( ctx->txn_out.accounts.is_writable[i] ) {
      fd_acc_t const * acc = ctx->txn_out.accounts.account[i];
      post_bal[i] = acc ? acc->lamports : pre_bal[i];
    } else {
      post_bal[i] = pre_bal[i];
    }
  }
  cursor += account_cnt * sizeof(ulong);

  /* 3. is_writable flags */
  fd_memcpy( cursor, ctx->txn_out.accounts.is_writable, account_cnt );
  cursor += account_cnt;

  /* 4. Account keys (needed for loaded addresses from ALTs) */
  for( ushort i=0; i<account_cnt; i++ ) {
    fd_memcpy( cursor, ctx->txn_out.accounts.keys[i].uc, 32UL );
    cursor += 32UL;
  }

  /* Determine which accounts are "invoked" (used as program_id in an
     instruction).  Agave excludes these from token balance emission. */
  uchar is_invoked[ 256 ] = {0};
  {
    fd_txn_t const * txn_desc = TXN( ctx->txn_in.txn );
    fd_txn_instr_t const * instrs = txn_desc->instr;
    for( ulong ii=0; ii<txn_desc->instr_cnt; ii++ ) {
      uchar pid = instrs[ii].program_id;
      if( pid<account_cnt ) is_invoked[ pid ] = 1;
    }
  }
  /* Determine has_token_program: is any account in the txn one of the
     SPL Token programs? */
  int has_token_program = 0;
  {
    static uchar const classic_spl[32] = { 0x06,0xdd,0xf6,0xe1,0xd7,0x65,0xa1,0x93,0xd9,0xcb,0xe1,0x46,0xce,0xeb,0x79,0xac,0x1c,0xb4,0x85,0xed,0x5f,0x5b,0x37,0x91,0x3a,0x8c,0xf5,0x85,0x7e,0xff,0x00,0xa9 };
    static uchar const token_2022[32] = { 0x06,0xdd,0xf6,0xe1,0xee,0x75,0x8f,0xde,0x18,0x42,0x5d,0xbc,0xe4,0x6c,0xcd,0xda,0xb6,0x1a,0xfc,0x4d,0x83,0xb9,0x0d,0x27,0xfe,0xbd,0xf9,0x28,0xd8,0xa1,0x8b,0xfc };
    for( ushort i=0; i<account_cnt; i++ ) {
      if( fd_memeq( ctx->txn_out.accounts.keys[i].uc, classic_spl, 32UL ) ||
          fd_memeq( ctx->txn_out.accounts.keys[i].uc, token_2022, 32UL ) ) {
        has_token_program = 1;
        break;
      }
    }
  }

  /* 5. Pre-token balances — from runtime->accounts.starting_token[],
     captured during account loading before execution began.  Agave rules:
     has_token_program AND !is_invoked(idx) AND account_key is not a token
     program itself AND account owner is a token program AND account
     parses as a valid initialized token account. */
  fd_stream_token_balance_t * pre_tok = (fd_stream_token_balance_t *)cursor;
  ushort pre_tok_cnt = 0;
  for( ushort i=0; i<account_cnt && pre_tok_cnt<128; i++ ) {
    if( FD_LIKELY( !ctx->runtime->accounts.starting_token[i].is_token ) ) continue;
    if( FD_UNLIKELY( !has_token_program ) ) continue;
    if( FD_UNLIKELY( is_invoked[i] ) ) continue;
    /* Note: account being a token account means its owner is a token program.
       The `!is_known_spl_token_id(key)` check in Agave excludes accounts
       whose ADDRESS is the Token program itself. Since a Token program
       is owned by BPFLoader (not itself), starting_token[i].is_token=1
       already excludes them. */
    pre_tok[ pre_tok_cnt ].account_idx = (uchar)i;
    /* Decimals lookup order: (1) mint cache (populated when mint was
       loaded into any prior txn on this tile), (2) same-txn account list
       search, (3) direct accdb open→read→close for the mint. */
    uchar dec = ctx->runtime->accounts.starting_token[i].decimals;
    if( !dec ) dec = lookup_token_decimals( ctx->runtime, &ctx->txn_out, ctx->runtime->accounts.starting_token[i].mint );
    if( !dec ) dec = lookup_mint_decimals_via_accdb( ctx, ctx->runtime->accounts.starting_token[i].mint );
    pre_tok[ pre_tok_cnt ].decimals = dec;
    memset( pre_tok[ pre_tok_cnt ]._pad, 0, sizeof(pre_tok[pre_tok_cnt]._pad) );
    fd_memcpy( pre_tok[ pre_tok_cnt ].mint,       ctx->runtime->accounts.starting_token[i].mint,       32UL );
    fd_memcpy( pre_tok[ pre_tok_cnt ].owner,      ctx->runtime->accounts.starting_token[i].owner,      32UL );
    fd_memcpy( pre_tok[ pre_tok_cnt ].program_id, ctx->runtime->accounts.starting_token[i].program_id, 32UL );
    pre_tok[ pre_tok_cnt ].amount = ctx->runtime->accounts.starting_token[i].amount;
    pre_tok_cnt++;
  }
  msg->pre_token_balance_cnt = pre_tok_cnt;
  cursor += pre_tok_cnt * sizeof(fd_stream_token_balance_t);

  /* 6. Post-token balances — iterate accounts, parse SPL Token layout.
     Same Agave filter rules as pre_token_balances (has_token_program,
     !is_invoked, valid token account).  Additionally skip closed
     accounts (lamports == 0 after execution).
     For FAILED transactions, Agave rolls back and reports post = pre;
     we mirror that by emitting starting_token[i] values instead of
     the (potentially half-mutated) live account meta. */
  fd_stream_token_balance_t * post_tok = (fd_stream_token_balance_t *)cursor;
  ushort post_tok_cnt = 0;
  if( FD_UNLIKELY( txn_failed ) ) {
    for( ushort i=0; i<account_cnt && post_tok_cnt<128; i++ ) {
      if( FD_LIKELY( !ctx->runtime->accounts.starting_token[i].is_token ) ) continue;
      if( FD_UNLIKELY( !has_token_program ) ) continue;
      if( FD_UNLIKELY( is_invoked[i] ) ) continue;
      if( FD_UNLIKELY( post_bal[i]==0UL ) ) continue;
      post_tok[ post_tok_cnt ].account_idx = (uchar)i;
      uchar dec_f = ctx->runtime->accounts.starting_token[i].decimals;
      if( !dec_f ) dec_f = lookup_token_decimals( ctx->runtime, &ctx->txn_out, ctx->runtime->accounts.starting_token[i].mint );
      if( !dec_f ) dec_f = lookup_mint_decimals_via_accdb( ctx, ctx->runtime->accounts.starting_token[i].mint );
      post_tok[ post_tok_cnt ].decimals = dec_f;
      memset( post_tok[ post_tok_cnt ]._pad, 0, sizeof(post_tok[post_tok_cnt]._pad) );
      fd_memcpy( post_tok[ post_tok_cnt ].mint,       ctx->runtime->accounts.starting_token[i].mint,       32UL );
      fd_memcpy( post_tok[ post_tok_cnt ].owner,      ctx->runtime->accounts.starting_token[i].owner,      32UL );
      fd_memcpy( post_tok[ post_tok_cnt ].program_id, ctx->runtime->accounts.starting_token[i].program_id, 32UL );
      post_tok[ post_tok_cnt ].amount = ctx->runtime->accounts.starting_token[i].amount;
      post_tok_cnt++;
    }
    msg->post_token_balance_cnt = post_tok_cnt;
    cursor += post_tok_cnt * sizeof(fd_stream_token_balance_t);
    goto post_tok_done;
  }
  for( ushort i=0; i<account_cnt && post_tok_cnt<128; i++ ) {
    fd_acc_t const * acc = ctx->txn_out.accounts.account[i];
    if( FD_UNLIKELY( !acc ) ) continue;
    if( FD_UNLIKELY( post_bal[i]==0UL ) ) continue; /* closed account */
    if( FD_UNLIKELY( !has_token_program ) ) continue;
    if( FD_UNLIKELY( is_invoked[i] ) ) continue;
    if( FD_LIKELY( !fd_stream_is_token_program( (fd_pubkey_t const *)acc->owner ) ) ) continue;
    uchar const * data    = acc->data;
    ulong         data_sz = acc->data_len;
    fd_stream_token_info_t info;
    if( FD_UNLIKELY( !fd_stream_parse_token_account( data, data_sz, &info ) ) ) continue;
    post_tok[ post_tok_cnt ].account_idx = (uchar)i;
    /* Same 3-tier decimals lookup as pre_tok. */
    uchar dec_p = ctx->runtime->accounts.starting_token[i].decimals;
    if( !dec_p ) dec_p = lookup_token_decimals( ctx->runtime, &ctx->txn_out, info.mint.uc );
    if( !dec_p ) dec_p = lookup_mint_decimals_via_accdb( ctx, info.mint.uc );
    post_tok[ post_tok_cnt ].decimals = dec_p;
    memset( post_tok[ post_tok_cnt ]._pad, 0, sizeof(post_tok[post_tok_cnt]._pad) );
    fd_memcpy( post_tok[ post_tok_cnt ].mint,       info.mint.uc,  32UL );
    fd_memcpy( post_tok[ post_tok_cnt ].owner,      info.owner.uc, 32UL );
    fd_memcpy( post_tok[ post_tok_cnt ].program_id, owner->uc,     32UL );
    post_tok[ post_tok_cnt ].amount = info.amount;
    post_tok_cnt++;
  }
  msg->post_token_balance_cnt = post_tok_cnt;
  cursor += post_tok_cnt * sizeof(fd_stream_token_balance_t);
  (void)0; /* fallthrough */
post_tok_done:;

  /* 7. Log messages — copy from log collector.
     For transactions that did not actually execute — never committed
     (is_committable==0) or committed as fees-only (is_fees_only==1) —
     no instructions ran, so Agave emits no logs.  Committed txns with
     instruction errors DID execute and have logs. */
  ushort log_sz = ctx->log_collector.buf_sz;
  if( FD_UNLIKELY( !ctx->txn_out.err.is_committable || ctx->txn_out.err.is_fees_only ) ) log_sz = 0;
  msg->log_sz = log_sz;
  if( FD_LIKELY( log_sz ) ) {
    fd_memcpy( cursor, ctx->log_collector.buf, log_sz );
  }
  cursor += log_sz;

  /* 8. Inner instructions — pack from runtime instruction trace.
     Instructions with stack_height > 1 are inner (CPI) instructions. */
  uchar * inner_start = cursor;
  ushort  inner_cnt   = 0;
  ulong   trace_len   = ctx->runtime->instr.trace_length;
  int     top_level_idx = -1;

  for( ulong t=0; t<trace_len; t++ ) {
    fd_instr_info_t const * instr = &ctx->runtime->instr.trace[t];
    if( instr->stack_height<=1 ) {
      /* Top-level instruction — track its index */
      top_level_idx++;
      continue;
    }
    /* Inner instruction — pack it */
    fd_stream_inner_instr_t * hdr = (fd_stream_inner_instr_t *)cursor;
    hdr->top_level_idx  = (uchar)fd_int_max( top_level_idx, 0 );
    hdr->program_id_idx = instr->program_id;
    hdr->acct_cnt       = instr->acct_cnt;
    hdr->data_sz        = instr->data_sz;
    hdr->stack_height   = instr->stack_height;
    hdr->_pad           = 0;
    cursor += sizeof(fd_stream_inner_instr_t);

    /* Account indices (index_in_transaction for each account) */
    for( ushort a=0; a<instr->acct_cnt; a++ ) {
      *cursor++ = (uchar)instr->accounts[a].index_in_transaction;
    }

    /* Instruction data */
    fd_memcpy( cursor, instr->data, instr->data_sz );
    cursor += instr->data_sz;

    inner_cnt++;
  }
  msg->inner_instruction_cnt      = inner_cnt;
  msg->inner_instructions_data_sz = (uint)( cursor - inner_start );

  /* 9. Return data */
  if( FD_LIKELY( msg->return_data_sz ) ) {
    fd_memcpy( cursor, ctx->txn_out.details.return_data.data, msg->return_data_sz );
  }
  cursor += msg->return_data_sz;

  /* Publish */
  ulong total_sz = (ulong)( cursor - dst );
  fd_stem_publish( stem, ctx->stream_out->idx, ctx->tile_idx, ctx->stream_out->chunk, total_sz, 0UL, 0UL, fd_frag_meta_ts_comp( fd_tickcount() ) );
  ctx->stream_out->chunk = fd_dcache_compact_next( ctx->stream_out->chunk, total_sz, ctx->stream_out->chunk0, ctx->stream_out->wmark );
}

/* publish_stream_acct_msgs publishes one fd_stream_acct_msg_t per
   writable account modified by the transaction.  Called after
   publish_stream_txn_msg, before commit releases handles.

   Account data can be large (up to 10MB for some programs), so we
   cap at the link MTU (USHORT_MAX).  Accounts larger than the MTU
   are skipped. */

static void
publish_stream_acct_msgs( fd_execrp_tile_t *  ctx,
                          fd_stem_context_t * stem ) {
  if( FD_LIKELY( !ctx->stream_enabled ) ) return;

  /* For non-committable transactions the account arrays may not be
     fully populated — e.g. txn_err=WOULD_EXCEED_MAX_BLOCK_COST_LIMIT is
     raised before account load completes.  Skip acct-msg emission for
     these; the txn-msg emitter has its own null-meta guards. */
  if( FD_UNLIKELY( !ctx->txn_out.err.is_committable ) ) return;

  ushort account_cnt = (ushort)ctx->txn_out.accounts.cnt;

  /* Get the txn signature for the txn_signature field */
  uchar const * sig = (uchar const *)fd_txn_get_signatures( TXN( ctx->txn_in.txn ), ctx->txn_in.txn->payload );

  for( ushort i=0; i<account_cnt; i++ ) {
    /* Only emit updates for writable accounts that were successfully loaded */
    if( FD_LIKELY( !ctx->txn_out.accounts.is_writable[i] ) ) continue;
    fd_acc_t const * acc = ctx->txn_out.accounts.account[i];
    if( FD_UNLIKELY( !acc ) ) continue;

    uchar const * data = acc->data;
    uint data_sz = (uint)acc->data_len;

    /* Skip accounts whose data exceeds the link MTU */
    ulong total_sz = sizeof(fd_stream_acct_msg_t) + (ulong)data_sz;
    if( FD_UNLIKELY( total_sz > USHORT_MAX ) ) continue;

    uchar * dst = (uchar *)fd_chunk_to_laddr( ctx->stream_out->mem, ctx->stream_out->chunk );
    fd_stream_acct_msg_t * amsg = (fd_stream_acct_msg_t *)dst;

    amsg->msg_type    = FD_STREAM_MSG_TYPE_ACCT;
    amsg->executable  = (uchar)acc->executable;
    amsg->_pad[0]     = 0;
    amsg->_pad[1]     = 0;
    amsg->data_sz     = data_sz;
    amsg->slot        = ctx->bank->f.slot;
    amsg->lamports    = acc->lamports;
    amsg->write_version = ctx->write_version_seq++;
    fd_memcpy( amsg->pubkey,        ctx->txn_out.accounts.keys[i].uc, 32UL );
    fd_memcpy( amsg->owner,         acc->owner,                       32UL );
    fd_memcpy( amsg->txn_signature, sig,                              64UL );

    /* Copy account data */
    if( FD_LIKELY( data_sz ) ) {
      fd_memcpy( dst + sizeof(fd_stream_acct_msg_t), data, data_sz );
    }

    fd_stem_publish( stem, ctx->stream_out->idx, ctx->tile_idx, ctx->stream_out->chunk, total_sz, 0UL, 0UL, fd_frag_meta_ts_comp( fd_tickcount() ) );
    ctx->stream_out->chunk = fd_dcache_compact_next( ctx->stream_out->chunk, total_sz, ctx->stream_out->chunk0, ctx->stream_out->wmark );
  }
}

static inline int
returnable_frag( fd_execrp_tile_t *  ctx,
                 ulong               in_idx,
                 ulong               seq FD_PARAM_UNUSED,
                 ulong               sig,
                 ulong               chunk,
                 ulong               sz,
                 ulong               ctl FD_PARAM_UNUSED,
                 ulong               tsorig FD_PARAM_UNUSED,
                 ulong               tspub,
                 fd_stem_context_t * stem ) {
  if( (sig&0xFFFFFFFFUL)!=ctx->tile_idx ) return 0;

  FD_MGAUGE_SET( EXECRP, PROCESSING, 1UL );

  if( FD_LIKELY( in_idx==ctx->replay_in->idx ) ) {
    if( FD_UNLIKELY( chunk < ctx->replay_in->chunk0 || chunk > ctx->replay_in->wmark ) ) {
      FD_LOG_ERR(( "chunk %lu %lu corrupt, not in range [%lu,%lu]", chunk, sz, ctx->replay_in->chunk0, ctx->replay_in->wmark ));
    }
    switch( sig>>32 ) {
      case FD_EXECRP_TT_TXN_EXEC: {
        /* Execute. */
        fd_execrp_txn_exec_msg_t * msg = fd_chunk_to_laddr( ctx->replay_in->mem, chunk );
        ctx->bank = fd_banks_bank_query( ctx->banks, msg->bank_idx );
        FD_TEST( ctx->bank );
        ctx->txn_in.txn = msg->txn;
        memcpy( ctx->txn_in.fec_merkle_root, msg->fec_merkle_root, 32UL );
        ctx->txn_in.index_in_slot = msg->index_in_slot;

        /* Set the capture txn index from the message so account updates
           during commit are recorded with the correct transaction index. */
        if( FD_UNLIKELY( ctx->capture_ctx ) ) {
          ctx->capture_ctx->current_txn_idx = msg->capture_txn_idx;
        }

        fd_runtime_prepare_and_execute_txn( ctx->runtime, ctx->bank, &ctx->txn_in, &ctx->txn_out );

        ctx->metrics.txn_result[ fd_execle_err_from_runtime_err( ctx->txn_out.err.txn_err ) ]++;

        ctx->txn_idx = msg->txn_idx;
        ctx->slot    = ctx->bank->f.slot;

        /* Upstream FD ordering: commit, then notify replay, then stream
           publishes.  Notifying replay BEFORE commit causes replay to
           create child banks off the current xid before we've finalized
           accounts — accdb rejects the writes because the xid now has
           children.  Stream post-balance emission runs last; meta
           pointers are still valid because acc_pool release happens
           after this returnable_frag returns. */
        ctx->dispatch_time_comp = tspub;

        if( FD_LIKELY( ctx->txn_out.err.is_committable ) ) {
          fd_runtime_commit_txn( ctx->runtime, ctx->bank, &ctx->txn_in, &ctx->txn_out, ctx->report_transaction_diffs );
        } else {
          fd_runtime_cancel_txn( ctx->runtime, ctx->bank, &ctx->txn_in, &ctx->txn_out, ctx->report_transaction_diffs );
        }

        long const txn_end_ticks = fd_tickcount();

        publish_txn_finalized_msg( ctx, stem );
        publish_stream_acct_msgs( ctx, stem );
        publish_stream_txn_msg( ctx, stem );

        /* Update metrics */
        ulong load_start_ticks_dt  = fd_ulong_if( ctx->txn_out.details.check_start_ticks==LONG_MAX  || ctx->txn_out.details.load_start_ticks==LONG_MAX,   0UL, (ulong)( ctx->txn_out.details.check_start_ticks  - ctx->txn_out.details.load_start_ticks ) );
        ulong check_start_ticks_dt = fd_ulong_if( ctx->txn_out.details.exec_start_ticks==LONG_MAX   || ctx->txn_out.details.check_start_ticks==LONG_MAX,  0UL, (ulong)( ctx->txn_out.details.exec_start_ticks   - ctx->txn_out.details.check_start_ticks ) );
        ulong exec_start_ticks_dt  = fd_ulong_if( ctx->txn_out.details.commit_start_ticks==LONG_MAX || ctx->txn_out.details.exec_start_ticks==LONG_MAX,   0UL, (ulong)( ctx->txn_out.details.commit_start_ticks - ctx->txn_out.details.exec_start_ticks ) );
        ulong commit_ticks_dt      = fd_ulong_if( txn_end_ticks==LONG_MAX                           || ctx->txn_out.details.commit_start_ticks==LONG_MAX, 0UL, (ulong)( txn_end_ticks                           - ctx->txn_out.details.commit_start_ticks ) );

        ctx->metrics.txn_load_cum_ticks   += load_start_ticks_dt;
        ctx->metrics.txn_check_cum_ticks  += check_start_ticks_dt;
        ctx->metrics.txn_exec_cum_ticks   += exec_start_ticks_dt;
        ctx->metrics.txn_commit_cum_ticks += commit_ticks_dt;

        break;
      }
      case FD_EXECRP_TT_TXN_SIGVERIFY: {
        fd_execrp_txn_sigverify_msg_t * msg = fd_chunk_to_laddr( ctx->replay_in->mem, chunk );
        int res = fd_executor_txn_verify( msg->txn, ctx->sha_lj );
        fd_execrp_task_done_msg_t * out_msg = fd_chunk_to_laddr( ctx->execrp_replay_out->mem, ctx->execrp_replay_out->chunk );
        out_msg->bank_idx               = msg->bank_idx;
        out_msg->txn_sigverify->txn_idx = msg->txn_idx;
        out_msg->txn_sigverify->err     = (res!=FD_RUNTIME_EXECUTE_SUCCESS);
        fd_stem_publish( stem, ctx->execrp_replay_out->idx, (FD_EXECRP_TT_TXN_SIGVERIFY<<32)|ctx->tile_idx, ctx->execrp_replay_out->chunk, sizeof(*out_msg), 0UL, 0UL, 0UL );
        ctx->execrp_replay_out->chunk = fd_dcache_compact_next( ctx->execrp_replay_out->chunk, sizeof(*out_msg), ctx->execrp_replay_out->chunk0, ctx->execrp_replay_out->wmark );
        ctx->metrics.sigverify_cnt += TXN( msg->txn )->signature_cnt;
        break;
      }
      case FD_EXECRP_TT_POH_HASH: {
        fd_execrp_poh_hash_msg_t * msg = fd_chunk_to_laddr( ctx->replay_in->mem, chunk );
        fd_execrp_task_done_msg_t * out_msg = fd_chunk_to_laddr( ctx->execrp_replay_out->mem, ctx->execrp_replay_out->chunk );
        out_msg->bank_idx           = msg->bank_idx;
        out_msg->poh_hash->mblk_idx = msg->mblk_idx;
        out_msg->poh_hash->hashcnt  = msg->hashcnt;
        fd_sha256_hash_32_repeated( msg->hash, out_msg->poh_hash->hash, msg->hashcnt );
        fd_stem_publish( stem, ctx->execrp_replay_out->idx, (FD_EXECRP_TT_POH_HASH<<32)|ctx->tile_idx, ctx->execrp_replay_out->chunk, sizeof(*out_msg), 0UL, 0UL, 0UL );
        ctx->execrp_replay_out->chunk = fd_dcache_compact_next( ctx->execrp_replay_out->chunk, sizeof(*out_msg), ctx->execrp_replay_out->chunk0, ctx->execrp_replay_out->wmark );
        ctx->metrics.poh_hash_cnt += msg->hashcnt;
        break;
      }
      default: FD_LOG_CRIT(( "unexpected signature %lu", sig ));
    }
  } else FD_LOG_CRIT(( "invalid in_idx %lu", in_idx ));

  FD_MGAUGE_SET( EXECRP, PROCESSING, 0UL );

  return 0;
}

extern FD_TL int fd_wksp_oom_silent;

static void
unprivileged_init( fd_topo_t const *      topo,
                   fd_topo_tile_t const * tile ) {
  void * scratch = fd_topo_obj_laddr( topo, tile->tile_obj_id );

  FD_SCRATCH_ALLOC_INIT( l, scratch );
  fd_execrp_tile_t * ctx    = FD_SCRATCH_ALLOC_APPEND( l, alignof(fd_execrp_tile_t),    sizeof(fd_execrp_tile_t)                             );
  void * _txncache          = FD_SCRATCH_ALLOC_APPEND( l, fd_txncache_align(),          fd_txncache_footprint( tile->execrp.max_live_slots ) );
  void * _accdb             = FD_SCRATCH_ALLOC_APPEND( l, fd_accdb_align(),             fd_accdb_footprint( tile->execrp.max_live_slots )    );
  uchar * pc_scratch        = FD_SCRATCH_ALLOC_APPEND( l, FD_PROGCACHE_SCRATCH_ALIGN,   FD_PROGCACHE_SCRATCH_FOOTPRINT                       );

  void * _capture_ctx = NULL;
  if( FD_UNLIKELY( strlen( tile->execrp.solcap_capture ) ) ) {
    _capture_ctx            = FD_SCRATCH_ALLOC_APPEND( l, fd_capture_ctx_align(),       fd_capture_ctx_footprint()                           );
  }

  void * _dump_proto_ctx = NULL;
  void * _txn_dump_ctx = NULL;
  void * _dumping = NULL;
  if( FD_UNLIKELY( strlen( tile->execrp.dump_proto_dir ) ) ) {
    _dump_proto_ctx         = FD_SCRATCH_ALLOC_APPEND( l, alignof(fd_dump_proto_ctx_t), sizeof(fd_dump_proto_ctx_t)                          );
    _txn_dump_ctx           = FD_SCRATCH_ALLOC_APPEND( l, fd_txn_dump_context_align(),  fd_txn_dump_context_footprint()                      );
    if( FD_UNLIKELY( tile->execrp.dump_instr_to_pb || tile->execrp.dump_syscall_to_pb || tile->execrp.dump_txn_to_pb ) ) {
      _dumping              = FD_SCRATCH_ALLOC_APPEND( l, FD_SPAD_ALIGN,                FD_SPAD_FOOTPRINT( 1UL<<28UL )                       );
    }
  }

  for( ulong i=0UL; i<FD_TXN_ACTUAL_SIG_MAX; i++ ) {
    fd_sha512_t * sha = fd_sha512_join( fd_sha512_new( ctx->sha_mem+i ) );
    FD_TEST( sha );
    ctx->sha_lj[ i ] = sha;
  }

  ctx->txn_in.bundle.is_bundle = 0;
  ctx->tile_idx = tile->kind_id;

  ulong banks_obj_id = fd_pod_queryf_ulong( topo->props, ULONG_MAX, "banks" );
  FD_TEST( banks_obj_id!=ULONG_MAX );

  ctx->banks = fd_banks_join( fd_topo_obj_laddr( topo, banks_obj_id ) );
  FD_TEST( ctx->banks );

  FD_TEST( fd_progcache_join( ctx->progcache, fd_topo_obj_laddr( topo, tile->execrp.progcache_obj_id ), pc_scratch, FD_PROGCACHE_SCRATCH_FOOTPRINT ) );

  void * _txncache_shmem = fd_topo_obj_laddr( topo, tile->execrp.txncache_obj_id );
  fd_txncache_shmem_t * txncache_shmem = fd_txncache_shmem_join( _txncache_shmem );
  FD_TEST( txncache_shmem );
  ctx->txncache = fd_txncache_join( fd_txncache_new( _txncache, txncache_shmem ) );
  FD_TEST( ctx->txncache );

  void * _accdb_shmem = fd_topo_obj_laddr( topo, tile->execrp.accdb_obj_id );
  fd_accdb_shmem_t * accdb_shmem = fd_accdb_shmem_join( _accdb_shmem );
  FD_TEST( accdb_shmem );
  ctx->accdb = fd_accdb_join( fd_accdb_new( _accdb, accdb_shmem, FD_ACCDB_FD_RW, 0UL, NULL ) );
  FD_TEST( ctx->accdb );


  /* First find and setup the in-link from replay to exec. */
  ctx->replay_in->idx = fd_topo_find_tile_in_link( topo, tile, "replay_execrp", 0UL );
  FD_TEST( ctx->replay_in->idx!=ULONG_MAX );
  fd_topo_link_t const * replay_in_link = &topo->links[ tile->in_link_id[ ctx->replay_in->idx ] ];
  ctx->replay_in->mem    = topo->workspaces[ topo->objs[ replay_in_link->dcache_obj_id ].wksp_id ].wksp;
  ctx->replay_in->chunk0 = fd_dcache_compact_chunk0( ctx->replay_in->mem, replay_in_link->dcache );
  ctx->replay_in->wmark  = fd_dcache_compact_wmark( ctx->replay_in->mem, replay_in_link->dcache, replay_in_link->mtu );
  ctx->replay_in->chunk  = ctx->replay_in->chunk0;

  ctx->execrp_replay_out->idx = fd_topo_find_tile_out_link( topo, tile, "execrp_replay", ctx->tile_idx );
  if( FD_LIKELY( ctx->execrp_replay_out->idx!=ULONG_MAX ) ) {
    fd_topo_link_t const * execrp_replay_link = &topo->links[ tile->out_link_id[ ctx->execrp_replay_out->idx ] ];
    ctx->execrp_replay_out->mem    = topo->workspaces[ topo->objs[ execrp_replay_link->dcache_obj_id ].wksp_id ].wksp;
    ctx->execrp_replay_out->chunk0 = fd_dcache_compact_chunk0( ctx->execrp_replay_out->mem, execrp_replay_link->dcache );
    ctx->execrp_replay_out->wmark  = fd_dcache_compact_wmark( ctx->execrp_replay_out->mem, execrp_replay_link->dcache, execrp_replay_link->mtu );
    ctx->execrp_replay_out->chunk  = ctx->execrp_replay_out->chunk0;
  }

  /* Stream output link — optional, only present when stream tile is enabled. */
  ctx->stream_out->idx = fd_topo_find_tile_out_link( topo, tile, "execrp_strm", ctx->tile_idx );
  ctx->stream_enabled  = ( ctx->stream_out->idx!=ULONG_MAX );
  ctx->write_version_seq = ctx->tile_idx * (1UL<<40); /* Ensure non-overlapping ranges across tiles */
  if( FD_UNLIKELY( ctx->stream_enabled ) ) {
    fd_topo_link_t const * stream_link = &topo->links[ tile->out_link_id[ ctx->stream_out->idx ] ];
    ctx->stream_out->mem    = topo->workspaces[ topo->objs[ stream_link->dcache_obj_id ].wksp_id ].wksp;
    ctx->stream_out->chunk0 = fd_dcache_compact_chunk0( ctx->stream_out->mem, stream_link->dcache );
    ctx->stream_out->wmark  = fd_dcache_compact_wmark( ctx->stream_out->mem, stream_link->dcache, stream_link->mtu );
    ctx->stream_out->chunk  = ctx->stream_out->chunk0;
  }

  ctx->capture_ctx = NULL;
  if( FD_UNLIKELY( strlen( tile->execrp.solcap_capture ) ) ) {
    ctx->capture_ctx = fd_capture_ctx_join( fd_capture_ctx_new( _capture_ctx ) );
    ctx->capture_ctx->solcap_start_slot = tile->execrp.capture_start_slot;

    ulong tile_idx = tile->kind_id;
    ulong idx = fd_topo_find_tile_out_link( topo, tile, "cap_execrp", tile_idx );
    FD_TEST( idx!=ULONG_MAX );

    fd_topo_link_t const * link = &topo->links[ tile->out_link_id[ idx ] ];
    fd_capture_link_buf_t * cap_execrp_out = ctx->cap_execrp_out;
    cap_execrp_out->base.vt = &fd_capture_link_buf_vt;
    cap_execrp_out->idx     = idx;
    cap_execrp_out->mem     = topo->workspaces[ topo->objs[ link->dcache_obj_id ].wksp_id ].wksp;
    cap_execrp_out->chunk0  = fd_dcache_compact_chunk0( cap_execrp_out->mem, link->dcache );
    cap_execrp_out->wmark   = fd_dcache_compact_wmark( cap_execrp_out->mem, link->dcache, link->mtu );
    cap_execrp_out->chunk   = cap_execrp_out->chunk0;
    cap_execrp_out->mcache  = link->mcache;
    cap_execrp_out->depth   = fd_mcache_depth( link->mcache );
    cap_execrp_out->seq     = 0UL;

    ulong consumer_tile_idx = fd_topo_find_tile(topo, "solcap", 0UL);
    fd_topo_tile_t const * consumer_tile = &topo->tiles[ consumer_tile_idx ];
    cap_execrp_out->fseq = NULL;
    for( ulong j = 0UL; j < consumer_tile->in_cnt; j++ ) {
      if( FD_UNLIKELY( consumer_tile->in_link_id[ j ]  == link->id ) ) {
        cap_execrp_out->fseq = fd_fseq_join( fd_topo_obj_laddr( topo, consumer_tile->in_link_fseq_obj_id[ j ] ) );
        FD_TEST( cap_execrp_out->fseq );
        break;
      }
    }

    ctx->capture_ctx->capture_solcap  = 1;
    ctx->capture_ctx->capctx_type.buf = cap_execrp_out;
    ctx->capture_ctx->capture_link    = &cap_execrp_out->base;
  }

  ctx->dump_proto_ctx = NULL;
  if( FD_UNLIKELY( strlen( tile->execrp.dump_proto_dir ) ) ) {
    ctx->dump_proto_ctx = _dump_proto_ctx;

    /* General dumping config */
    ctx->dump_proto_ctx->dump_proto_output_dir = tile->execrp.dump_proto_dir;
    ctx->dump_proto_ctx->dump_proto_start_slot = tile->execrp.capture_start_slot;

    /* Syscall dumping config */
    ctx->dump_proto_ctx->dump_syscall_to_pb       = !!tile->execrp.dump_syscall_to_pb;
    ctx->dump_proto_ctx->dump_syscall_name_filter = tile->execrp.dump_syscall_name_filter;

    /* Instruction dumping config */
    ctx->dump_proto_ctx->dump_instr_to_pb                 = !!tile->execrp.dump_instr_to_pb;
    ctx->dump_proto_ctx->has_dump_instr_program_id_filter = !!strlen(tile->execrp.dump_instr_program_id_filter);
    if( FD_UNLIKELY( ctx->dump_proto_ctx->has_dump_instr_program_id_filter &&
                     !fd_base58_decode_32( tile->execrp.dump_instr_program_id_filter, ctx->dump_proto_ctx->dump_instr_program_id_filter ) ) ) {
      FD_LOG_ERR(( "failed to parse [capture.dump_instr_program_id_filter] %s", tile->execrp.dump_instr_program_id_filter ));
    }

    /* Transaction dumping config */
    ctx->dump_proto_ctx->dump_txn_to_pb      = !!tile->execrp.dump_txn_to_pb;
    ctx->dump_proto_ctx->dump_txn_as_fixture = !!tile->execrp.dump_txn_as_fixture;

    if( FD_UNLIKELY( ctx->dump_proto_ctx->dump_txn_as_fixture && !ctx->dump_proto_ctx->dump_txn_to_pb ) ) {
      FD_LOG_ERR(( "[capture.dump_txn_as_fixture] requires [capture.dump_txn_to_pb] to be enabled" ));
    }
  }

  /* Transaction dump context (for fixture dumping) */
  ctx->txn_dump_ctx = NULL;
  if( FD_UNLIKELY( ctx->dump_proto_ctx && ctx->dump_proto_ctx->dump_txn_to_pb ) ) {
    ctx->txn_dump_ctx = fd_txn_dump_context_join( fd_txn_dump_context_new( _txn_dump_ctx ) );
  }

  ctx->runtime->accdb                    = ctx->accdb;
  ctx->runtime->progcache                = ctx->progcache;
  ctx->runtime->status_cache             = ctx->txncache;
  memset( &ctx->runtime->log, 0, sizeof(ctx->runtime->log) );
  ctx->runtime->log.log_collector        = &ctx->log_collector;
  ctx->runtime->log.enable_log_collector = 1; /* Enable logs for stream tile log_messages emission */
  ctx->runtime->log.dumping_mem          = _dumping;
  ctx->runtime->log.tracing_mem          = NULL;
  ctx->runtime->log.capture_ctx          = ctx->capture_ctx;
  ctx->runtime->log.dump_proto_ctx       = ctx->dump_proto_ctx;
  ctx->runtime->log.txn_dump_ctx         = ctx->txn_dump_ctx;
  ctx->runtime->fuzz.enabled             = 0;
  ctx->runtime->fuzz.reclaim_accounts    = 0;
  ctx->runtime->accounts.executable_cnt  = 0UL;
  ctx->runtime->accounts.account_cnt     = 0UL;

  memset( &ctx->metrics,          0, sizeof(ctx->metrics)          );
  memset( &ctx->runtime->metrics, 0, sizeof(ctx->runtime->metrics) );

  ctx->report_transaction_diffs = tile->execrp.report_transaction_diffs;

  fd_wksp_oom_silent = 1;

  ulong scratch_top = FD_SCRATCH_ALLOC_FINI( l, scratch_align() );
  if( FD_UNLIKELY( scratch_top > (ulong)scratch + scratch_footprint( tile ) ) )
    FD_LOG_ERR(( "scratch overflow %lu %lu %lu", scratch_top - (ulong)scratch - scratch_footprint( tile ), scratch_top, (ulong)scratch + scratch_footprint( tile ) ));

  fd_sleep_until_replay_started( topo );
}

static ulong
populate_allowed_seccomp( fd_topo_t const *      topo FD_PARAM_UNUSED,
                          fd_topo_tile_t const * tile FD_PARAM_UNUSED,
                          ulong                  out_cnt,
                          struct sock_filter *   out ) {
  populate_sock_filter_policy_fd_execrp_tile( out_cnt, out, (uint)fd_log_private_logfile_fd(), (uint)FD_ACCDB_FD_RW );
  return sock_filter_policy_fd_execrp_tile_instr_cnt;
}

static ulong
populate_allowed_fds( fd_topo_t const *      topo FD_PARAM_UNUSED,
                      fd_topo_tile_t const * tile FD_PARAM_UNUSED,
                      ulong                  out_fds_cnt,
                      int *                  out_fds ) {

  if( FD_UNLIKELY( out_fds_cnt<3UL ) ) FD_LOG_ERR(( "out_fds_cnt %lu", out_fds_cnt ));

  ulong out_cnt = 0UL;
  out_fds[ out_cnt++ ] = 2; /* stderr */
  if( FD_LIKELY( -1!=fd_log_private_logfile_fd() ) ) {
    out_fds[ out_cnt++ ] = fd_log_private_logfile_fd(); /* logfile */
  }
  out_fds[ out_cnt++ ] = FD_ACCDB_FD_RW; /* accounts db */

  return out_cnt;
}

#define STEM_BURST (1UL)

/* Right now, depth of the replay_exec link and depth of the execrp_replay
   links is 16K.  At 1M TPS, that's ~16ms to fill.  But we also want to
   be conservative here, so we use 1ms. */
#define STEM_LAZY  (1000000UL)

#define STEM_CALLBACK_CONTEXT_TYPE  fd_execrp_tile_t
#define STEM_CALLBACK_CONTEXT_ALIGN alignof(fd_execrp_tile_t)

#define STEM_CALLBACK_METRICS_WRITE   metrics_write
#define STEM_CALLBACK_RETURNABLE_FRAG returnable_frag

#include "../../disco/stem/fd_stem.c"

static ulong
max_event_sz( fd_topo_tile_t const * tile ) {
  /* execrp emits accdb_partition_added, plus runtime_txn when diffs are on. */
  ulong sz = sizeof(fd_event_accdb_partition_added_t);
  if( tile->execrp.report_transaction_diffs && sizeof(fd_event_runtime_txn_t)>sz ) sz = sizeof(fd_event_runtime_txn_t);
  return sz;
}

fd_topo_run_tile_t fd_tile_execrp = {
  .name                     = "execrp",
  .max_event_sz             = max_event_sz,
  .loose_footprint          = 0UL,
  .populate_allowed_seccomp = populate_allowed_seccomp,
  .populate_allowed_fds     = populate_allowed_fds,
  .scratch_align            = scratch_align,
  .scratch_footprint        = scratch_footprint,
  .unprivileged_init        = unprivileged_init,
  .run                      = stem_run,
};
