#include "fd_stream_tile.h"
#include "fd_stream_msg.h"
#include "fd_stream_proto.h"
#include "fd_stream_grpc.h"

#include "../../disco/fd_disco_base.h"
#include "../../disco/metrics/fd_metrics.h"
#include "../../disco/topo/fd_topo.h"
#include "../../ballet/pb/fd_pb_encode.h"
#include "../replay/fd_replay_tile.h"

#include <sys/socket.h>

#include "generated/fd_stream_tile_seccomp.h"

/* The stream tile receives executed transaction data from execrp tiles
   and serves it to external consumers (Laserstream) via gRPC.

   Data flow:
     execrp tiles → execrp_strm links → STREAM TILE → gRPC clients

   The stream tile is an UNRELIABLE consumer of all execrp_strm links.
   If it falls behind, messages are dropped at the mcache level (the
   execrp tiles are never backpressured).

   Protobuf encoding and gRPC sending happen directly in after_frag,
   with no intermediate buffering.  The UNRELIABLE link guarantees
   we never backpressure execution.

   The gRPC server is polled in before_credit. */

/* Input link types */
#define STREAM_IN_EXECRP  (0)
#define STREAM_IN_TOWER   (1)
#define STREAM_IN_REPLAY  (2)

typedef struct {
  fd_wksp_t * mem;
  ulong       chunk0;
  ulong       wmark;
  ulong       mtu;
  int         link_type;      /* STREAM_IN_EXECRP, STREAM_IN_TOWER, STREAM_IN_REPLAY */
} fd_stream_in_ctx_t;

struct fd_stream_tile {
  /* Input link contexts */
  fd_stream_in_ctx_t in[ 64UL ];
  ulong              in_cnt;

  /* gRPC server */
  fd_stream_grpc_t grpc[1];

  /* Protobuf encode buffer — large enough for any single transaction */
  uchar pb_buf[ FD_STREAM_GRPC_MAX_MSG_SZ + 32UL ];

  /* Staging buffer for copying the stream message from dcache */
  uchar msg_buf[ USHORT_MAX ];

  /* Last received sig for tower/replay message dispatch */
  ulong last_sig;
  int   last_link_type;

  /* Ping keepalive */
  long  last_ping_ns;

  /* Reward accumulation buffer for block_meta encoding */
  fd_stream_reward_t pending_rewards[ 4096 ];
  ulong              pending_rewards_cnt;
  ulong              pending_rewards_slot;

  /* Metrics */
  struct {
    ulong txns_received;
    ulong txns_encoded;
    ulong txns_sent;
    ulong txns_dropped;
  } metrics;
};

typedef struct fd_stream_tile fd_stream_tile_t;

FD_FN_CONST static inline ulong
scratch_align( void ) {
  return 128UL;
}

FD_FN_PURE static inline ulong
scratch_footprint( fd_topo_tile_t const * tile ) {
  (void)tile;
  ulong l = FD_LAYOUT_INIT;
  l = FD_LAYOUT_APPEND( l, alignof(fd_stream_tile_t), sizeof(fd_stream_tile_t) );
  return FD_LAYOUT_FINI( l, scratch_align() );
}

static inline void
metrics_write( fd_stream_tile_t * ctx ) {
  (void)ctx;
  /* TODO: publish metrics */
}

/* before_credit is called every stem loop iteration.  Poll the gRPC
   server for new connections and H2 frames. */

static void
before_credit( fd_stream_tile_t *  ctx,
               fd_stem_context_t * stem FD_PARAM_UNUSED,
               int *               charge_busy ) {

  /* Drive the gRPC server (accept connections, read H2 frames) */
  int grpc_busy = fd_stream_grpc_poll( ctx->grpc );
  if( grpc_busy ) *charge_busy = 1;

  /* Send periodic ping to keep connection alive (every 5 seconds) */
  if( FD_LIKELY( fd_stream_grpc_is_streaming( ctx->grpc ) ) ) {
    long now_ns = fd_log_wallclock();
    if( FD_UNLIKELY( now_ns - ctx->last_ping_ns > (long)5e9 ) ) {
      ctx->last_ping_ns = now_ns;
      /* Encode SubscribeUpdate with ping (field 6, empty submsg) */
      fd_pb_encoder_t _enc[1];
      fd_pb_encoder_t * enc = fd_pb_encoder_init( _enc, ctx->pb_buf, sizeof(ctx->pb_buf) );
      fd_pb_submsg_open( enc, 6U /* SubscribeUpdatePing */ );
      fd_pb_submsg_close( enc );
      ulong pb_sz = fd_pb_encoder_out_sz( enc );
      fd_stream_grpc_send( ctx->grpc, ctx->pb_buf, pb_sz );
    }
  }
}

/* before_frag filters out messages we don't need.  The stream tile gets
   txn execution information via execrp_strm, so REPLAY_SIG_TXN_EXECUTED
   messages on replay_out are redundant and can be skipped to avoid
   wasted decode work. */

static inline int
before_frag( fd_stream_tile_t * ctx,
             ulong              in_idx,
             ulong              seq FD_PARAM_UNUSED,
             ulong              sig ) {
  if( FD_UNLIKELY( ctx->in[ in_idx ].link_type==STREAM_IN_REPLAY &&
                   sig==REPLAY_SIG_TXN_EXECUTED ) ) return -1; /* skip */
  return 0;
}

/* during_frag is called when a new fragment is detected on an input
   link.  We copy the fd_stream_txn_msg_t from the dcache into a
   local staging buffer. */

static inline void
during_frag( fd_stream_tile_t * ctx,
             ulong              in_idx,
             ulong              seq FD_PARAM_UNUSED,
             ulong              sig,
             ulong              chunk,
             ulong              sz,
             ulong              ctl FD_PARAM_UNUSED ) {
  if( FD_UNLIKELY( chunk<ctx->in[ in_idx ].chunk0 || chunk>ctx->in[ in_idx ].wmark || sz>ctx->in[ in_idx ].mtu ) )
    FD_LOG_ERR(( "chunk %lu %lu corrupt, not in range [%lu,%lu]", chunk, sz, ctx->in[ in_idx ].chunk0, ctx->in[ in_idx ].wmark ));

  uchar const * src = (uchar const *)fd_chunk_to_laddr_const( ctx->in[ in_idx ].mem, chunk );

  fd_memcpy( ctx->msg_buf, src, sz );
  ctx->last_sig       = sig;
  ctx->last_link_type = ctx->in[ in_idx ].link_type;
  ctx->metrics.txns_received++;
}

/* Helper: encode and send a protobuf message to the gRPC client.
   Returns 1 if sent, 0 if dropped or no client. */
static inline int
stream_send( fd_stream_tile_t * ctx, ulong pb_sz ) {
  if( FD_UNLIKELY( !pb_sz ) ) { ctx->metrics.txns_dropped++; return 0; }
  ctx->metrics.txns_encoded++;
  if( FD_LIKELY( fd_stream_grpc_is_streaming( ctx->grpc ) ) ) {
    if( FD_LIKELY( fd_stream_grpc_send( ctx->grpc, ctx->pb_buf, pb_sz ) ) ) {
      ctx->metrics.txns_sent++;
      return 1;
    }
    ctx->metrics.txns_dropped++;
  }
  return 0;
}

/* after_frag is called after verifying no overrun occurred.  We encode
   the staged message as Yellowstone protobuf and send it directly to
   the gRPC client. */

static inline void
after_frag( fd_stream_tile_t *  ctx,
            ulong               in_idx FD_PARAM_UNUSED,
            ulong               seq    FD_PARAM_UNUSED,
            ulong               sig    FD_PARAM_UNUSED,
            ulong               sz     FD_PARAM_UNUSED,
            ulong               tsorig FD_PARAM_UNUSED,
            ulong               tspub  FD_PARAM_UNUSED,
            fd_stem_context_t * stem   FD_PARAM_UNUSED ) {

  ulong pb_sz = 0;
  int ok = 0;

  if( FD_LIKELY( ctx->last_link_type==STREAM_IN_EXECRP ) ) {
    uchar msg_type = ctx->msg_buf[0];
    if( FD_LIKELY( msg_type==FD_STREAM_MSG_TYPE_TXN ) ) {
      fd_stream_txn_msg_t const * msg = (fd_stream_txn_msg_t const *)ctx->msg_buf;
      ok = fd_stream_encode_txn_update( ctx->pb_buf, sizeof(ctx->pb_buf), msg, &pb_sz );
    } else if( FD_LIKELY( msg_type==FD_STREAM_MSG_TYPE_ACCT ) ) {
      fd_stream_acct_msg_t const * msg = (fd_stream_acct_msg_t const *)ctx->msg_buf;
      ok = fd_stream_encode_acct_update( ctx->pb_buf, sizeof(ctx->pb_buf), msg, &pb_sz );
    }
    if( FD_UNLIKELY( !ok ) ) { ctx->metrics.txns_dropped++; return; }
    stream_send( ctx, pb_sz );

  } else if( ctx->last_link_type==STREAM_IN_TOWER ) {
    ok = fd_stream_encode_tower_msg( ctx->pb_buf, sizeof(ctx->pb_buf),
                                     ctx->msg_buf, ctx->last_sig, &pb_sz );
    if( FD_UNLIKELY( !ok ) ) { ctx->metrics.txns_dropped++; return; }
    stream_send( ctx, pb_sz );

  } else if( ctx->last_link_type==STREAM_IN_REPLAY ) {
    if( ctx->last_sig==REPLAY_SIG_REWARDS ) {
      /* Accumulate rewards for the upcoming block_meta */
      fd_replay_rewards_batch_t const * batch = (fd_replay_rewards_batch_t const *)ctx->msg_buf;
      if( batch->slot != ctx->pending_rewards_slot ) {
        ctx->pending_rewards_cnt  = 0;
        ctx->pending_rewards_slot = batch->slot;
      }
      for( ulong i=0; i<batch->cnt && ctx->pending_rewards_cnt<4096; i++ ) {
        ctx->pending_rewards[ ctx->pending_rewards_cnt++ ] = batch->rewards[i];
      }
    } else if( ctx->last_sig==REPLAY_SIG_SLOT_COMPLETED ) {
      /* 1. Block meta (with accumulated rewards) */
      ok = fd_stream_encode_replay_msg_with_rewards(
          ctx->pb_buf, sizeof(ctx->pb_buf), ctx->msg_buf,
          ctx->pending_rewards, ctx->pending_rewards_cnt, &pb_sz );
      if( ok ) stream_send( ctx, pb_sz );
      ctx->pending_rewards_cnt = 0;

      /* 2. Processed slot update with parent_slot */
      fd_replay_slot_completed_t const * sc = (fd_replay_slot_completed_t const *)ctx->msg_buf;
      ok = fd_stream_encode_slot_update( ctx->pb_buf, sizeof(ctx->pb_buf),
                                         sc->slot, sc->parent_slot, 0 /* PROCESSED */, &pb_sz );
      if( ok ) stream_send( ctx, pb_sz );
    } else {
      ok = fd_stream_encode_replay_msg( ctx->pb_buf, sizeof(ctx->pb_buf),
                                        ctx->msg_buf, ctx->last_sig, &pb_sz );
      if( FD_UNLIKELY( !ok ) ) { ctx->metrics.txns_dropped++; return; }
      stream_send( ctx, pb_sz );
    }
  }
  return;
}

static void
unprivileged_init( fd_topo_t *      topo,
                   fd_topo_tile_t * tile ) {
  void * scratch = fd_topo_obj_laddr( topo, tile->tile_obj_id );

  FD_SCRATCH_ALLOC_INIT( l, scratch );
  fd_stream_tile_t * ctx = FD_SCRATCH_ALLOC_APPEND( l, alignof(fd_stream_tile_t), sizeof(fd_stream_tile_t) );
  ulong scratch_top      = FD_SCRATCH_ALLOC_FINI( l, scratch_align() );

  if( FD_UNLIKELY( scratch_top > (ulong)scratch + scratch_footprint( tile ) ) )
    FD_LOG_ERR(( "scratch overflow" ));

  /* Initialize input link contexts */
  ctx->in_cnt = 0UL;
  for( ulong i=0UL; i<tile->in_cnt; i++ ) {
    if( FD_UNLIKELY( !tile->in_link_poll[ i ] ) ) continue;

    fd_topo_link_t * link     = &topo->links[ tile->in_link_id[ i ] ];
    fd_topo_wksp_t * link_wksp = &topo->workspaces[ topo->objs[ link->dcache_obj_id ].wksp_id ];

    ctx->in[ ctx->in_cnt ].mem    = link_wksp->wksp;
    ctx->in[ ctx->in_cnt ].mtu    = link->mtu;
    ctx->in[ ctx->in_cnt ].chunk0 = fd_dcache_compact_chunk0( ctx->in[ ctx->in_cnt ].mem, link->dcache );
    ctx->in[ ctx->in_cnt ].wmark  = fd_dcache_compact_wmark ( ctx->in[ ctx->in_cnt ].mem, link->dcache, link->mtu );

    /* Tag link type based on link name */
    if( !strcmp( link->name, "tower_out" ) ) {
      ctx->in[ ctx->in_cnt ].link_type = STREAM_IN_TOWER;
    } else if( !strcmp( link->name, "replay_out" ) ) {
      ctx->in[ ctx->in_cnt ].link_type = STREAM_IN_REPLAY;
    } else {
      ctx->in[ ctx->in_cnt ].link_type = STREAM_IN_EXECRP;
    }
    ctx->in_cnt++;
  }

  /* Metrics */
  memset( &ctx->metrics, 0, sizeof(ctx->metrics) );

  /* Initialize gRPC server on the configured port. */
  ushort listen_port = tile->stream.listen_port;
  if( FD_UNLIKELY( !listen_port ) ) listen_port = 10102; /* fallback */
  if( FD_UNLIKELY( !fd_stream_grpc_init( ctx->grpc, 0 /* INADDR_ANY */, listen_port ) ) ) {
    FD_LOG_ERR(( "failed to initialize gRPC server on port %hu", listen_port ));
  }
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
  if( FD_UNLIKELY( out_fds_cnt<2UL ) ) FD_LOG_ERR(( "out_fds_cnt %lu", out_fds_cnt ));

  ulong out_cnt = 0UL;
  out_fds[ out_cnt++ ] = 2; /* stderr */
  if( FD_LIKELY( -1!=fd_log_private_logfile_fd() ) )
    out_fds[ out_cnt++ ] = fd_log_private_logfile_fd(); /* logfile */
  return out_cnt;
}

#define STEM_BURST (16UL)  /* v42 best config */
#define STEM_LAZY  (128L*200L)  /* 25.6μs — aggressive polling for low-latency streaming */

#define STEM_CALLBACK_CONTEXT_TYPE  fd_stream_tile_t
#define STEM_CALLBACK_CONTEXT_ALIGN alignof(fd_stream_tile_t)

#define STEM_CALLBACK_METRICS_WRITE metrics_write
#define STEM_CALLBACK_BEFORE_CREDIT before_credit
#define STEM_CALLBACK_BEFORE_FRAG   before_frag
#define STEM_CALLBACK_DURING_FRAG   during_frag
#define STEM_CALLBACK_AFTER_FRAG    after_frag

#include "../../disco/stem/fd_stem.c"

fd_topo_run_tile_t fd_tile_stream = {
  .name                     = "stream",
  .populate_allowed_seccomp = populate_allowed_seccomp,
  .populate_allowed_fds     = populate_allowed_fds,
  .scratch_align            = scratch_align,
  .scratch_footprint        = scratch_footprint,
  .unprivileged_init        = unprivileged_init,
  .run                      = stem_run,
};
