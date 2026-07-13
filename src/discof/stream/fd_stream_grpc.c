#include "fd_stream_grpc.h"
#include "../../waltz/h2/fd_h2_rbuf.h"
#include "../../waltz/h2/fd_h2_rbuf_sock.h"

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

/* gRPC server using waltz/h2.  Follows the test_h2_server.c pattern
   but adapted for non-blocking sockets in a tile polling loop. */

static fd_stream_grpc_t * g_grpc;

static void
grpc_close_conn( fd_stream_grpc_t * grpc ) {
  if( grpc->conn_fd>=0 ) close( grpc->conn_fd );
  grpc->conn_fd          = -1;
  grpc->state            = FD_STREAM_GRPC_STATE_LISTENING;
  grpc->active_stream_id = 0;
  memset( grpc->h2_stream, 0, sizeof(grpc->h2_stream) );
  memset( grpc->tx_op,     0, sizeof(grpc->tx_op) );
}

/* Try to flush rbuf_tx to socket. Returns 1 if all flushed, 0 if would block. */
static int
grpc_flush_tx( fd_stream_grpc_t * grpc ) {
  while( fd_h2_rbuf_used_sz( grpc->rbuf_tx ) ) {
    int err = fd_h2_rbuf_sendmsg( grpc->rbuf_tx, grpc->conn_fd, MSG_NOSIGNAL | MSG_DONTWAIT );
    if( err==EAGAIN || err==EWOULDBLOCK ) return 0;
    if( FD_UNLIKELY( err ) ) {
      FD_LOG_NOTICE(( "stream tile: sendmsg failed (%i)", err ));
      grpc_close_conn( grpc );
      return 0;
    }
  }
  return 1;
}

/* Try to read from socket into rbuf_rx. Returns 1 if data read, 0 if nothing. */
static int
grpc_recv( fd_stream_grpc_t * grpc ) {
  int err = fd_h2_rbuf_recvmsg( grpc->rbuf_rx, grpc->conn_fd, MSG_NOSIGNAL | MSG_DONTWAIT );
  if( FD_UNLIKELY( err==EPIPE ) ) {
    FD_LOG_NOTICE(( "stream tile: client disconnected" ));
    grpc_close_conn( grpc );
    return 0;
  }
  if( err ) return 0; /* EAGAIN or error */
  return fd_h2_rbuf_used_sz( grpc->rbuf_rx ) > 0;
}

/* H2 callbacks */

static fd_h2_stream_t *
cb_stream_create( fd_h2_conn_t * conn FD_PARAM_UNUSED,
                  uint           stream_id FD_PARAM_UNUSED ) {
  fd_stream_grpc_t * grpc = g_grpc;
  fd_h2_stream_t * stream = grpc->h2_stream;
  if( FD_UNLIKELY( stream->stream_id ) ) return NULL;
  fd_h2_stream_init( stream );
  return stream;
}

static fd_h2_stream_t *
cb_stream_query( fd_h2_conn_t * conn FD_PARAM_UNUSED,
                 uint           stream_id ) {
  fd_stream_grpc_t * grpc = g_grpc;
  fd_h2_stream_t * stream = grpc->h2_stream;
  if( stream->stream_id!=stream_id ) return NULL;
  return stream;
}

static void
cb_conn_established( fd_h2_conn_t * conn FD_PARAM_UNUSED ) {
  FD_LOG_NOTICE(( "stream tile: H2 connection established" ));
}

static void
cb_conn_final( fd_h2_conn_t * conn FD_PARAM_UNUSED,
               uint           h2_err,
               int            closed_by FD_PARAM_UNUSED ) {
  FD_LOG_NOTICE(( "stream tile: H2 connection closed (%u-%s)", h2_err, fd_h2_strerror( h2_err ) ));
  grpc_close_conn( g_grpc );
}

static void
cb_headers( fd_h2_conn_t *   conn FD_PARAM_UNUSED,
            fd_h2_stream_t * stream,
            void const *     data    FD_PARAM_UNUSED,
            ulong            data_sz FD_PARAM_UNUSED,
            ulong            flags ) {
  if( flags & FD_H2_FLAG_END_STREAM ) {
    /* Client sent headers-only request. Send response headers and start streaming. */
    fd_stream_grpc_t * grpc = g_grpc;
    uchar hpack[26];
    hpack[0] = 0x88; /* :status: 200 */
    hpack[1] = 0x0F; hpack[2] = 0x10; hpack[3] = 22;
    memcpy( hpack+4, "application/grpc+proto", 22 );
    fd_h2_tx( grpc->rbuf_tx, hpack, 26UL,
              FD_H2_FRAME_TYPE_HEADERS, FD_H2_FLAG_END_HEADERS,
              stream->stream_id );
    grpc->active_stream_id = stream->stream_id;
    grpc->state = FD_STREAM_GRPC_STATE_STREAMING;
    FD_LOG_NOTICE(( "stream tile: gRPC stream %u active (headers-only)", stream->stream_id ));
  }
}

static void
cb_data( fd_h2_conn_t *   conn FD_PARAM_UNUSED,
         fd_h2_stream_t * stream,
         void const *     data    FD_PARAM_UNUSED,
         ulong            data_sz FD_PARAM_UNUSED,
         ulong            flags ) {
  if( flags & FD_H2_FLAG_END_STREAM ) {
    fd_stream_grpc_t * grpc = g_grpc;
    uchar hpack[26];
    hpack[0] = 0x88;
    hpack[1] = 0x0F; hpack[2] = 0x10; hpack[3] = 22;
    memcpy( hpack+4, "application/grpc+proto", 22 );
    fd_h2_tx( grpc->rbuf_tx, hpack, 26UL,
              FD_H2_FRAME_TYPE_HEADERS, FD_H2_FLAG_END_HEADERS,
              stream->stream_id );
    grpc->active_stream_id = stream->stream_id;
    grpc->state = FD_STREAM_GRPC_STATE_STREAMING;
    FD_LOG_NOTICE(( "stream tile: gRPC stream %u active", stream->stream_id ));
  }
}

static void cb_rst_stream( fd_h2_conn_t * conn FD_PARAM_UNUSED, fd_h2_stream_t * stream FD_PARAM_UNUSED, uint error_code FD_PARAM_UNUSED, int closed_by FD_PARAM_UNUSED ) {
  fd_stream_grpc_t * grpc = g_grpc;
  memset( grpc->h2_stream, 0, sizeof(grpc->h2_stream) );
  memset( grpc->tx_op, 0, sizeof(grpc->tx_op) );
  if( grpc->state==FD_STREAM_GRPC_STATE_STREAMING ) grpc->state = FD_STREAM_GRPC_STATE_ACTIVE;
}

static void cb_window_update( fd_h2_conn_t * conn FD_PARAM_UNUSED, uint delta FD_PARAM_UNUSED ) {
  fd_stream_grpc_t * grpc = g_grpc;
  if( grpc->h2_stream->stream_id && grpc->tx_op->chunk_sz )
    fd_h2_tx_op_copy( grpc->h2_conn, grpc->h2_stream, grpc->rbuf_tx, grpc->tx_op );
}

static void cb_stream_window_update( fd_h2_conn_t * conn FD_PARAM_UNUSED, fd_h2_stream_t * stream FD_PARAM_UNUSED, uint delta FD_PARAM_UNUSED ) {
  fd_stream_grpc_t * grpc = g_grpc;
  if( grpc->h2_stream->stream_id && grpc->tx_op->chunk_sz )
    fd_h2_tx_op_copy( grpc->h2_conn, grpc->h2_stream, grpc->rbuf_tx, grpc->tx_op );
}

static void cb_ping_ack( fd_h2_conn_t * conn FD_PARAM_UNUSED ) {}

int
fd_stream_grpc_init( fd_stream_grpc_t * grpc,
                     uint               listen_addr,
                     ushort             listen_port ) {
  memset( grpc, 0, sizeof(*grpc) );
  grpc->listen_fd  = -1;
  grpc->conn_fd    = -1;
  grpc->state      = FD_STREAM_GRPC_STATE_LISTENING;

  fd_h2_callbacks_init( grpc->h2_cb );
  grpc->h2_cb->stream_create        = cb_stream_create;
  grpc->h2_cb->stream_query         = cb_stream_query;
  grpc->h2_cb->conn_established     = cb_conn_established;
  grpc->h2_cb->conn_final           = cb_conn_final;
  grpc->h2_cb->headers              = cb_headers;
  grpc->h2_cb->data                 = cb_data;
  grpc->h2_cb->rst_stream           = cb_rst_stream;
  grpc->h2_cb->window_update        = cb_window_update;
  grpc->h2_cb->stream_window_update = cb_stream_window_update;
  grpc->h2_cb->ping_ack             = cb_ping_ack;

  int fd = socket( AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP );
  if( FD_UNLIKELY( fd<0 ) ) return 0;

  int optval = 1;
  setsockopt( fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval) );
  setsockopt( fd, IPPROTO_TCP, TCP_NODELAY, &optval, sizeof(optval) );

  struct sockaddr_in addr;
  memset( &addr, 0, sizeof(addr) );
  addr.sin_family      = AF_INET;
  addr.sin_port        = fd_ushort_bswap( listen_port );
  addr.sin_addr.s_addr = listen_addr;

  if( FD_UNLIKELY( bind( fd, fd_type_pun( &addr ), sizeof(addr) )<0 ) ) {
    close( fd ); return 0;
  }
  if( FD_UNLIKELY( listen( fd, 1 )<0 ) ) {
    close( fd ); return 0;
  }

  grpc->listen_fd = fd;
  FD_LOG_NOTICE(( "stream tile: gRPC server listening on port %hu", listen_port ));
  return 1;
}

int
fd_stream_grpc_poll( fd_stream_grpc_t * grpc ) {
  if( FD_UNLIKELY( grpc->listen_fd<0 ) ) return 0;
  g_grpc = grpc;

  /* === LISTENING: accept new connection === */
  if( grpc->state==FD_STREAM_GRPC_STATE_LISTENING ) {
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);
    int conn_fd = accept( grpc->listen_fd, fd_type_pun( &client_addr ), &client_len );
    if( conn_fd<0 ) return 0;

    int flags = fcntl( conn_fd, F_GETFL, 0 );
    fcntl( conn_fd, F_SETFL, flags | O_NONBLOCK );
    int optval = 1;
    setsockopt( conn_fd, IPPROTO_TCP, TCP_NODELAY, &optval, sizeof(optval) );

    grpc->conn_fd    = conn_fd;
    grpc->state      = FD_STREAM_GRPC_STATE_PREFACE;
    grpc->preface_sz = 0;
    FD_LOG_NOTICE(( "stream tile: TCP client connected" ));
    return 1;
  }

  /* === PREFACE: read 24-byte HTTP/2 client preface === */
  if( grpc->state==FD_STREAM_GRPC_STATE_PREFACE ) {
    uchar buf[24];
    long n = read( grpc->conn_fd, buf, 24UL - grpc->preface_sz );
    if( n<=0 ) {
      if( n==0 || (errno!=EAGAIN && errno!=EWOULDBLOCK) ) {
        grpc_close_conn( grpc );
      }
      return 0;
    }

    if( FD_UNLIKELY( !fd_memeq( buf, fd_h2_client_preface + grpc->preface_sz, (ulong)n ) ) ) {
      FD_LOG_WARNING(( "stream tile: not HTTP/2" ));
      grpc_close_conn( grpc );
      return 1;
    }

    grpc->preface_sz += (ulong)n;
    if( grpc->preface_sz < 24 ) return 1; /* need more */

    /* Preface complete. Initialize H2 server. */
    fd_h2_conn_init_server( grpc->h2_conn );
    grpc->h2_conn->self_settings.max_concurrent_streams = 1;

    fd_h2_rbuf_init( grpc->rbuf_rx, grpc->rx_buf, sizeof(grpc->rx_buf) );
    fd_h2_rbuf_init( grpc->rbuf_tx, grpc->tx_buf, sizeof(grpc->tx_buf) );
    memset( grpc->h2_stream, 0, sizeof(grpc->h2_stream) );
    memset( grpc->tx_op, 0, sizeof(grpc->tx_op) );

    grpc->state = FD_STREAM_GRPC_STATE_ACTIVE;

    /* Generate and send SETTINGS immediately */
    fd_h2_tx_control( grpc->h2_conn, grpc->rbuf_tx, grpc->h2_cb );
    grpc_flush_tx( grpc );

    FD_LOG_NOTICE(( "stream tile: H2 preface OK, SETTINGS sent" ));
    return 1;
  }

  /* === ACTIVE / STREAMING: drive H2 protocol === */
  if( grpc->state>=FD_STREAM_GRPC_STATE_ACTIVE ) {
    int busy = 0;

    /* Send control frames (SETTINGS ACK, PING ACK, etc.) */
    fd_h2_tx_control( grpc->h2_conn, grpc->rbuf_tx, grpc->h2_cb );

    /* Flush TX */
    grpc_flush_tx( grpc );

    /* Check dead */
    if( FD_UNLIKELY( grpc->h2_conn->flags & FD_H2_CONN_FLAGS_DEAD ) ) {
      grpc_close_conn( grpc );
      return 1;
    }

    /* Read from socket */
    if( grpc_recv( grpc ) ) {
      /* Process H2 frames */
      fd_h2_rx( grpc->h2_conn, grpc->rbuf_rx, grpc->rbuf_tx,
                grpc->scratch, sizeof(grpc->scratch), grpc->h2_cb );
      busy = 1;

      /* Flush any response frames generated by rx processing */
      grpc_flush_tx( grpc );
    }

    /* Continue any pending tx_op */
    if( grpc->h2_stream->stream_id && grpc->tx_op->chunk_sz ) {
      fd_h2_tx_op_copy( grpc->h2_conn, grpc->h2_stream, grpc->rbuf_tx, grpc->tx_op );
      grpc_flush_tx( grpc );
      busy = 1;
    }

    return busy;
  }

  return 0;
}

int
fd_stream_grpc_send( fd_stream_grpc_t * grpc,
                     uchar const *      pb_data,
                     ulong              pb_sz ) {
  if( FD_UNLIKELY( grpc->state != FD_STREAM_GRPC_STATE_STREAMING ) ) return 0;
  if( FD_UNLIKELY( !grpc->active_stream_id ) ) return 0;
  if( FD_UNLIKELY( pb_sz > FD_STREAM_GRPC_MAX_MSG_SZ ) ) return 0;
  if( FD_UNLIKELY( grpc->tx_op->chunk_sz ) ) return 0; /* previous send still in progress */

  g_grpc = grpc;

  /* Build gRPC frame */
  uchar * frame = grpc->grpc_frame;
  frame[0] = 0;
  frame[1] = (uchar)( pb_sz >> 24 );
  frame[2] = (uchar)( pb_sz >> 16 );
  frame[3] = (uchar)( pb_sz >> 8  );
  frame[4] = (uchar)( pb_sz       );
  fd_memcpy( frame + 5, pb_data, pb_sz );

  ulong frame_sz = 5UL + pb_sz;

  /* Send as H2 DATA frame (no END_STREAM) */
  fd_h2_tx_op_init( grpc->tx_op, (char const *)frame, frame_sz, 0 );
  fd_h2_tx_op_copy( grpc->h2_conn, grpc->h2_stream, grpc->rbuf_tx, grpc->tx_op );
  grpc_flush_tx( grpc );

  return 1;
}
