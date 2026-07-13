#ifndef HEADER_fd_src_discof_stream_fd_stream_grpc_h
#define HEADER_fd_src_discof_stream_fd_stream_grpc_h

#include "../../waltz/h2/fd_h2.h"
#include "../../util/fd_util.h"

#define FD_STREAM_GRPC_MAX_MSG_SZ (65536UL)

#define FD_STREAM_GRPC_STATE_LISTENING (0)
#define FD_STREAM_GRPC_STATE_PREFACE  (1)
#define FD_STREAM_GRPC_STATE_ACTIVE   (2)
#define FD_STREAM_GRPC_STATE_STREAMING (3)

struct fd_stream_grpc {
  int              listen_fd;
  int              conn_fd;
  int              state;

  /* H2 state */
  fd_h2_conn_t     h2_conn[1];
  fd_h2_stream_t   h2_stream[1];
  fd_h2_rbuf_t     rbuf_tx[1];
  fd_h2_rbuf_t     rbuf_rx[1];
  fd_h2_callbacks_t h2_cb[1];
  fd_h2_tx_op_t    tx_op[1];

  uchar            tx_buf[ 1UL<<16UL ];
  uchar            rx_buf[ 1UL<<14UL ];
  uchar            scratch[ 1UL<<14UL ];
  uchar            grpc_frame[ 5UL + FD_STREAM_GRPC_MAX_MSG_SZ ];

  /* Preface tracking */
  ulong            preface_sz;

  uint             active_stream_id;
};

typedef struct fd_stream_grpc fd_stream_grpc_t;

FD_PROTOTYPES_BEGIN

int fd_stream_grpc_init( fd_stream_grpc_t * grpc, uint listen_addr, ushort listen_port );
int fd_stream_grpc_poll( fd_stream_grpc_t * grpc );
int fd_stream_grpc_send( fd_stream_grpc_t * grpc, uchar const * pb_data, ulong pb_sz );

static inline int
fd_stream_grpc_is_streaming( fd_stream_grpc_t const * grpc ) {
  return grpc->state == FD_STREAM_GRPC_STATE_STREAMING;
}

FD_PROTOTYPES_END

#endif
