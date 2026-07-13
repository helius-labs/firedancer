/* THIS FILE IS A PLACEHOLDER.  Proper seccomp policy generation
   requires running generate_filters.py on the .seccomppolicy file.
   For now, the stream tile returns 0 instructions (no seccomp filter)
   which means the tile runs without seccomp restriction.  This MUST
   be replaced with a properly generated filter before production use. */

#ifndef HEADER_fd_src_discof_stream_generated_fd_stream_tile_seccomp_h
#define HEADER_fd_src_discof_stream_generated_fd_stream_tile_seccomp_h

#if defined(__linux__)
#include <linux/filter.h>

static const unsigned int sock_filter_policy_fd_stream_tile_instr_cnt = 0;

static void
populate_sock_filter_policy_fd_stream_tile( ulong out_cnt  FD_PARAM_UNUSED,
                                            struct sock_filter * out FD_PARAM_UNUSED,
                                            unsigned int logfile_fd  FD_PARAM_UNUSED ) {
  /* No filter — all syscalls allowed (prototype only) */
}

#endif /* defined(__linux__) */
#endif /* HEADER_fd_src_discof_stream_generated_fd_stream_tile_seccomp_h */
