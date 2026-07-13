#ifndef HEADER_fd_src_discof_stream_fd_stream_proto_h
#define HEADER_fd_src_discof_stream_fd_stream_proto_h

#include "fd_stream_msg.h"
#include "../../ballet/pb/fd_pb_encode.h"
#include "../../ballet/txn/fd_txn.h"
#include "../replay/fd_replay_tile.h"

/* fd_stream_proto.h provides protobuf encoders for Yellowstone gRPC
   SubscribeUpdateTransaction messages.  The encoding matches the
   schema in geyser.proto and solana-storage.proto.

   All encode functions return the encoder on success or NULL on failure
   (out of buffer space). */

FD_PROTOTYPES_BEGIN

/* fd_stream_encode_txn_update encodes a full SubscribeUpdate message
   containing one SubscribeUpdateTransaction.  This is the top-level
   function called by the stream tile.

   buf/buf_sz: output buffer (should be at least 64KB + 32 for safety)
   msg: the fd_stream_txn_msg_t from the execrp tile
   out_sz: on success, set to the number of bytes written

   Returns 1 on success, 0 on failure (message too large for buffer). */

int
fd_stream_encode_txn_update( uchar *                       buf,
                             ulong                         buf_sz,
                             fd_stream_txn_msg_t const *   msg,
                             ulong *                       out_sz );

/* fd_stream_encode_acct_update encodes a full SubscribeUpdate message
   containing one SubscribeUpdateAccount.

   buf/buf_sz: output buffer
   msg: the fd_stream_acct_msg_t from the execrp tile
   out_sz: on success, set to the number of bytes written

   Returns 1 on success, 0 on failure. */

int
fd_stream_encode_acct_update( uchar *                        buf,
                              ulong                          buf_sz,
                              fd_stream_acct_msg_t const *   msg,
                              ulong *                        out_sz );

/* fd_stream_encode_tower_msg encodes a SubscribeUpdate from a tower_out
   message.  The sig field from the stem determines the message type
   (slot_confirmed, slot_done, slot_rooted). */

int
fd_stream_encode_tower_msg( uchar *       buf,
                            ulong         buf_sz,
                            uchar const * tower_msg,
                            ulong         sig,
                            ulong *       out_sz );

/* fd_stream_encode_replay_msg encodes a SubscribeUpdate from a
   replay_out message (slot_completed → block_meta, etc). */

int
fd_stream_encode_replay_msg( uchar *       buf,
                             ulong         buf_sz,
                             uchar const * replay_msg,
                             ulong         sig,
                             ulong *       out_sz );

/* fd_stream_encode_slot_update encodes a SubscribeUpdateSlot with
   explicit slot, parent_slot, and status. */

/* fd_stream_encode_replay_msg_with_rewards encodes a block_meta with
   accumulated reward entries. */

int
fd_stream_encode_replay_msg_with_rewards( uchar *                       buf,
                                          ulong                         buf_sz,
                                          uchar const *                 replay_msg,
                                          fd_stream_reward_t const *    rewards,
                                          ulong                         rewards_cnt,
                                          ulong *                       out_sz );

int
fd_stream_encode_slot_update( uchar * buf,
                              ulong   buf_sz,
                              ulong   slot,
                              ulong   parent_slot,
                              uint    status,
                              ulong * out_sz );

FD_PROTOTYPES_END

#endif /* HEADER_fd_src_discof_stream_fd_stream_proto_h */
