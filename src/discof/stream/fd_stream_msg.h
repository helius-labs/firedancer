#ifndef HEADER_fd_src_discof_stream_fd_stream_msg_h
#define HEADER_fd_src_discof_stream_fd_stream_msg_h

#include "../../disco/fd_txn_p.h"
#include "../../flamenco/log_collector/fd_log_collector_base.h"

/* Message type discriminator.  The first byte of every message on the
   execrp_strm link identifies the payload type.  The stream tile uses
   this to dispatch to the correct protobuf encoder. */

#define FD_STREAM_MSG_TYPE_TXN       (0)
#define FD_STREAM_MSG_TYPE_ACCT      (1)
#define FD_STREAM_MSG_TYPE_SLOT      (2)
#define FD_STREAM_MSG_TYPE_ENTRY     (3)
#define FD_STREAM_MSG_TYPE_BLOCK_META (4)

/* fd_stream_acct_msg_t is a variable-length message for a single
   account update.  Sent on the same execrp_strm link as txn messages.

   Variable region (immediately after the fixed header):
     1. data: data_sz bytes of raw account data */

struct fd_stream_acct_msg {
  uchar  msg_type;            /* Always FD_STREAM_MSG_TYPE_ACCT */
  uchar  executable;
  uchar  _pad[2];
  uint   data_sz;             /* Size of account data following header */
  ulong  slot;
  ulong  lamports;
  ulong  write_version;       /* Monotonic counter for ordering */
  uchar  pubkey[32];
  uchar  owner[32];
  uchar  txn_signature[64];   /* Signature of the transaction that modified this account */
  /* data_sz bytes of account data follow */
};
typedef struct fd_stream_acct_msg fd_stream_acct_msg_t;

static inline uchar *
fd_stream_acct_msg_data( fd_stream_acct_msg_t * msg ) {
  return (uchar *)msg + sizeof(fd_stream_acct_msg_t);
}

static inline ulong
fd_stream_acct_msg_sz( fd_stream_acct_msg_t const * msg ) {
  return sizeof(fd_stream_acct_msg_t) + (ulong)msg->data_sz;
}

/* Slot status values matching Yellowstone SlotStatus enum */

#define FD_STREAM_SLOT_PROCESSED (0)
#define FD_STREAM_SLOT_CONFIRMED (1)
#define FD_STREAM_SLOT_FINALIZED (2)

/* fd_stream_slot_msg_t is a fixed-size message for slot status updates.
   Published by the stream tile when it reads tower_out events. */

struct fd_stream_slot_msg {
  uchar  msg_type;            /* Always FD_STREAM_MSG_TYPE_SLOT */
  uchar  status;              /* FD_STREAM_SLOT_{PROCESSED,CONFIRMED,FINALIZED} */
  uchar  _pad[6];
  ulong  slot;
  ulong  parent_slot;
};
typedef struct fd_stream_slot_msg fd_stream_slot_msg_t;

/* fd_stream_entry_msg_t is a fixed-size message for entry (PoH tick)
   updates within a block. */

struct fd_stream_entry_msg {
  uchar  msg_type;            /* Always FD_STREAM_MSG_TYPE_ENTRY */
  uchar  _pad[7];
  ulong  slot;
  ulong  index;               /* Entry index within the block */
  ulong  num_hashes;          /* Number of PoH hashes in this entry */
  uchar  hash[32];            /* Entry hash */
  ulong  executed_transaction_count;
  ulong  starting_transaction_index;
};
typedef struct fd_stream_entry_msg fd_stream_entry_msg_t;

/* fd_stream_block_meta_msg_t is a fixed-size message emitted when a
   block finishes execution.  Contains summary metadata. */

struct fd_stream_block_meta_msg {
  uchar  msg_type;            /* Always FD_STREAM_MSG_TYPE_BLOCK_META */
  uchar  _pad[7];
  ulong  slot;
  ulong  parent_slot;
  ulong  block_time;          /* Unix timestamp (seconds) */
  ulong  block_height;
  ulong  executed_transaction_count;
  ulong  entries_count;
  uchar  blockhash[32];
  uchar  parent_blockhash[32];
};
typedef struct fd_stream_block_meta_msg fd_stream_block_meta_msg_t;

/* fd_stream_txn_msg_t is a variable-length message written by execrp
   tiles to a fd_circq for consumption by the stream tile.  It contains
   everything needed to construct a Yellowstone gRPC
   SubscribeUpdateTransaction message.

   The message is packed with variable-length regions following the
   fixed header.  The total size is the value passed to
   fd_circq_push_back.

   Variable regions (in order, immediately after the fixed header):
     1. pre_balances:  account_cnt × sizeof(ulong)
     2. post_balances: account_cnt × sizeof(ulong)
     3. is_writable:   account_cnt × sizeof(uchar)
     4. account_keys:  account_cnt × 32 bytes (fd_pubkey_t)
     5. pre_token_balances:  pre_token_balance_cnt  × sizeof(fd_stream_token_balance_t)
     6. post_token_balances: post_token_balance_cnt × sizeof(fd_stream_token_balance_t)
     7. log_data:      log_sz bytes
     8. inner_instructions_data: inner_instructions_data_sz bytes
        (packed as repeated [fd_stream_inner_instr_t header][acct_idxs][data])
     9. return_data:   return_data_sz bytes

   Access each region using the helper macros/functions below.
*/

/* fd_stream_inner_instr_t is a compact representation of one inner
   instruction.  Followed by acct_cnt bytes of account indices, then
   data_sz bytes of instruction data. */

struct fd_stream_inner_instr {
  uchar  top_level_idx;   /* Index of the top-level instruction that spawned this CPI */
  uchar  program_id_idx;  /* Index of the program account in the transaction's account list */
  ushort acct_cnt;        /* Number of account indices following this header */
  ushort data_sz;         /* Size of instruction data following the account indices */
  uchar  stack_height;    /* Instruction stack depth (2 = direct CPI, 3+ = nested) */
  uchar  _pad;
};
typedef struct fd_stream_inner_instr fd_stream_inner_instr_t;

/* fd_stream_token_balance_t is a compact token balance entry. */

struct fd_stream_token_balance {
  uchar  account_idx;     /* Index in the transaction's account list */
  uchar  decimals;        /* Token decimals (from mint account, 0 if unknown) */
  uchar  _pad[6];
  uchar  mint[32];        /* Token mint address */
  uchar  owner[32];       /* Token account owner */
  ulong  amount;          /* Token amount in base units */
};
typedef struct fd_stream_token_balance fd_stream_token_balance_t;

struct fd_stream_txn_msg {

  uchar      msg_type;          /* Always FD_STREAM_MSG_TYPE_TXN */

  /* ---- Raw transaction ---- */

  fd_txn_p_t txn;               /* Raw transaction payload + parsed metadata */
  ulong      slot;
  ulong      txn_idx;           /* Transaction index within the block */

  /* ---- Execution result ---- */

  int        is_committable;
  int        is_fees_only;
  int        txn_err;
  int        exec_err;
  int        exec_err_kind;
  int        exec_err_idx;
  uint       custom_err;
  int        is_simple_vote;

  /* ---- Fees and compute ---- */

  ulong      fee;
  ulong      compute_units_consumed;

  /* ---- Counts for variable-length regions ---- */

  ushort     account_cnt;
  ushort     pre_token_balance_cnt;
  ushort     post_token_balance_cnt;
  ushort     inner_instruction_cnt;
  uint       inner_instructions_data_sz;
  ushort     log_sz;
  ushort     return_data_sz;
  uchar      return_data_program_id[32];

  /* Variable-length data follows. Access with fd_stream_txn_msg_*() */
};
typedef struct fd_stream_txn_msg fd_stream_txn_msg_t;

/* Accessors for variable-length regions. Each region immediately
   follows the previous one in memory. */

static inline ulong *
fd_stream_txn_msg_pre_balances( fd_stream_txn_msg_t * msg ) {
  return (ulong *)( (uchar *)msg + sizeof(fd_stream_txn_msg_t) );
}

static inline ulong *
fd_stream_txn_msg_post_balances( fd_stream_txn_msg_t * msg ) {
  return fd_stream_txn_msg_pre_balances( msg ) + msg->account_cnt;
}

static inline uchar *
fd_stream_txn_msg_is_writable( fd_stream_txn_msg_t * msg ) {
  return (uchar *)( fd_stream_txn_msg_post_balances( msg ) + msg->account_cnt );
}

static inline uchar *
fd_stream_txn_msg_account_keys( fd_stream_txn_msg_t * msg ) {
  return fd_stream_txn_msg_is_writable( msg ) + msg->account_cnt;
}

static inline fd_stream_token_balance_t *
fd_stream_txn_msg_pre_token_balances( fd_stream_txn_msg_t * msg ) {
  return (fd_stream_token_balance_t *)( fd_stream_txn_msg_account_keys( msg ) + msg->account_cnt * 32UL );
}

static inline fd_stream_token_balance_t *
fd_stream_txn_msg_post_token_balances( fd_stream_txn_msg_t * msg ) {
  return fd_stream_txn_msg_pre_token_balances( msg ) + msg->pre_token_balance_cnt;
}

static inline uchar *
fd_stream_txn_msg_log_data( fd_stream_txn_msg_t * msg ) {
  return (uchar *)( fd_stream_txn_msg_post_token_balances( msg ) + msg->post_token_balance_cnt );
}

static inline uchar *
fd_stream_txn_msg_inner_instructions_data( fd_stream_txn_msg_t * msg ) {
  return fd_stream_txn_msg_log_data( msg ) + msg->log_sz;
}

static inline uchar *
fd_stream_txn_msg_return_data( fd_stream_txn_msg_t * msg ) {
  return fd_stream_txn_msg_inner_instructions_data( msg ) + msg->inner_instructions_data_sz;
}

/* fd_stream_txn_msg_sz returns the total size of the message including
   all variable-length regions. */

static inline ulong
fd_stream_txn_msg_sz( fd_stream_txn_msg_t const * msg ) {
  return sizeof(fd_stream_txn_msg_t)
       + (ulong)msg->account_cnt * ( sizeof(ulong) + sizeof(ulong) + 1UL + 32UL )  /* pre + post + writable + keys */
       + (ulong)msg->pre_token_balance_cnt  * sizeof(fd_stream_token_balance_t)
       + (ulong)msg->post_token_balance_cnt * sizeof(fd_stream_token_balance_t)
       + (ulong)msg->log_sz
       + (ulong)msg->inner_instructions_data_sz
       + (ulong)msg->return_data_sz;
}

#endif /* HEADER_fd_src_discof_stream_fd_stream_msg_h */
