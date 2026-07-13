#include "fd_stream_proto.h"
#include "../../ballet/base58/fd_base58.h"
#include "../tower/fd_tower_tile.h"
#include "../replay/fd_replay_tile.h"
#include <stdio.h> /* snprintf for amount string */
#include <string.h> /* memcpy */

/* Forward declaration */
static int encode_created_at_and_finish( fd_pb_encoder_t * enc, ulong * out_sz );

/* Return 10^exp as double for exp in [0, 18].  Values beyond that would
   overflow uint64 anyway. */
static inline double
pow10_u32( uchar decimals ) {
  static double const table[] = {
    1.0, 10.0, 100.0, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
    1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18
  };
  if( decimals>18 ) return 1e18;
  return table[decimals];
}

/* Format an amount+decimals pair as Agave-compatible ui_amount_string.
   Matches spl-token-2022 real_number_string_trimmed: the decimal point
   and everything after it is omitted entirely when the fractional part
   is zero (e.g. amount=10000000000000000 decimals=9 -> "10000000",
   NOT "10000000.0"). Otherwise trailing zeros in the fractional part
   are trimmed but at least one digit remains after the decimal point.
   Special case: amount==0 formats as "0". */
static int
format_ui_amount( char * out, ulong out_sz, ulong amount, uchar decimals ) {
  if( amount==0UL ) {
    if( out_sz>=2 ) { out[0]='0'; out[1]=0; return 1; }
    return 0;
  }
  if( decimals==0 ) {
    return snprintf( out, out_sz, "%lu", amount );
  }
  /* Print amount with enough leading zeros so that we can slice the
     decimal point in. */
  char raw[32];
  int  raw_len = snprintf( raw, sizeof(raw), "%0*lu", (int)(decimals+1), amount );
  int  int_len = raw_len - (int)decimals;
  int  frac_end = raw_len;
  while( frac_end > int_len && raw[frac_end-1]=='0' ) frac_end--;
  int  frac_len = frac_end - int_len;
  if( frac_len==0 ) {
    /* Fractional part is entirely zeros: omit the decimal point. */
    if( (ulong)(int_len + 1) > out_sz ) return 0;
    memcpy( out, raw, (ulong)int_len );
    out[int_len] = 0;
    return int_len;
  }
  if( (ulong)(int_len + 1 + frac_len + 1) > out_sz ) return 0;
  memcpy( out, raw, (ulong)int_len );
  out[int_len] = '.';
  memcpy( out + int_len + 1, raw + int_len, (ulong)frac_len );
  out[int_len + 1 + frac_len] = 0;
  return int_len + 1 + frac_len;
}

/* Yellowstone gRPC protobuf field IDs.  These must match geyser.proto
   and solana-storage.proto exactly. */

/* SubscribeUpdate */
#define PB_SUBSCRIBE_UPDATE_FILTERS    1U
#define PB_SUBSCRIBE_UPDATE_ACCT       2U  /* oneof: account */
#define PB_SUBSCRIBE_UPDATE_TXN        4U  /* oneof: transaction */
#define PB_SUBSCRIBE_UPDATE_CREATED_AT 11U

/* SubscribeUpdateAccount */
#define PB_SUB_UPD_ACCT_INFO       1U
#define PB_SUB_UPD_ACCT_SLOT       2U
#define PB_SUB_UPD_ACCT_IS_STARTUP 3U

/* SubscribeUpdateAccountInfo */
#define PB_ACCT_INFO_PUBKEY        1U
#define PB_ACCT_INFO_LAMPORTS      2U
#define PB_ACCT_INFO_OWNER         3U
#define PB_ACCT_INFO_EXECUTABLE    4U
#define PB_ACCT_INFO_RENT_EPOCH    5U
#define PB_ACCT_INFO_DATA          6U
#define PB_ACCT_INFO_WRITE_VERSION 7U
#define PB_ACCT_INFO_TXN_SIGNATURE 8U

/* SubscribeUpdateTransaction */
#define PB_SUB_UPD_TXN_INFO 1U
#define PB_SUB_UPD_TXN_SLOT 2U

/* SubscribeUpdateTransactionInfo */
#define PB_TXN_INFO_SIGNATURE 1U
#define PB_TXN_INFO_IS_VOTE   2U
#define PB_TXN_INFO_TXN       3U
#define PB_TXN_INFO_META      4U
#define PB_TXN_INFO_INDEX     5U

/* solana.storage.ConfirmedBlock.Transaction */
#define PB_TXN_SIGNATURES 1U
#define PB_TXN_MESSAGE    2U

/* solana.storage.ConfirmedBlock.Message */
#define PB_MSG_HEADER                  1U
#define PB_MSG_ACCOUNT_KEYS            2U
#define PB_MSG_RECENT_BLOCKHASH        3U
#define PB_MSG_INSTRUCTIONS            4U
#define PB_MSG_VERSIONED               5U
#define PB_MSG_ADDRESS_TABLE_LOOKUPS   6U

/* MessageHeader */
#define PB_HDR_NUM_REQUIRED_SIGNATURES        1U
#define PB_HDR_NUM_READONLY_SIGNED_ACCOUNTS   2U
#define PB_HDR_NUM_READONLY_UNSIGNED_ACCOUNTS 3U

/* CompiledInstruction */
#define PB_INSTR_PROGRAM_ID_INDEX 1U
#define PB_INSTR_ACCOUNTS         2U
#define PB_INSTR_DATA             3U

/* MessageAddressTableLookup */
#define PB_ALT_ACCOUNT_KEY       1U
#define PB_ALT_WRITABLE_INDEXES  2U
#define PB_ALT_READONLY_INDEXES  3U

/* TransactionStatusMeta */
#define PB_META_ERR                        1U
#define PB_META_FEE                        2U
#define PB_META_PRE_BALANCES               3U
#define PB_META_POST_BALANCES              4U
#define PB_META_INNER_INSTRUCTIONS         5U
#define PB_META_LOG_MESSAGES               6U
#define PB_META_PRE_TOKEN_BALANCES         7U
#define PB_META_POST_TOKEN_BALANCES        8U
#define PB_META_REWARDS                    9U
#define PB_META_INNER_INSTRUCTIONS_NONE   10U
#define PB_META_LOG_MESSAGES_NONE         11U
#define PB_META_LOADED_WRITABLE_ADDRESSES 12U
#define PB_META_LOADED_READONLY_ADDRESSES 13U
#define PB_META_RETURN_DATA               14U
#define PB_META_RETURN_DATA_NONE          15U
#define PB_META_COMPUTE_UNITS_CONSUMED    16U

/* TransactionError */
#define PB_TXN_ERR_ERR 1U

/* InnerInstructions */
#define PB_INNER_INSTRS_INDEX        1U
#define PB_INNER_INSTRS_INSTRUCTIONS 2U

/* InnerInstruction */
#define PB_INNER_INSTR_PROGRAM_ID_INDEX 1U
#define PB_INNER_INSTR_ACCOUNTS         2U
#define PB_INNER_INSTR_DATA             3U
#define PB_INNER_INSTR_STACK_HEIGHT     4U

/* ReturnData */
#define PB_RETURN_DATA_PROGRAM_ID 1U
#define PB_RETURN_DATA_DATA       2U

/* google.protobuf.Timestamp */
#define PB_TIMESTAMP_SECONDS 1U
#define PB_TIMESTAMP_NANOS   2U

/* Helper: encode the solana-storage Transaction message from raw
   fd_txn_p_t data. */

static fd_pb_encoder_t *
encode_transaction( fd_pb_encoder_t * enc,
                    fd_txn_p_t const * txnp ) {
  fd_txn_t const * txn     = (fd_txn_t const *)txnp->_;
  uchar const *    payload = txnp->payload;

  /* Transaction.signatures (repeated bytes) */
  for( uchar i=0; i<txn->signature_cnt; i++ ) {
    fd_pb_push_bytes( enc, PB_TXN_SIGNATURES, payload + txn->signature_off + (ulong)i*64UL, 64UL );
  }

  /* Transaction.message (submessage) */
  if( FD_UNLIKELY( !fd_pb_submsg_open( enc, PB_TXN_MESSAGE ) ) ) return NULL;
  {
    /* MessageHeader */
    if( FD_UNLIKELY( !fd_pb_submsg_open( enc, PB_MSG_HEADER ) ) ) return NULL;
    fd_pb_push_uint32( enc, PB_HDR_NUM_REQUIRED_SIGNATURES,        txn->signature_cnt );
    fd_pb_push_uint32( enc, PB_HDR_NUM_READONLY_SIGNED_ACCOUNTS,   txn->readonly_signed_cnt );
    fd_pb_push_uint32( enc, PB_HDR_NUM_READONLY_UNSIGNED_ACCOUNTS, txn->readonly_unsigned_cnt );
    fd_pb_submsg_close( enc );

    /* account_keys (repeated bytes, from the static account table) */
    ushort acct_addr_cnt = txn->acct_addr_cnt;
    for( ushort i=0; i<acct_addr_cnt; i++ ) {
      fd_pb_push_bytes( enc, PB_MSG_ACCOUNT_KEYS, payload + txn->acct_addr_off + (ulong)i*32UL, 32UL );
    }

    /* recent_blockhash */
    fd_pb_push_bytes( enc, PB_MSG_RECENT_BLOCKHASH, payload + txn->recent_blockhash_off, 32UL );

    /* instructions (repeated CompiledInstruction) */
    for( ushort i=0; i<txn->instr_cnt; i++ ) {
      if( FD_UNLIKELY( !fd_pb_submsg_open( enc, PB_MSG_INSTRUCTIONS ) ) ) return NULL;
      fd_txn_instr_t const * instr = &txn->instr[i];
      fd_pb_push_uint32( enc, PB_INSTR_PROGRAM_ID_INDEX, instr->program_id );
      fd_pb_push_bytes( enc, PB_INSTR_ACCOUNTS, payload + instr->acct_off, instr->acct_cnt );
      fd_pb_push_bytes( enc, PB_INSTR_DATA, payload + instr->data_off, instr->data_sz );
      fd_pb_submsg_close( enc );
    }

    /* versioned flag */
    if( txn->transaction_version==FD_TXN_V0 ) {
      fd_pb_push_bool( enc, PB_MSG_VERSIONED, 1 );
    }

    /* address_table_lookups (repeated, for v0 transactions) */
    if( txn->transaction_version==FD_TXN_V0 ) {
      fd_txn_acct_addr_lut_t const * atls = fd_txn_get_address_tables_const( txn );
      for( uchar i=0; i<txn->addr_table_lookup_cnt; i++ ) {
        fd_txn_acct_addr_lut_t const * atl = &atls[i];
        if( FD_UNLIKELY( !fd_pb_submsg_open( enc, PB_MSG_ADDRESS_TABLE_LOOKUPS ) ) ) return NULL;
        fd_pb_push_bytes( enc, PB_ALT_ACCOUNT_KEY,      payload + atl->addr_off, 32UL );
        fd_pb_push_bytes( enc, PB_ALT_WRITABLE_INDEXES, payload + atl->writable_off, atl->writable_cnt );
        fd_pb_push_bytes( enc, PB_ALT_READONLY_INDEXES, payload + atl->readonly_off, atl->readonly_cnt );
        fd_pb_submsg_close( enc );
      }
    }
  }
  fd_pb_submsg_close( enc );

  return enc;
}

/* Helper: encode TransactionStatusMeta from stream message data. */

static fd_pb_encoder_t *
encode_meta( fd_pb_encoder_t *           enc,
             fd_stream_txn_msg_t const * msg ) {

  /* err — only present if txn_err != 0.
     The protobuf field is bytes containing a bincode-serialized
     TransactionError enum.  Bincode enum format:
       [4 bytes LE: variant index]
       [variant data if any]

     Firedancer error codes are negated: agave_variant = -(fd_err) - 1.
     InstructionError (variant 8) carries:
       [1 byte: instruction index] [4 bytes LE: InstructionError variant]
     Custom InstructionError adds:
       [4 bytes LE: custom error code] */
  if( msg->txn_err ) {
    /* Map Firedancer txn_err to Agave variant index */
    int fd_err = msg->txn_err;
    /* Handle special cases that map to BLOCKHASH_NOT_FOUND */
    if( fd_err <= -50 ) fd_err = -8; /* FD_RUNTIME_TXN_ERR_BLOCKHASH_* → BlockhashNotFound */

    uint agave_variant = (uint)( -fd_err - 1 );

    if( FD_UNLIKELY( !fd_pb_submsg_open( enc, PB_META_ERR ) ) ) return NULL;

    if( agave_variant == 8U /* InstructionError */ ) {
      /* InstructionError(u8, InstructionError) */
      uint agave_instr_variant = (uint)( -msg->exec_err - 1 );

      if( agave_instr_variant == 25U /* Custom */ ) {
        /* Custom(u32) — 4 + 1 + 4 + 4 = 13 bytes */
        uchar err_bytes[13];
        FD_STORE( uint, err_bytes+0, agave_variant );
        err_bytes[4] = (uchar)msg->exec_err_idx;
        FD_STORE( uint, err_bytes+5, agave_instr_variant );
        FD_STORE( uint, err_bytes+9, msg->custom_err );
        fd_pb_push_bytes( enc, PB_TXN_ERR_ERR, err_bytes, 13UL );
      } else {
        /* Simple InstructionError — 4 + 1 + 4 = 9 bytes */
        uchar err_bytes[9];
        FD_STORE( uint, err_bytes+0, agave_variant );
        err_bytes[4] = (uchar)msg->exec_err_idx;
        FD_STORE( uint, err_bytes+5, agave_instr_variant );
        fd_pb_push_bytes( enc, PB_TXN_ERR_ERR, err_bytes, 9UL );
      }
    } else {
      /* Simple variant with no data — 4 bytes */
      uchar err_bytes[4];
      FD_STORE( uint, err_bytes, agave_variant );
      fd_pb_push_bytes( enc, PB_TXN_ERR_ERR, err_bytes, 4UL );
    }

    fd_pb_submsg_close( enc );
  }

  /* fee */
  fd_pb_push_uint64( enc, PB_META_FEE, msg->fee );

  /* pre_balances (repeated uint64) */
  ulong const * pre_bal  = (ulong const *)( (uchar const *)msg + sizeof(fd_stream_txn_msg_t) );
  for( ushort i=0; i<msg->account_cnt; i++ ) {
    fd_pb_push_uint64( enc, PB_META_PRE_BALANCES, pre_bal[i] );
  }

  /* post_balances (repeated uint64) */
  ulong const * post_bal = pre_bal + msg->account_cnt;
  for( ushort i=0; i<msg->account_cnt; i++ ) {
    fd_pb_push_uint64( enc, PB_META_POST_BALANCES, post_bal[i] );
  }

  /* For transactions that did not actually execute instructions —
     either they weren't committed at all (is_committable == 0) or they
     were committed as fees-only (is_fees_only == 1, fee charged but no
     execution) — Agave emits inner_instructions_none=true and
     log_messages_none=true.  Committed txns with instruction errors
     (is_committable == 1, is_fees_only == 0) DID execute and have logs. */
  int txn_err_only = !msg->is_committable || msg->is_fees_only;

  /* inner_instructions (repeated InnerInstructions, grouped by
     top-level instruction index) */
  if( !txn_err_only && msg->inner_instruction_cnt>0 ) {
    /* Walk the packed inner instructions and group by top_level_idx */
    uchar const * inner_data = (uchar const *)msg
                             + sizeof(fd_stream_txn_msg_t)
                             + (ulong)msg->account_cnt * (sizeof(ulong) + sizeof(ulong) + 1UL + 32UL)
                             + (ulong)msg->pre_token_balance_cnt  * sizeof(fd_stream_token_balance_t)
                             + (ulong)msg->post_token_balance_cnt * sizeof(fd_stream_token_balance_t)
                             + (ulong)msg->log_sz;

    uchar const * cursor    = inner_data;
    int           cur_group = -1;

    for( ushort i=0; i<msg->inner_instruction_cnt; i++ ) {
      fd_stream_inner_instr_t const * hdr = (fd_stream_inner_instr_t const *)cursor;
      cursor += sizeof(fd_stream_inner_instr_t);
      uchar const * acct_idxs = cursor;
      cursor += hdr->acct_cnt;
      uchar const * data = cursor;
      cursor += hdr->data_sz;

      if( (int)hdr->top_level_idx != cur_group ) {
        /* Close previous group if open */
        if( cur_group>=0 ) fd_pb_submsg_close( enc );
        /* Open new InnerInstructions group */
        if( FD_UNLIKELY( !fd_pb_submsg_open( enc, PB_META_INNER_INSTRUCTIONS ) ) ) return NULL;
        fd_pb_push_uint32( enc, PB_INNER_INSTRS_INDEX, (uint)hdr->top_level_idx );
        cur_group = (int)hdr->top_level_idx;
      }

      /* Encode InnerInstruction */
      if( FD_UNLIKELY( !fd_pb_submsg_open( enc, PB_INNER_INSTRS_INSTRUCTIONS ) ) ) return NULL;
      fd_pb_push_uint32( enc, PB_INNER_INSTR_PROGRAM_ID_INDEX, (uint)hdr->program_id_idx );
      fd_pb_push_bytes( enc, PB_INNER_INSTR_ACCOUNTS, acct_idxs, hdr->acct_cnt );
      fd_pb_push_bytes( enc, PB_INNER_INSTR_DATA, data, hdr->data_sz );
      fd_pb_push_uint32( enc, PB_INNER_INSTR_STACK_HEIGHT, (uint)hdr->stack_height );
      fd_pb_submsg_close( enc );
    }

    /* Close last group */
    if( cur_group>=0 ) fd_pb_submsg_close( enc );
  }
  /* Yellowstone semantics: for executed txns (even failed with exec_err),
     inner_instructions is Some(vec); an empty vec is omitted by proto3.
     For txn_err failures where no execution occurred, Agave sets
     inner_instructions to None, which serializes as
     inner_instructions_none=true (field 10). */
  if( FD_UNLIKELY( txn_err_only ) ) {
    fd_pb_push_bool( enc, PB_META_INNER_INSTRUCTIONS_NONE, 1 );
  }

  /* log_messages (repeated string, field 6).
     The log collector buffer is already in protobuf wire format: each
     entry is [tag=0x32][varint length][string bytes].  Tag 0x32 =
     (field 6 << 3) | wire_type 2 (LEN), which is exactly the tag for
     repeated string field 6 of TransactionStatusMeta.  So we can
     append the raw bytes directly. */
  if( FD_LIKELY( msg->log_sz > 0 ) ) {
    uchar const * log_region = (uchar const *)msg
                              + sizeof(fd_stream_txn_msg_t)
                              + (ulong)msg->account_cnt * (sizeof(ulong) + sizeof(ulong) + 1UL + 32UL)
                              + (ulong)msg->pre_token_balance_cnt  * sizeof(fd_stream_token_balance_t)
                              + (ulong)msg->post_token_balance_cnt * sizeof(fd_stream_token_balance_t);
    /* Directly copy the pre-encoded protobuf log bytes into the
       encoder output.  This avoids re-serializing each log string. */
    if( FD_LIKELY( fd_pb_encoder_space( enc ) >= msg->log_sz ) ) {
      fd_memcpy( enc->cur, log_region, msg->log_sz );
      enc->cur += msg->log_sz;
    }
    /* log_messages_none defaults to false in proto3 — omit field 11. */
  } else {
    fd_pb_push_bool( enc, PB_META_LOG_MESSAGES_NONE, 1 );
  }

  /* pre_token_balances (repeated TokenBalance, field 7) */
  if( msg->pre_token_balance_cnt > 0 ) {
    fd_stream_token_balance_t const * pre_tok_region =
      (fd_stream_token_balance_t const *)( (uchar const *)msg
        + sizeof(fd_stream_txn_msg_t)
        + (ulong)msg->account_cnt * (sizeof(ulong) + sizeof(ulong) + 1UL + 32UL) );

    for( ushort i=0; i<msg->pre_token_balance_cnt; i++ ) {
      fd_stream_token_balance_t const * tb = &pre_tok_region[i];
      if( FD_UNLIKELY( !fd_pb_submsg_open( enc, 7U /* pre_token_balances */ ) ) ) return NULL;

      fd_pb_push_uint32( enc, 1U, (uint)tb->account_idx );

      char mint_b58[ FD_BASE58_ENCODED_32_SZ ];
      fd_base58_encode_32( tb->mint, NULL, mint_b58 );
      fd_pb_push_string( enc, 2U, mint_b58, strlen( mint_b58 ) );

      if( FD_UNLIKELY( !fd_pb_submsg_open( enc, 3U ) ) ) return NULL;
      /* UiTokenAmount.ui_amount (field 1, double) — omit if amount==0
         (Agave uses Option<f64>; None serializes as absent). */
      if( tb->amount>0UL ) {
        double ui_amount = (double)tb->amount / pow10_u32( tb->decimals );
        fd_pb_push_double( enc, 1U, ui_amount );
      }
      fd_pb_push_uint32( enc, 2U, (uint)tb->decimals );
      char amount_str[21];
      int amount_len = snprintf( amount_str, sizeof(amount_str), "%lu", tb->amount );
      fd_pb_push_string( enc, 3U, amount_str, (ulong)amount_len );
      char ui_amount_str[32];
      int ui_amount_len = format_ui_amount( ui_amount_str, sizeof(ui_amount_str), tb->amount, tb->decimals );
      fd_pb_push_string( enc, 4U, ui_amount_str, (ulong)ui_amount_len );
      fd_pb_submsg_close( enc );

      char owner_b58[ FD_BASE58_ENCODED_32_SZ ];
      fd_base58_encode_32( tb->owner, NULL, owner_b58 );
      fd_pb_push_string( enc, 4U, owner_b58, strlen( owner_b58 ) );
      {
        char program_id_b58[ FD_BASE58_ENCODED_32_SZ ];
        fd_base58_encode_32( tb->program_id, NULL, program_id_b58 );
        fd_pb_push_string( enc, 5U, program_id_b58, strlen( program_id_b58 ) );
      }

      fd_pb_submsg_close( enc );
    }
  }

  /* post_token_balances (repeated TokenBalance, field 8) */
  if( msg->post_token_balance_cnt > 0 ) {
    fd_stream_token_balance_t const * tok_region =
      (fd_stream_token_balance_t const *)( (uchar const *)msg
        + sizeof(fd_stream_txn_msg_t)
        + (ulong)msg->account_cnt * (sizeof(ulong) + sizeof(ulong) + 1UL + 32UL)
        + (ulong)msg->pre_token_balance_cnt * sizeof(fd_stream_token_balance_t) );

    for( ushort i=0; i<msg->post_token_balance_cnt; i++ ) {
      fd_stream_token_balance_t const * tb = &tok_region[i];
      if( FD_UNLIKELY( !fd_pb_submsg_open( enc, 8U /* post_token_balances */ ) ) ) return NULL;

      /* TokenBalance.account_index (field 1) */
      fd_pb_push_uint32( enc, 1U, (uint)tb->account_idx );

      /* TokenBalance.mint (field 2, string — base58 encoded) */
      char mint_b58[ FD_BASE58_ENCODED_32_SZ ];
      fd_base58_encode_32( tb->mint, NULL, mint_b58 );
      fd_pb_push_string( enc, 2U, mint_b58, strlen( mint_b58 ) );

      /* TokenBalance.ui_token_amount (field 3, submessage) */
      if( FD_UNLIKELY( !fd_pb_submsg_open( enc, 3U ) ) ) return NULL;
      {
        /* UiTokenAmount.ui_amount (field 1, double) — Agave uses
           Option<f64>; None serializes as absent (omit if amount==0). */
        if( tb->amount>0UL ) {
          double ui_amount = (double)tb->amount / pow10_u32( tb->decimals );
          fd_pb_push_double( enc, 1U, ui_amount );
        }
        /* UiTokenAmount.decimals (field 2) */
        fd_pb_push_uint32( enc, 2U, (uint)tb->decimals );
        /* UiTokenAmount.amount (field 3, string — decimal representation) */
        char amount_str[21];
        int amount_len = snprintf( amount_str, sizeof(amount_str), "%lu", tb->amount );
        fd_pb_push_string( enc, 3U, amount_str, (ulong)amount_len );
        /* UiTokenAmount.ui_amount_string (field 4, string) */
        char ui_amount_str[32];
        int ui_amount_len = format_ui_amount( ui_amount_str, sizeof(ui_amount_str), tb->amount, tb->decimals );
        fd_pb_push_string( enc, 4U, ui_amount_str, (ulong)ui_amount_len );
      }
      fd_pb_submsg_close( enc );

      /* TokenBalance.owner (field 4, string — base58 encoded) */
      char owner_b58[ FD_BASE58_ENCODED_32_SZ ];
      fd_base58_encode_32( tb->owner, NULL, owner_b58 );
      fd_pb_push_string( enc, 4U, owner_b58, strlen( owner_b58 ) );

      /* TokenBalance.program_id (field 5, string — always SPL Token for now) */
      {
        char program_id_b58[ FD_BASE58_ENCODED_32_SZ ];
        fd_base58_encode_32( tb->program_id, NULL, program_id_b58 );
        fd_pb_push_string( enc, 5U, program_id_b58, strlen( program_id_b58 ) );
      }

      fd_pb_submsg_close( enc );
    }
  }

  /* loaded_writable_addresses, loaded_readonly_addresses */
  /* For v0 transactions, the loaded addresses are the accounts resolved
     from address lookup tables.  These are in the account_keys region
     beyond the static account table.  For legacy transactions, these
     are empty. */
  fd_txn_t const * txn = (fd_txn_t const *)msg->txn._;
  if( txn->transaction_version==FD_TXN_V0 && msg->account_cnt > txn->acct_addr_cnt ) {
    uchar const * acct_keys_region = (uchar const *)msg
                                   + sizeof(fd_stream_txn_msg_t)
                                   + (ulong)msg->account_cnt * (sizeof(ulong) + sizeof(ulong) + 1UL);
    uchar const * is_writable = (uchar const *)msg
                               + sizeof(fd_stream_txn_msg_t)
                               + (ulong)msg->account_cnt * (sizeof(ulong) + sizeof(ulong));

    for( ushort i=txn->acct_addr_cnt; i<msg->account_cnt; i++ ) {
      uchar const * key = acct_keys_region + (ulong)i * 32UL;
      if( is_writable[i] ) {
        fd_pb_push_bytes( enc, PB_META_LOADED_WRITABLE_ADDRESSES, key, 32UL );
      } else {
        fd_pb_push_bytes( enc, PB_META_LOADED_READONLY_ADDRESSES, key, 32UL );
      }
    }
  }

  /* return_data */
  if( msg->return_data_sz > 0 ) {
    if( FD_UNLIKELY( !fd_pb_submsg_open( enc, PB_META_RETURN_DATA ) ) ) return NULL;
    fd_pb_push_bytes( enc, PB_RETURN_DATA_PROGRAM_ID, msg->return_data_program_id, 32UL );
    /* Return data is in the variable region */
    uchar const * ret_data = (uchar const *)msg
                           + sizeof(fd_stream_txn_msg_t)
                           + (ulong)msg->account_cnt * (sizeof(ulong) + sizeof(ulong) + 1UL + 32UL)
                           + (ulong)msg->pre_token_balance_cnt  * sizeof(fd_stream_token_balance_t)
                           + (ulong)msg->post_token_balance_cnt * sizeof(fd_stream_token_balance_t)
                           + (ulong)msg->log_sz
                           + (ulong)msg->inner_instructions_data_sz;
    fd_pb_push_bytes( enc, PB_RETURN_DATA_DATA, ret_data, msg->return_data_sz );
    fd_pb_submsg_close( enc );
  }
  fd_pb_push_bool( enc, PB_META_RETURN_DATA_NONE, (msg->return_data_sz==0) );

  /* compute_units_consumed (optional uint64) */
  fd_pb_push_uint64( enc, PB_META_COMPUTE_UNITS_CONSUMED, msg->compute_units_consumed );

  /* cost_units (field 17, optional uint64) */
  if( FD_LIKELY( msg->cost_units > 0UL ) ) {
    fd_pb_push_uint64( enc, 17U /* PB_META_COST_UNITS */, msg->cost_units );
  }

  return enc;
}

int
fd_stream_encode_txn_update( uchar *                       buf,
                             ulong                         buf_sz,
                             fd_stream_txn_msg_t const *   msg,
                             ulong *                       out_sz ) {

  fd_pb_encoder_t enc[1];
  fd_pb_encoder_init( enc, buf, buf_sz );

  /* SubscribeUpdate { filters = ["all"], transaction (field 4) = SubscribeUpdateTransaction { ... } } */

  /* SubscribeUpdate.filters (field 1, repeated string).  Echo back the
     subscription filter name(s) so the client knows which subscription
     matched.  FD currently only supports a single hardcoded "all" filter. */
  fd_pb_push_bytes( enc, PB_SUBSCRIBE_UPDATE_FILTERS, (uchar const *)"all", 3UL );

  /* Open SubscribeUpdate.transaction (oneof field 4) */
  if( FD_UNLIKELY( !fd_pb_submsg_open( enc, PB_SUBSCRIBE_UPDATE_TXN ) ) ) return 0;
  {
    /* SubscribeUpdateTransaction.transaction (field 1) = SubscribeUpdateTransactionInfo */
    if( FD_UNLIKELY( !fd_pb_submsg_open( enc, PB_SUB_UPD_TXN_INFO ) ) ) return 0;
    {
      /* signature */
      fd_txn_t const * txn = (fd_txn_t const *)msg->txn._;
      uchar const * sig = msg->txn.payload + txn->signature_off;
      fd_pb_push_bytes( enc, PB_TXN_INFO_SIGNATURE, sig, 64UL );

      /* is_vote */
      fd_pb_push_bool( enc, PB_TXN_INFO_IS_VOTE, msg->is_simple_vote );

      /* transaction (submessage) */
      if( FD_UNLIKELY( !fd_pb_submsg_open( enc, PB_TXN_INFO_TXN ) ) ) return 0;
      if( FD_UNLIKELY( !encode_transaction( enc, &msg->txn ) ) ) return 0;
      fd_pb_submsg_close( enc );

      /* meta (submessage) */
      if( FD_UNLIKELY( !fd_pb_submsg_open( enc, PB_TXN_INFO_META ) ) ) return 0;
      if( FD_UNLIKELY( !encode_meta( enc, msg ) ) ) return 0;
      fd_pb_submsg_close( enc );

      /* index */
      fd_pb_push_uint64( enc, PB_TXN_INFO_INDEX, msg->txn_idx );
    }
    fd_pb_submsg_close( enc ); /* SubscribeUpdateTransactionInfo */

    /* SubscribeUpdateTransaction.slot (field 2) */
    fd_pb_push_uint64( enc, PB_SUB_UPD_TXN_SLOT, msg->slot );
  }
  fd_pb_submsg_close( enc ); /* SubscribeUpdateTransaction */

  /* created_at timestamp (field 11) */
  {
    long now_ns = fd_log_wallclock();
    long secs   = now_ns / (long)1e9;
    int  nanos  = (int)( now_ns - secs * (long)1e9 );
    if( FD_UNLIKELY( !fd_pb_submsg_open( enc, PB_SUBSCRIBE_UPDATE_CREATED_AT ) ) ) return 0;
    fd_pb_push_int64( enc, PB_TIMESTAMP_SECONDS, secs );
    fd_pb_push_int32( enc, PB_TIMESTAMP_NANOS, nanos );
    fd_pb_submsg_close( enc );
  }

  *out_sz = fd_pb_encoder_out_sz( enc );
  return 1;
}

/* Encode a SubscribeUpdate containing one SubscribeUpdateAccount. */

int
fd_stream_encode_acct_update( uchar *                        buf,
                              ulong                          buf_sz,
                              fd_stream_acct_msg_t const *   msg,
                              ulong *                        out_sz ) {

  fd_pb_encoder_t _enc[1];
  fd_pb_encoder_t * enc = fd_pb_encoder_init( _enc, buf, buf_sz );

  /* SubscribeUpdate.account (field 2, submsg) */
  if( FD_UNLIKELY( !fd_pb_submsg_open( enc, PB_SUBSCRIBE_UPDATE_ACCT ) ) ) return 0;
  {
    /* SubscribeUpdateAccount.account (field 1, submsg) */
    if( FD_UNLIKELY( !fd_pb_submsg_open( enc, PB_SUB_UPD_ACCT_INFO ) ) ) return 0;
    {
      /* SubscribeUpdateAccountInfo fields */
      fd_pb_push_bytes( enc, PB_ACCT_INFO_PUBKEY,    msg->pubkey, 32UL );
      fd_pb_push_uint64( enc, PB_ACCT_INFO_LAMPORTS, msg->lamports );
      fd_pb_push_bytes( enc, PB_ACCT_INFO_OWNER,     msg->owner, 32UL );
      if( msg->executable ) fd_pb_push_bool( enc, PB_ACCT_INFO_EXECUTABLE, 1 );
      fd_pb_push_uint64( enc, PB_ACCT_INFO_RENT_EPOCH, ULONG_MAX ); /* All accounts are rent-exempt */
      if( msg->data_sz ) {
        uchar const * data = (uchar const *)msg + sizeof(fd_stream_acct_msg_t);
        fd_pb_push_bytes( enc, PB_ACCT_INFO_DATA, data, msg->data_sz );
      }
      fd_pb_push_uint64( enc, PB_ACCT_INFO_WRITE_VERSION, msg->write_version );
      fd_pb_push_bytes( enc, PB_ACCT_INFO_TXN_SIGNATURE, msg->txn_signature, 64UL );
    }
    fd_pb_submsg_close( enc ); /* SubscribeUpdateAccountInfo */

    /* SubscribeUpdateAccount.slot (field 2) */
    fd_pb_push_uint64( enc, PB_SUB_UPD_ACCT_SLOT, msg->slot );

    /* SubscribeUpdateAccount.is_startup (field 3) — always false */
  }
  fd_pb_submsg_close( enc ); /* SubscribeUpdateAccount */

  return encode_created_at_and_finish( enc, out_sz );
}

/* SubscribeUpdateSlot field numbers */
#define PB_SUBSCRIBE_UPDATE_SLOT       3U
#define PB_UPD_SLOT_SLOT               1U
#define PB_UPD_SLOT_PARENT             2U
#define PB_UPD_SLOT_STATUS             3U

/* SubscribeUpdateBlockMeta field numbers */
#define PB_SUBSCRIBE_UPDATE_BLOCK_META 7U
#define PB_BLOCK_META_SLOT             1U
#define PB_BLOCK_META_BLOCKHASH        2U
#define PB_BLOCK_META_REWARDS          3U
#define PB_BLOCK_META_BLOCK_TIME       4U
#define PB_BLOCK_META_BLOCK_HEIGHT     5U
#define PB_BLOCK_META_PARENT_SLOT      6U
#define PB_BLOCK_META_PARENT_BLOCKHASH 7U
#define PB_BLOCK_META_EXEC_TXN_COUNT   8U
#define PB_BLOCK_META_ENTRIES_COUNT    9U

static int
encode_created_at_and_finish( fd_pb_encoder_t * enc, ulong * out_sz ) {
  long now_ns = fd_log_wallclock();
  long secs   = now_ns / (long)1e9;
  int  nanos  = (int)( now_ns - secs * (long)1e9 );
  if( FD_UNLIKELY( !fd_pb_submsg_open( enc, PB_SUBSCRIBE_UPDATE_CREATED_AT ) ) ) return 0;
  fd_pb_push_int64( enc, PB_TIMESTAMP_SECONDS, secs );
  fd_pb_push_int32( enc, PB_TIMESTAMP_NANOS, nanos );
  fd_pb_submsg_close( enc );
  *out_sz = fd_pb_encoder_out_sz( enc );
  return 1;
}

int
fd_stream_encode_slot_update( uchar * buf,
                              ulong   buf_sz,
                              ulong   slot,
                              ulong   parent_slot,
                              uint    status,
                              ulong * out_sz ) {
  fd_pb_encoder_t _enc[1];
  fd_pb_encoder_t * enc = fd_pb_encoder_init( _enc, buf, buf_sz );

  if( FD_UNLIKELY( !fd_pb_submsg_open( enc, PB_SUBSCRIBE_UPDATE_SLOT ) ) ) return 0;
  fd_pb_push_uint64( enc, PB_UPD_SLOT_SLOT, slot );
  fd_pb_push_uint64( enc, PB_UPD_SLOT_PARENT, parent_slot );
  fd_pb_push_uint32( enc, PB_UPD_SLOT_STATUS, status );
  fd_pb_submsg_close( enc );

  return encode_created_at_and_finish( enc, out_sz );
}

int
fd_stream_encode_tower_msg( uchar *       buf,
                            ulong         buf_sz,
                            uchar const * tower_msg,
                            ulong         sig,
                            ulong *       out_sz ) {
  uint slot_status;
  ulong slot;

  if( sig==FD_TOWER_SIG_SLOT_CONFIRMED ) {
    fd_tower_slot_confirmed_t const * msg = (fd_tower_slot_confirmed_t const *)tower_msg;
    /* Emit CONFIRMED for duplicate confirmation (52% stake) or higher.
       This matches Agave's is_slot_duplicate_confirmed threshold.
       Duplicate confirmation fires earlier than optimistic (67%) and
       better matches Yellowstone's confirmation timing. */
    if( msg->level < FD_TOWER_SLOT_CONFIRMED_DUPLICATE ) return 0;
    slot_status = 1;
    slot = msg->slot;
  } else if( sig==FD_TOWER_SIG_SLOT_DONE ) {
    fd_tower_slot_done_t const * msg = (fd_tower_slot_done_t const *)tower_msg;
    slot_status = 0;
    slot = msg->replay_slot;
  } else if( sig==FD_TOWER_SIG_SLOT_ROOTED ) {
    fd_tower_slot_rooted_t const * msg = (fd_tower_slot_rooted_t const *)tower_msg;
    slot_status = 2;
    slot = msg->slot;
  } else {
    return 0;
  }

  fd_pb_encoder_t _enc[1];
  fd_pb_encoder_t * enc = fd_pb_encoder_init( _enc, buf, buf_sz );

  if( FD_UNLIKELY( !fd_pb_submsg_open( enc, PB_SUBSCRIBE_UPDATE_SLOT ) ) ) return 0;
  fd_pb_push_uint64( enc, PB_UPD_SLOT_SLOT, slot );
  fd_pb_push_uint32( enc, PB_UPD_SLOT_STATUS, slot_status );
  fd_pb_submsg_close( enc );

  return encode_created_at_and_finish( enc, out_sz );
}

/* SubscribeUpdateEntry field numbers */
#define PB_SUBSCRIBE_UPDATE_ENTRY      8U
#define PB_ENTRY_SLOT                  1U
#define PB_ENTRY_INDEX                 2U
#define PB_ENTRY_NUM_HASHES            3U
#define PB_ENTRY_HASH                  4U
#define PB_ENTRY_EXEC_TXN_COUNT        5U
#define PB_ENTRY_STARTING_TXN_INDEX    6U

int
fd_stream_encode_replay_msg( uchar *       buf,
                             ulong         buf_sz,
                             uchar const * replay_msg,
                             ulong         sig,
                             ulong *       out_sz ) {

  if( sig==REPLAY_SIG_SLOT_CONFIRMED ) {
    fd_replay_slot_confirmed_t const * conf = (fd_replay_slot_confirmed_t const *)replay_msg;
    return fd_stream_encode_slot_update( buf, buf_sz, conf->slot, conf->parent_slot, 1 /* CONFIRMED */, out_sz );
  }

  if( sig==REPLAY_SIG_SLOT_DEAD ) {
    /* Dead slots are not typically sent by Yellowstone, but we include
       them for completeness.  The dead_error field (field 4) is optional. */
    return 0; /* Skip dead slots — Yellowstone doesn't send them */
  }

  if( sig==REPLAY_SIG_ENTRY ) {
    fd_replay_entry_t const * entry = (fd_replay_entry_t const *)replay_msg;

    fd_pb_encoder_t _enc[1];
    fd_pb_encoder_t * enc = fd_pb_encoder_init( _enc, buf, buf_sz );

    if( FD_UNLIKELY( !fd_pb_submsg_open( enc, PB_SUBSCRIBE_UPDATE_ENTRY ) ) ) return 0;
    fd_pb_push_uint64( enc, PB_ENTRY_SLOT, entry->slot );
    fd_pb_push_uint64( enc, PB_ENTRY_INDEX, entry->index );
    fd_pb_push_uint64( enc, PB_ENTRY_NUM_HASHES, entry->num_hashes );
    fd_pb_push_bytes( enc, PB_ENTRY_HASH, entry->hash, 32UL );
    fd_pb_push_uint64( enc, PB_ENTRY_EXEC_TXN_COUNT, entry->executed_transaction_count );
    if( entry->starting_transaction_index )
      fd_pb_push_uint64( enc, PB_ENTRY_STARTING_TXN_INDEX, entry->starting_transaction_index );
    fd_pb_submsg_close( enc );

    return encode_created_at_and_finish( enc, out_sz );
  }

  if( sig!=REPLAY_SIG_SLOT_COMPLETED ) return 0;

  fd_replay_slot_completed_t const * msg = (fd_replay_slot_completed_t const *)replay_msg;

  fd_pb_encoder_t _enc[1];
  fd_pb_encoder_t * enc = fd_pb_encoder_init( _enc, buf, buf_sz );

  if( FD_UNLIKELY( !fd_pb_submsg_open( enc, PB_SUBSCRIBE_UPDATE_BLOCK_META ) ) ) return 0;
  fd_pb_push_uint64( enc, PB_BLOCK_META_SLOT, msg->slot );
  {
    char b58[ FD_BASE58_ENCODED_32_SZ ];
    fd_base58_encode_32( msg->block_hash.uc, NULL, b58 );
    fd_pb_push_string( enc, PB_BLOCK_META_BLOCKHASH, b58, strlen( b58 ) );
  }
  /* Rewards: individual reward entries are not yet captured from the
     runtime.  We emit num_partitions when in the reward distribution
     period (non-zero for ~100 blocks after epoch start).
     TODO: capture per-validator Reward entries during epoch transition. */
  {
    if( FD_UNLIKELY( !fd_pb_submsg_open( enc, PB_BLOCK_META_REWARDS ) ) ) return 0;
    /* No individual Reward entries (field 1) — would need runtime hooks */
    if( msg->num_partitions ) {
      /* NumPartitions submsg (field 2) */
      if( FD_UNLIKELY( !fd_pb_submsg_open( enc, 2U /* num_partitions */ ) ) ) return 0;
      fd_pb_push_uint64( enc, 1U /* num_partitions field */, msg->num_partitions );
      fd_pb_submsg_close( enc );
    }
    fd_pb_submsg_close( enc );
  }
  {
    if( msg->block_time ) {
      if( FD_UNLIKELY( !fd_pb_submsg_open( enc, PB_BLOCK_META_BLOCK_TIME ) ) ) return 0;
      fd_pb_push_int64( enc, 1U, msg->block_time );
      fd_pb_submsg_close( enc );
    }
  }
  if( msg->block_height ) {
    if( FD_UNLIKELY( !fd_pb_submsg_open( enc, PB_BLOCK_META_BLOCK_HEIGHT ) ) ) return 0;
    fd_pb_push_uint64( enc, 1U, msg->block_height );
    fd_pb_submsg_close( enc );
  }
  fd_pb_push_uint64( enc, PB_BLOCK_META_PARENT_SLOT, msg->parent_slot );
  {
    char b58[ FD_BASE58_ENCODED_32_SZ ];
    fd_base58_encode_32( msg->parent_block_id.uc, NULL, b58 );
    fd_pb_push_string( enc, PB_BLOCK_META_PARENT_BLOCKHASH, b58, strlen( b58 ) );
  }
  fd_pb_push_uint64( enc, PB_BLOCK_META_EXEC_TXN_COUNT, msg->transaction_count );
  fd_pb_push_uint64( enc, PB_BLOCK_META_ENTRIES_COUNT, msg->entries_count );
  fd_pb_submsg_close( enc );

  return encode_created_at_and_finish( enc, out_sz );
}

/* Reward protobuf field numbers */
#define PB_REWARD_PUBKEY       1U
#define PB_REWARD_LAMPORTS     2U
#define PB_REWARD_POST_BALANCE 3U
#define PB_REWARD_REWARD_TYPE  4U
#define PB_REWARD_COMMISSION   5U

static int
encode_rewards( fd_pb_encoder_t *          enc,
                fd_stream_reward_t const * rewards,
                ulong                      rewards_cnt,
                ulong                      num_partitions ) {
  if( FD_UNLIKELY( !fd_pb_submsg_open( enc, PB_BLOCK_META_REWARDS ) ) ) return 0;
  for( ulong i=0; i<rewards_cnt; i++ ) {
    if( FD_UNLIKELY( !fd_pb_submsg_open( enc, 1U /* repeated Reward */ ) ) ) return 0;
    char b58[ FD_BASE58_ENCODED_32_SZ ];
    fd_base58_encode_32( rewards[i].pubkey, NULL, b58 );
    fd_pb_push_string( enc, PB_REWARD_PUBKEY, b58, strlen( b58 ) );
    fd_pb_push_int64( enc, PB_REWARD_LAMPORTS, rewards[i].lamports );
    fd_pb_push_uint64( enc, PB_REWARD_POST_BALANCE, rewards[i].post_balance );
    fd_pb_push_uint32( enc, PB_REWARD_REWARD_TYPE, rewards[i].reward_type );
    if( rewards[i].commission ) {
      char comm_str[4];
      int  comm_len = snprintf( comm_str, sizeof(comm_str), "%u", rewards[i].commission );
      fd_pb_push_string( enc, PB_REWARD_COMMISSION, comm_str, (ulong)comm_len );
    }
    fd_pb_submsg_close( enc );
  }
  if( num_partitions ) {
    if( FD_UNLIKELY( !fd_pb_submsg_open( enc, 2U /* NumPartitions */ ) ) ) return 0;
    fd_pb_push_uint64( enc, 1U, num_partitions );
    fd_pb_submsg_close( enc );
  }
  fd_pb_submsg_close( enc );
  return 1;
}

int
fd_stream_encode_replay_msg_with_rewards( uchar *                       buf,
                                          ulong                         buf_sz,
                                          uchar const *                 replay_msg,
                                          fd_stream_reward_t const *    rewards,
                                          ulong                         rewards_cnt,
                                          ulong *                       out_sz ) {
  fd_replay_slot_completed_t const * msg = (fd_replay_slot_completed_t const *)replay_msg;

  fd_pb_encoder_t _enc[1];
  fd_pb_encoder_t * enc = fd_pb_encoder_init( _enc, buf, buf_sz );

  if( FD_UNLIKELY( !fd_pb_submsg_open( enc, PB_SUBSCRIBE_UPDATE_BLOCK_META ) ) ) return 0;
  fd_pb_push_uint64( enc, PB_BLOCK_META_SLOT, msg->slot );
  {
    char b58[ FD_BASE58_ENCODED_32_SZ ];
    fd_base58_encode_32( msg->block_hash.uc, NULL, b58 );
    fd_pb_push_string( enc, PB_BLOCK_META_BLOCKHASH, b58, strlen( b58 ) );
  }
  if( !encode_rewards( enc, rewards, rewards_cnt, msg->num_partitions ) ) return 0;
  {
    if( msg->block_time ) {
      if( FD_UNLIKELY( !fd_pb_submsg_open( enc, PB_BLOCK_META_BLOCK_TIME ) ) ) return 0;
      fd_pb_push_int64( enc, 1U, msg->block_time );
      fd_pb_submsg_close( enc );
    }
  }
  if( msg->block_height ) {
    if( FD_UNLIKELY( !fd_pb_submsg_open( enc, PB_BLOCK_META_BLOCK_HEIGHT ) ) ) return 0;
    fd_pb_push_uint64( enc, 1U, msg->block_height );
    fd_pb_submsg_close( enc );
  }
  fd_pb_push_uint64( enc, PB_BLOCK_META_PARENT_SLOT, msg->parent_slot );
  {
    char b58[ FD_BASE58_ENCODED_32_SZ ];
    fd_base58_encode_32( msg->parent_block_id.uc, NULL, b58 );
    fd_pb_push_string( enc, PB_BLOCK_META_PARENT_BLOCKHASH, b58, strlen( b58 ) );
  }
  fd_pb_push_uint64( enc, PB_BLOCK_META_EXEC_TXN_COUNT, msg->transaction_count );
  fd_pb_push_uint64( enc, PB_BLOCK_META_ENTRIES_COUNT, msg->entries_count );
  fd_pb_submsg_close( enc );

  return encode_created_at_and_finish( enc, out_sz );
}
