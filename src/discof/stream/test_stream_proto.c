/* test_stream_proto: Encodes synthetic transactions as Yellowstone
   SubscribeUpdateTransaction protobufs and writes them to files.
   A Rust decoder verifies correctness.

   Build:
     (see Makefile or build command in test runner)

   Outputs one file per test case to /tmp/test_*.pb
*/

#include "fd_stream_msg.h"
#include "fd_stream_proto.h"
#include "fd_stream_token.h"
#include "../../flamenco/log_collector/fd_log_collector_base.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

static void
set_base_txn( fd_stream_txn_msg_t * msg, uchar * buf ) {
  memset( msg, 0, sizeof(*msg) );

  uchar * payload = msg->txn.payload;
  fd_txn_t * txn  = (fd_txn_t *)msg->txn._;

  /* One signature of 0xAA */
  memset( payload, 0xAA, 64 );

  txn->transaction_version    = FD_TXN_VLEGACY;
  txn->signature_cnt          = 1;
  txn->signature_off          = 0;
  txn->acct_addr_cnt          = 3;
  txn->acct_addr_off          = 65;
  txn->readonly_signed_cnt    = 0;
  txn->readonly_unsigned_cnt  = 1;
  txn->recent_blockhash_off   = 65 + 3*32;
  txn->instr_cnt              = 1;
  txn->addr_table_lookup_cnt  = 0;

  memset( payload + 65,  0x11, 32 );
  memset( payload + 97,  0x22, 32 );
  memset( payload + 129, 0x33, 32 );
  memset( payload + 161, 0xBB, 32 );

  txn->instr[0].program_id = 2;
  txn->instr[0].acct_off   = 193;
  txn->instr[0].acct_cnt   = 2;
  txn->instr[0].data_off   = 195;
  txn->instr[0].data_sz    = 4;
  payload[193] = 0; payload[194] = 1;
  payload[195] = 0xDE; payload[196] = 0xAD; payload[197] = 0xBE; payload[198] = 0xEF;

  msg->txn.payload_sz = 199;
  msg->slot               = 42424242UL;
  msg->txn_idx            = 7;
  msg->is_committable     = 1;
  msg->is_simple_vote     = 0;
  msg->fee                = 5000;
  msg->compute_units_consumed = 200000;
  msg->account_cnt = 3;

  (void)buf;
}

static int
write_pb( const char * path, uchar * pb_buf, ulong pb_sz ) {
  int fd = open( path, O_WRONLY | O_CREAT | O_TRUNC, 0644 );
  if( fd<0 ) { perror( "open" ); return 0; }
  ulong written = 0;
  while( written<pb_sz ) {
    long n = write( fd, pb_buf+written, pb_sz-written );
    if( n<=0 ) { perror( "write" ); close(fd); return 0; }
    written += (ulong)n;
  }
  close( fd );
  return 1;
}

/* Test 1: Simple successful transaction (baseline) */
static int
test_success( void ) {
  uchar msg_buf[ 65536 ];
  fd_stream_txn_msg_t * msg = (fd_stream_txn_msg_t *)msg_buf;
  set_base_txn( msg, msg_buf );

  uchar * cursor = msg_buf + sizeof(fd_stream_txn_msg_t);
  ulong pre[] = { 1000000000UL, 500000000UL, 0UL };
  ulong post[] = { 999995000UL, 500005000UL, 0UL };
  memcpy( cursor, pre, sizeof(pre) ); cursor += sizeof(pre);
  memcpy( cursor, post, sizeof(post) ); cursor += sizeof(post);
  uchar wr[] = { 1, 1, 0 };
  memcpy( cursor, wr, 3 ); cursor += 3;
  memset( cursor, 0x11, 32 ); cursor += 32;
  memset( cursor, 0x22, 32 ); cursor += 32;
  memset( cursor, 0x33, 32 ); cursor += 32;

  uchar pb_buf[ 65536 + 32 ];
  ulong pb_sz = 0;
  if( !fd_stream_encode_txn_update( pb_buf, sizeof(pb_buf), msg, &pb_sz ) ) return 0;
  return write_pb( "/tmp/test_1_success.pb", pb_buf, pb_sz );
}

/* Test 2: Failed transaction with InstructionError(0, Custom(42)) */
static int
test_instruction_error( void ) {
  uchar msg_buf[ 65536 ];
  fd_stream_txn_msg_t * msg = (fd_stream_txn_msg_t *)msg_buf;
  set_base_txn( msg, msg_buf );

  msg->txn_err       = -9; /* FD_RUNTIME_TXN_ERR_INSTRUCTION_ERROR */
  msg->exec_err      = -26; /* FD_EXECUTOR_INSTR_ERR_CUSTOM_ERR */
  msg->exec_err_idx  = 0;
  msg->custom_err    = 42;
  msg->is_committable = 1; /* fees-only transactions are still committable */

  uchar * cursor = msg_buf + sizeof(fd_stream_txn_msg_t);
  ulong pre[] = { 1000000000UL, 500000000UL, 0UL };
  ulong post[] = { 999995000UL, 500000000UL, 0UL };
  memcpy( cursor, pre, sizeof(pre) ); cursor += sizeof(pre);
  memcpy( cursor, post, sizeof(post) ); cursor += sizeof(post);
  uchar wr[] = { 1, 1, 0 };
  memcpy( cursor, wr, 3 ); cursor += 3;
  memset( cursor, 0x11, 32 ); cursor += 32;
  memset( cursor, 0x22, 32 ); cursor += 32;
  memset( cursor, 0x33, 32 ); cursor += 32;

  uchar pb_buf[ 65536 + 32 ];
  ulong pb_sz = 0;
  if( !fd_stream_encode_txn_update( pb_buf, sizeof(pb_buf), msg, &pb_sz ) ) return 0;
  return write_pb( "/tmp/test_2_instr_error.pb", pb_buf, pb_sz );
}

/* Test 3: Transaction with inner instructions */
static int
test_inner_instructions( void ) {
  uchar msg_buf[ 65536 ];
  fd_stream_txn_msg_t * msg = (fd_stream_txn_msg_t *)msg_buf;
  set_base_txn( msg, msg_buf );

  uchar * cursor = msg_buf + sizeof(fd_stream_txn_msg_t);
  /* Pre/post balances */
  ulong pre[] = { 1000000000UL, 500000000UL, 0UL };
  ulong post[] = { 999995000UL, 500005000UL, 0UL };
  memcpy( cursor, pre, sizeof(pre) ); cursor += sizeof(pre);
  memcpy( cursor, post, sizeof(post) ); cursor += sizeof(post);
  uchar wr[] = { 1, 1, 0 };
  memcpy( cursor, wr, 3 ); cursor += 3;
  memset( cursor, 0x11, 32 ); cursor += 32;
  memset( cursor, 0x22, 32 ); cursor += 32;
  memset( cursor, 0x33, 32 ); cursor += 32;

  /* No token balances, no logs */

  /* Inner instructions: 2 inner instructions under top-level instruction 0 */
  uchar * inner_start = cursor;

  /* Inner instruction 1: program_id=2, accounts=[0,1], data=[0x01,0x02], stack_height=2 */
  fd_stream_inner_instr_t * hdr1 = (fd_stream_inner_instr_t *)cursor;
  hdr1->top_level_idx  = 0;
  hdr1->program_id_idx = 2;
  hdr1->acct_cnt       = 2;
  hdr1->data_sz        = 2;
  hdr1->stack_height   = 2;
  hdr1->_pad           = 0;
  cursor += sizeof(fd_stream_inner_instr_t);
  cursor[0] = 0; cursor[1] = 1; cursor += 2; /* accounts */
  cursor[0] = 0x01; cursor[1] = 0x02; cursor += 2; /* data */

  /* Inner instruction 2: program_id=2, accounts=[1], data=[0x03], stack_height=3 */
  fd_stream_inner_instr_t * hdr2 = (fd_stream_inner_instr_t *)cursor;
  hdr2->top_level_idx  = 0;
  hdr2->program_id_idx = 2;
  hdr2->acct_cnt       = 1;
  hdr2->data_sz        = 1;
  hdr2->stack_height   = 3;
  hdr2->_pad           = 0;
  cursor += sizeof(fd_stream_inner_instr_t);
  cursor[0] = 1; cursor += 1; /* accounts */
  cursor[0] = 0x03; cursor += 1; /* data */

  msg->inner_instruction_cnt = 2;
  msg->inner_instructions_data_sz = (uint)(cursor - inner_start);

  uchar pb_buf[ 65536 + 32 ];
  ulong pb_sz = 0;
  if( !fd_stream_encode_txn_update( pb_buf, sizeof(pb_buf), msg, &pb_sz ) ) return 0;
  return write_pb( "/tmp/test_3_inner_instrs.pb", pb_buf, pb_sz );
}

/* Test 4: Transaction with log messages */
static int
test_log_messages( void ) {
  uchar msg_buf[ 65536 ];
  fd_stream_txn_msg_t * msg = (fd_stream_txn_msg_t *)msg_buf;
  set_base_txn( msg, msg_buf );

  uchar * cursor = msg_buf + sizeof(fd_stream_txn_msg_t);
  /* Pre/post balances */
  ulong pre[] = { 1000000000UL, 500000000UL, 0UL };
  ulong post[] = { 999995000UL, 500005000UL, 0UL };
  memcpy( cursor, pre, sizeof(pre) ); cursor += sizeof(pre);
  memcpy( cursor, post, sizeof(post) ); cursor += sizeof(post);
  uchar wr[] = { 1, 1, 0 };
  memcpy( cursor, wr, 3 ); cursor += 3;
  memset( cursor, 0x11, 32 ); cursor += 32;
  memset( cursor, 0x22, 32 ); cursor += 32;
  memset( cursor, 0x33, 32 ); cursor += 32;

  /* Synthesize log messages in the log collector protobuf format.
     Each entry: [tag=0x32] [varint length] [string bytes]
     Tag 0x32 = field 6, wire type LEN = TransactionStatusMeta.log_messages */
  uchar * log_start = cursor;
  const char * log1 = "Program 111 invoke [1]";
  const char * log2 = "Program 111 success";
  ulong l1 = strlen(log1);
  ulong l2 = strlen(log2);

  /* Log entry 1 */
  *cursor++ = 0x32;            /* tag */
  *cursor++ = (uchar)l1;      /* length (fits in 1 byte) */
  memcpy( cursor, log1, l1 ); cursor += l1;

  /* Log entry 2 */
  *cursor++ = 0x32;
  *cursor++ = (uchar)l2;
  memcpy( cursor, log2, l2 ); cursor += l2;

  msg->log_sz = (ushort)(cursor - log_start);

  uchar pb_buf[ 65536 + 32 ];
  ulong pb_sz = 0;
  if( !fd_stream_encode_txn_update( pb_buf, sizeof(pb_buf), msg, &pb_sz ) ) return 0;
  return write_pb( "/tmp/test_4_logs.pb", pb_buf, pb_sz );
}

int
main( int     argc,
      char ** argv ) {
  (void)argc; (void)argv;

  int pass = 0;
  int fail = 0;

#define RUN_TEST(name, fn) do { \
    fprintf( stderr, "%-30s ... ", name ); \
    if( fn() ) { fprintf( stderr, "ENCODED\n" ); pass++; } \
    else       { fprintf( stderr, "FAIL\n" ); fail++; } \
  } while(0)

  RUN_TEST( "test_success",             test_success );
  RUN_TEST( "test_instruction_error",   test_instruction_error );
  RUN_TEST( "test_inner_instructions",  test_inner_instructions );
  RUN_TEST( "test_log_messages",        test_log_messages );

  fprintf( stderr, "\n%d encoded, %d failed\n", pass, fail );
  return fail ? 1 : 0;
}
