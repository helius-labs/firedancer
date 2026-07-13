#ifndef HEADER_fd_src_discof_stream_fd_stream_token_h
#define HEADER_fd_src_discof_stream_fd_stream_token_h

#include "../../flamenco/fd_flamenco_base.h"
#include "../../flamenco/runtime/fd_system_ids.h"

/* fd_stream_token.h provides a minimal parser for SPL Token and
   Token-2022 account data, extracting just the fields needed for
   Yellowstone gRPC TokenBalance messages.

   SPL Token Account layout (165 bytes, fixed):
     [0..32)    mint       (Pubkey)
     [32..64)   owner      (Pubkey)
     [64..72)   amount     (u64 LE)
     [72..76)   delegate   (COption<Pubkey> tag)
     [76..108)  delegate   (Pubkey, if present)
     [108]      state      (AccountState enum: 0=Uninitialized, 1=Initialized, 2=Frozen)
     [109..113) is_native  (COption<u64> tag)
     [113..121) is_native  (u64, if present)
     [121..129) delegated_amount (u64)
     [129..133) close_authority  (COption<Pubkey> tag)
     [133..165) close_authority  (Pubkey, if present)

   Token-2022 has the same base layout with optional extensions after
   byte 165.  We only parse the base fields.

   SPL Token Mint layout (82 bytes):
     [0..4)     mint_authority   (COption<Pubkey> tag)
     [4..36)    mint_authority   (Pubkey, if present)
     [36..44)   supply           (u64 LE)
     [44]       decimals         (u8)
     [45]       is_initialized   (bool)
     ...
*/

#define FD_SPL_TOKEN_ACCOUNT_SZ  (165UL)
#define FD_SPL_TOKEN_MINT_SZ     (82UL)

/* Account state enum values */
#define FD_SPL_TOKEN_STATE_UNINITIALIZED (0)
#define FD_SPL_TOKEN_STATE_INITIALIZED   (1)
#define FD_SPL_TOKEN_STATE_FROZEN        (2)

struct fd_stream_token_info {
  fd_pubkey_t mint;
  fd_pubkey_t owner;
  ulong       amount;
};
typedef struct fd_stream_token_info fd_stream_token_info_t;

/* Token-2022 program ID: TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb
   Not currently in fd_system_ids.h, so we define it here. */

static const fd_pubkey_t fd_stream_spl_token_2022_id = { .uc = {
  0x06, 0xdd, 0xf6, 0xe1, 0xee, 0x75, 0x8f, 0xde,
  0x18, 0x42, 0x5d, 0xbc, 0xe4, 0x6c, 0xcd, 0xda,
  0xb6, 0x1a, 0xfc, 0x4d, 0x83, 0xb9, 0x0d, 0x27,
  0xfe, 0xbd, 0xf9, 0x28, 0xd8, 0xa1, 0x8b, 0xfc,
} };

/* fd_stream_is_token_program returns 1 if the given pubkey is the SPL
   Token program or Token-2022 program. */

static inline int
fd_stream_is_token_program( fd_pubkey_t const * owner ) {
  return fd_pubkey_eq( owner, &fd_solana_spl_token_id )
      || fd_pubkey_eq( owner, &fd_stream_spl_token_2022_id );
}

/* fd_stream_parse_token_account attempts to parse SPL Token account
   data.  Returns 1 on success with info populated, 0 if the data is
   not a valid initialized token account.

   data points to the account data bytes, data_sz is the length.
   The account must be owned by the SPL Token or Token-2022 program
   (caller should check this before calling). */

static inline int
fd_stream_parse_token_account( uchar const *            data,
                               ulong                    data_sz,
                               fd_stream_token_info_t * info ) {
  /* Must be at least 165 bytes for a token account */
  if( FD_UNLIKELY( data_sz<FD_SPL_TOKEN_ACCOUNT_SZ ) ) return 0;

  /* Check state field — must be Initialized or Frozen */
  uchar state = data[ 108 ];
  if( FD_UNLIKELY( state!=FD_SPL_TOKEN_STATE_INITIALIZED &&
                   state!=FD_SPL_TOKEN_STATE_FROZEN ) ) return 0;

  /* Extract fields */
  fd_memcpy( info->mint.uc,  data,      32UL );
  fd_memcpy( info->owner.uc, data+32UL, 32UL );
  info->amount = FD_LOAD( ulong, data+64UL );

  return 1;
}

/* fd_stream_parse_mint_decimals attempts to extract the decimals field
   from an SPL Token Mint account.  Returns 1 on success with decimals
   populated, 0 if the data is not a valid mint. */

static inline int
fd_stream_parse_mint_decimals( uchar const * data,
                               ulong         data_sz,
                               uchar *       decimals ) {
  if( FD_UNLIKELY( data_sz<FD_SPL_TOKEN_MINT_SZ ) ) return 0;

  /* Check is_initialized */
  if( FD_UNLIKELY( !data[ 45 ] ) ) return 0;

  *decimals = data[ 44 ];
  return 1;
}

#endif /* HEADER_fd_src_discof_stream_fd_stream_token_h */
