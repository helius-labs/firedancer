#ifndef HEADER_fd_src_flamenco_rewards_fd_rewards_h
#define HEADER_fd_src_flamenco_rewards_fd_rewards_h

/* fd_rewards.h provides APIs for distributing Solana staking rewards. */

#include "../stakes/fd_stake_delegations.h"
#include "../runtime/sysvar/fd_sysvar_base.h"

struct fd_prev_epoch_inflation_rewards {
  ulong  validator_rewards;
  double prev_epoch_duration_in_years;
  double validator_rate;
  double foundation_rate;
};
typedef struct fd_prev_epoch_inflation_rewards fd_prev_epoch_inflation_rewards_t;

struct fd_partitioned_rewards_calculation {
  uint128 validator_points;
  ulong   old_vote_balance_and_staked;
  ulong   validator_rewards;
  double  validator_rate;
  double  foundation_rate;
  double  prev_epoch_duration_in_years;
  ulong   capitalization;
};
typedef struct fd_partitioned_rewards_calculation fd_partitioned_rewards_calculation_t;

FD_PROTOTYPES_BEGIN

/* fd_begin_partitioned_rewards updates epoch bank stake and vote
   account reward calculations.  Updates vote accounts with payouts and
   increases capitalization.  Called in the epoch boundary (start of
   first block of an epoch).

   Call stack is as follows:
   - begin_partitioned_rewards
     - calculate_rewards_and_distribute_vote_rewards
       - calculate_rewards_for_partitioning
         - calculate_reward_points_partitioned (calculates total points)
           - calculate_points_all
         - calculate_stake_vote_rewards (calculates reward list)
           - calculate_stake_vote_rewards_account
             - for each delegation: redeem_rewards
               - calculate_stake_rewards
                 - calculate_stake_points_and_credits
       - ... update all vote accounts ...
     - ... update epoch rewards bank field ... */

void
fd_begin_partitioned_rewards( fd_bank_t *                    bank,
                              fd_accdb_t *                   accdb,
                              fd_runtime_stack_t *           runtime_stack,
                              fd_capture_ctx_t *             capture_ctx,
                              fd_stake_delegations_t const * stake_delegations,
                              fd_hash_t const *              parent_blockhash,
                              ulong                          parent_epoch );

/* fd_rewards_recalculate_partitioned_rewards restores epoch bank stake
   and account reward calculations.  Does not update accounts.  Called
   when restoring replay state from a snapshot.

   Call stack is as follows:
   - calculate_stake_vote_rewards (calculates reward list)
     - calculate_stake_vote_rewards_account
       - for each delegation: redeem_rewards
         - calculate_stake_rewards
           - calculate_stake_points_and_credits */

void
fd_rewards_recalculate_partitioned_rewards( fd_banks_t *         banks,
                                            fd_bank_t *          bank,
                                            fd_accdb_t *         accdb,
                                            fd_runtime_stack_t * runtime_stack,
                                            fd_capture_ctx_t *   capture_ctx );

/* fd_distribute_partitioned_epoch_rewards pays out rewards to stake
   accounts.  Called at the beginning of a few slots per epoch.

   Call stack is as follows:
   - distribute_epoch_rewards_in_partition
     - for each stake account: distribute_epoch_reward_to_stake_acc */

void
fd_distribute_partitioned_epoch_rewards( fd_bank_t *        bank,
                                         fd_accdb_t *       accdb,
                                         fd_capture_ctx_t * capture_ctx );

/* fd_rewards_get_reward_distribution_num_blocks returns the number of
   blocks required to distribute rewards for a given epoch schedule and
   stake account count. Useful for testing partition sizing logic. */

uint
fd_rewards_get_reward_distribution_num_blocks( fd_epoch_schedule_t const * epoch_schedule,
                                               ulong                       slot,
                                               ulong                       total_stake_accounts,
                                               ulong                       stake_account_stores_per_block );

struct fd_commission_split {
  ulong voter_portion;
  ulong staker_portion;
  uint  is_split;
};

typedef struct fd_commission_split fd_commission_split_t;

void
fd_vote_commission_split( ushort                  commission,
                          ulong                   on,
                          fd_commission_split_t * result );

/* Reward sink: thread-local callback for capturing per-validator
   rewards during epoch distribution.  Set by the replay tile before
   calling fd_runtime_block_execute_prepare. */

struct fd_reward_sink_entry {
  uchar  pubkey[32];
  long   lamports;       /* reward amount (can be negative for rent) */
  ulong  post_balance;   /* balance after reward */
  uchar  reward_type;    /* 0=Unspecified, 1=Fee, 2=Rent, 3=Staking, 4=Voting */
  uchar  commission;     /* validator commission (0-100) */
  uchar  _pad[6];
};
typedef struct fd_reward_sink_entry fd_reward_sink_entry_t;

struct fd_reward_sink {
  fd_reward_sink_entry_t * buf;
  ulong                    cnt;
  ulong                    max;
};
typedef struct fd_reward_sink fd_reward_sink_t;

extern FD_TL fd_reward_sink_t * fd_reward_sink;

/* Call from replay tile before/after block prepare */
void fd_reward_sink_set( fd_reward_sink_t * sink );
void fd_reward_sink_clear( void );

FD_PROTOTYPES_END

#endif /* HEADER_fd_src_flamenco_rewards_fd_rewards_h */
