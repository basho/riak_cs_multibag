%% Riak's bucket and key to store weight information
-define(WEIGHT_BUCKET, <<"riak-cs-bag">>).
-define(WEIGHT_KEY,    <<"weight">>).

-record(weight_info, {
          bag_id :: riak_cs_bag:bag_id(),
          weight :: non_neg_integer(),
          free :: non_neg_integer(),     % not used currently
          total :: non_neg_integer()     % not used currently
          }).
