%% Riak's bucket and key to store weight information
-define(WEIGHT_BUCKET, <<"riak-cs-multibag">>).
-define(WEIGHT_KEY,    <<"weight">>).

-record(weight_info, {
          bag_id :: riak_cs_multibag:bag_id(),
          weight :: non_neg_integer(),
          opts = [] :: proplists:proplist()   %% Not used
          }).
