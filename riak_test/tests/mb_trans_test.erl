%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.

-module(mb_trans_test).

%% @doc `riak_test' module for testing transition from single bag configuration
%% to multiple bag one

-export([confirm/0]).
-include_lib("erlcloud/include/erlcloud_aws.hrl").
-include_lib("eunit/include/eunit.hrl").
%% -include_lib("riak_cs/include/riak_cs.hrl").

-define(CS_CURRENT, <<"build_paths.cs_current">>).
-define(STANCHION_CURRENT, <<"build_paths.stanchion_current">>).

-define(OLD_BUCKET,     "multi-bag-transition-old").
-define(NEW_BUCKET,     "multi-bag-transition-new").
-define(OLD_KEY_IN_OLD, "old_key_in_old").
-define(NEW_KEY_IN_OLD, "new_key_in_old").
-define(NEW_KEY_IN_NEW, "new_key_in_new").

confirm() ->
    NodesInMaster = 1,
    %% setup single bag cluster at first
    {UserConfig, {RiakNodes, CSNodes, StanchionNode}} =
        rtcs:setupNxMsingles(NodesInMaster, 4),
    OldInOldContent = setup_old_bucket_and_key(UserConfig, ?OLD_BUCKET, ?OLD_KEY_IN_OLD),
    rtcs:assert_error_log_empty(1),

    transition_to_multibag_configuration(
      NodesInMaster, lists:zip(CSNodes, RiakNodes), StanchionNode),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?NEW_BUCKET, UserConfig)),
    NewInOldContent = rand_content(),
    erlcloud_s3:put_object(?OLD_BUCKET, ?NEW_KEY_IN_OLD, NewInOldContent, UserConfig),
    NewInNewContent = rand_content(),
    erlcloud_s3:put_object(?NEW_BUCKET, ?NEW_KEY_IN_NEW, NewInNewContent, UserConfig),

    rtcs_object:assert_whole_content(
      UserConfig, ?OLD_BUCKET, ?OLD_KEY_IN_OLD, OldInOldContent),
    rtcs_object:assert_whole_content(
      UserConfig, ?OLD_BUCKET, ?NEW_KEY_IN_OLD, NewInOldContent),
    rtcs_object:assert_whole_content(
      UserConfig, ?NEW_BUCKET, ?NEW_KEY_IN_NEW, NewInNewContent),

    %% TODO: s3 list for two buckets.
    [BagA, _BagB, BagC, BagD, BagE] = RiakNodes,

    %% Assert manifests are in proper bags
    lager:info("Manifests in the old bucket are in the master bag"),
    {_, MOldInOld} = rtcs_bag:assert_manifest_in_single_bag(
                       ?OLD_BUCKET, ?OLD_KEY_IN_OLD, RiakNodes, BagA),
    {_, MNewInOld} = rtcs_bag:assert_manifest_in_single_bag(
                       ?OLD_BUCKET, ?NEW_KEY_IN_OLD, RiakNodes, BagA),
    lager:info("A manifest in the new bucket is in non-master bag C"),
    {_, MNewInNew} = rtcs_bag:assert_manifest_in_single_bag(
                       ?NEW_BUCKET, ?NEW_KEY_IN_NEW, RiakNodes, BagC),

    %% Assert blocks are in proper bags
    lager:info("Old blocks in the old bucket are in the master bag"),
    ok = rtcs_bag:assert_block_in_single_bag(?OLD_BUCKET, MOldInOld, RiakNodes, BagA),
    lager:info("New blocks in the old/new bucket are in one of non-master bags D or E"),
    [assert_block_bag(B, K, M, RiakNodes, [BagD, BagE]) ||
        {B, K, M} <- [{?OLD_BUCKET, ?NEW_KEY_IN_OLD, MNewInOld},
                      {?NEW_BUCKET, ?NEW_KEY_IN_NEW, MNewInNew}]],

    assert_gc_run(hd(CSNodes), UserConfig),
    [ok = rtcs_bag:assert_no_manifest_in_any_bag(B, K, RiakNodes) ||
        {B, K} <- [{?OLD_BUCKET, ?OLD_KEY_IN_OLD},
                   {?OLD_BUCKET, ?NEW_KEY_IN_OLD},
                   {?NEW_BUCKET, ?NEW_KEY_IN_NEW}]],
    %% [ok = rtcs_bag:assert_no_block_in_any_bag(B, M, RiakNodes) ||
    [ok = rtcs_bag:assert_no_block_in_any_bag(B, M, RiakNodes) ||
        {B, M} <- [{?OLD_BUCKET, MOldInOld},
                   {?OLD_BUCKET, MNewInOld},
                   {?NEW_BUCKET, MNewInNew}]],
    rtcs:assert_error_log_empty(1),
    pass.

assert_block_bag(Bucket, Key, Manifest, RiakNodes, [BagD, BagE]) ->
    BlockBag = case rtcs_bag:high_low({Bucket, Key, Manifest}) of
                   low  -> BagD;
                   high -> BagE
               end,
    ok = rtcs_bag:assert_block_in_single_bag(Bucket, Manifest, RiakNodes, BlockBag),
    ok.

setup_old_bucket_and_key(UserConfig, Bucket, Key) ->
    lager:info("creating bucket ~p", [Bucket]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(Bucket, UserConfig)),
    Content = rand_content(),
    erlcloud_s3:put_object(Bucket, Key, Content, UserConfig),
    Content.

rand_content() ->
    crypto:rand_bytes(4 * 1024 * 1024).

transition_to_multibag_configuration(NodesInMaster, NodeList, StanchionNode) ->
    BagConf = rtcs_bag:conf(NodesInMaster, disjoint),
    rtcs_exec:stop_cs_and_stanchion_nodes(NodeList, current),
    rtcs_exec:stop_stanchion(),
    %% Because there are noises from poolboy shutdown at stopping riak-cs,
    %% truncate error log here and re-assert emptiness of error.log file later.
    rtcs:truncate_error_log(1),

    rt:pmap(fun({_CSNode, RiakNode}) ->
                    N = rtcs_dev:node_id(RiakNode),
                    rtcs:set_conf({cs, current, N}, BagConf),
                    rtcs_exec:start_cs(N)
            end, NodeList),
    rtcs:set_conf({stanchion, current}, BagConf),
    rtcs_exec:start_stanchion(),
    [ok = rt:wait_until_pingable(CSNode) || {CSNode, _RiakNode} <- NodeList],
    rt:wait_until_pingable(StanchionNode),
    rtcs_bag:set_weights(rtcs_bag:weights(disjoint)),
    {0, ListWeightRes} = rtcs_bag:list_weight(),
    lager:info("Weight: ~s~n", [ListWeightRes]),
    ok.

assert_gc_run(CSNode, UserConfig) ->
    rtcs_exec:gc(1, "set-interval infinity"),
    rtcs_exec:gc(1, "set-leeway 1"),
    rtcs_exec:gc(1, "cancel"),

    erlcloud_s3:delete_object(?OLD_BUCKET, ?OLD_KEY_IN_OLD, UserConfig),
    timer:sleep(2000),
    erlcloud_s3:delete_object(?OLD_BUCKET, ?NEW_KEY_IN_OLD, UserConfig),
    erlcloud_s3:delete_object(?NEW_BUCKET, ?NEW_KEY_IN_NEW, UserConfig),

    %% Ensure the leeway has expired
    timer:sleep(2000),

    rt:setup_log_capture(CSNode),
    rtcs_exec:gc(1, "batch 1"),
    true = rt:expect_in_log(CSNode,
                            "Finished garbage collection: \\d+ seconds, "
                            "\\d+ batch_count, \\d+ batch_skips, "
                            "\\d+ manif_count, \\d+ block_count"),
    ok.
