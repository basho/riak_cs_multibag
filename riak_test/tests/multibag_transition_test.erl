%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.

-module(multibag_transition_test).

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
    %% setup single bag cluster at first
    {UserConfig, {RiakNodes, CSNodes, StanchionNode}} = rtcs:setupNxMsingles(1, 4),
    OldInOldContent = setup_old_bucket_and_key(UserConfig, ?OLD_BUCKET, ?OLD_KEY_IN_OLD),

    transition_to_multibag_configuration(
      UserConfig, lists:zip(CSNodes, RiakNodes), StanchionNode),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?NEW_BUCKET, UserConfig)),
    NewInOldContent = rand_content(),
    erlcloud_s3:put_object(?OLD_BUCKET, ?NEW_KEY_IN_OLD, NewInOldContent, UserConfig),
    NewInNewContent = rand_content(),
    erlcloud_s3:put_object(?NEW_BUCKET, ?NEW_KEY_IN_NEW, NewInNewContent, UserConfig),

    assert_whole_content(?OLD_BUCKET, ?OLD_KEY_IN_OLD, OldInOldContent, UserConfig),
    assert_whole_content(?OLD_BUCKET, ?NEW_KEY_IN_OLD, NewInOldContent, UserConfig),
    assert_whole_content(?NEW_BUCKET, ?NEW_KEY_IN_NEW, NewInNewContent, UserConfig),

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

transition_to_multibag_configuration(AdminConfig, NodeList, StanchionNode) ->
    Configs = rtcs_bag:configs(rtcs_bag:bags(disjoint)),
    #aws_config{access_key_id=K, secret_access_key=S} = AdminConfig,
    rtcs:stop_cs_and_stanchion_nodes(NodeList),
    rtcs:stop_stanchion(),
    rt:pmap(fun({_CSNode, RiakNode}) ->
                    N = rt_cs_dev:node_id(RiakNode),
                    rtcs:update_cs_config(rt_config:get(?CS_CURRENT),
                                          N,
                                          proplists:get_value(cs, Configs),
                                          {K, S}),
                    rtcs:start_cs(N)
            end, NodeList),
    rtcs:update_stanchion_config(rt_config:get(?STANCHION_CURRENT),
                                 proplists:get_value(stanchion, Configs),
                                 {K, S}),
    rtcs:start_stanchion(),
    [ok = rt:wait_until_pingable(CSNode) || {CSNode, _RiakNode} <- NodeList],
    rt:wait_until_pingable(StanchionNode),
    rtcs_bag:set_weights(rtcs_bag:weights(disjoint)),
    {0, ListWeightRes} = rtcs_bag:list_weight(),
    lager:info("Weight: ~s~n", [ListWeightRes]),
    ok.

assert_whole_content(Bucket, Key, ExpectedContent, Config) ->
    Obj = erlcloud_s3:get_object(Bucket, Key, Config),
    assert_whole_content(ExpectedContent, Obj).

assert_whole_content(ExpectedContent, ResultObj) ->
    Content = proplists:get_value(content, ResultObj),
    ContentLength = proplists:get_value(content_length, ResultObj),
    ?assertEqual(byte_size(ExpectedContent), list_to_integer(ContentLength)),
    ?assertEqual(byte_size(ExpectedContent), byte_size(Content)),
    ?assertEqual(ExpectedContent, Content).
