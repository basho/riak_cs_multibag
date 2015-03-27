%% Copyright (c) 2015 Basho Technologies, Inc.  All Rights Reserved.

-module(mb_trans2_test).

%% @doc `riak_test' module for testing transition from single bag configuration
%% to multiple bag one, another pattern with `cs_suites'.

-export([confirm/0]).
-export([transition_to_mb/1,
         set_uniform_weights/1]).

confirm() ->
    rt_config:set(console_log_level, info),
    SetupRes = setup_single_bag(),
    {ok, InitialState} = cs_suites:new(SetupRes),
    {ok, EvolvedState} = cs_suites:fold_with_state(InitialState, history()),
    {ok, _FinalState}  = cs_suites:cleanup(EvolvedState),
    pass.

custom_configs() ->
    %% This branch is only for debugging this module
    [{riak,
      rtcs:riak_config([{bitcask, [{max_file_size, 4*1024*1024}]}])},
     {cs, rtcs:cs_config([{leeway_seconds, 1}])}].

custom_configs(MultiBags) ->
    %% This branch is only for debugging this module
    [{riak,
      rtcs:riak_config([{bitcask, [{max_file_size, 4*1024*1024}]}])},
     {cs, rtcs:cs_config([{leeway_seconds, 1}],
                         [{riak_cs_multibag, [{bags, MultiBags}]}])},
     {stanchion, rtcs:stanchion_config([{bags, MultiBags}])}].

history() ->
    [
     {cs_suites, run,                 ["single-bag"]},
     {?MODULE  , transition_to_mb,    []},
     {cs_suites, run,                 ["mb-disjoint"]},
     {?MODULE  , set_uniform_weights, []},
     {cs_suites, run,                 ["mb-uniform"]}
    ].
    
setup_single_bag() ->
    rtcs:setupNxMsingles(2, 4, custom_configs(), current).

transition_to_mb(State) ->
    RiakNodes = cs_suites:nodes_of(riak, State),
    [StanchionNode] = cs_suites:nodes_of(stanchion, State),
    CsNodes = cs_suites:nodes_of(cs, State),
    NodeList = lists:zip(CsNodes, RiakNodes),
    AdminCredential = cs_suites:admin_credential(State),
    Configs = custom_configs(rtcs_bag:bags(disjoint)),
    rtcs:stop_cs_and_stanchion_nodes(NodeList, current),
    rtcs:stop_stanchion(),
    %% Because there are noises from poolboy shutdown at stopping riak-cs,
    %% truncate error log here and re-assert emptiness of error.log file later.
    rtcs:truncate_error_log(1),

    rt:pmap(fun({_CSNode, RiakNode}) ->
                    N = rt_cs_dev:node_id(RiakNode),
                    rtcs:update_cs_config(rtcs:get_rt_config(cs, current),
                                          N,
                                          proplists:get_value(cs, Configs),
                                          AdminCredential),
                    rtcs:start_cs(N)
            end, NodeList),
    rtcs:update_stanchion_config(rtcs:get_rt_config(stanchion, current),
                                 proplists:get_value(stanchion, Configs),
                                 AdminCredential),
    rtcs:start_stanchion(),
    [ok = rt:wait_until_pingable(CsNode) || CsNode <- CsNodes],
    ok = rt:wait_until_pingable(StanchionNode),
    rt:setup_log_capture(hd(cs_suites:nodes_of(cs, State))),
    rtcs_bag:set_weights(rtcs_bag:weights(disjoint)),
    {0, ListWeightRes} = rtcs_bag:list_weight(),
    lager:info("Weight disjoint: ~s~n", [ListWeightRes]),
    {ok, State}.
    
set_uniform_weights(State) ->
    rtcs_bag:set_weights(uniform_all_weights()),
    {0, ListWeightRes} = rtcs_bag:list_weight(),
    lager:info("Weight disjoint: ~s~n", [ListWeightRes]),
    {ok, State}.

uniform_all_weights() ->
    [{all, "bag-A", 100},
     {all, "bag-B", 100},
     {all, "bag-C", 100},
     {all, "bag-D", 100},
     {all, "bag-E", 100}].
    

%%     OldInOldContent = setup_old_bucket_and_key(UserConfig, ?OLD_BUCKET, ?OLD_KEY_IN_OLD),
%%     rtcs:assert_error_log_empty(1),

%%     transition_to_multibag_configuration(
%%       UserConfig, lists:zip(CSNodes, RiakNodes), StanchionNode),
%%     ?assertEqual(ok, erlcloud_s3:create_bucket(?NEW_BUCKET, UserConfig)),
%%     NewInOldContent = rand_content(),
%%     erlcloud_s3:put_object(?OLD_BUCKET, ?NEW_KEY_IN_OLD, NewInOldContent, UserConfig),
%%     NewInNewContent = rand_content(),
%%     erlcloud_s3:put_object(?NEW_BUCKET, ?NEW_KEY_IN_NEW, NewInNewContent, UserConfig),

%%     assert_whole_content(?OLD_BUCKET, ?OLD_KEY_IN_OLD, OldInOldContent, UserConfig),
%%     assert_whole_content(?OLD_BUCKET, ?NEW_KEY_IN_OLD, NewInOldContent, UserConfig),
%%     assert_whole_content(?NEW_BUCKET, ?NEW_KEY_IN_NEW, NewInNewContent, UserConfig),

%%     %% TODO: s3 list for two buckets.
%%     [BagA, _BagB, BagC, BagD, BagE] = RiakNodes,

%%     %% Assert manifests are in proper bags
%%     lager:info("Manifests in the old bucket are in the master bag"),
%%     {_, MOldInOld} = rtcs_bag:assert_manifest_in_single_bag(
%%                        ?OLD_BUCKET, ?OLD_KEY_IN_OLD, RiakNodes, BagA),
%%     {_, MNewInOld} = rtcs_bag:assert_manifest_in_single_bag(
%%                        ?OLD_BUCKET, ?NEW_KEY_IN_OLD, RiakNodes, BagA),
%%     lager:info("A manifest in the new bucket is in non-master bag C"),
%%     {_, MNewInNew} = rtcs_bag:assert_manifest_in_single_bag(
%%                        ?NEW_BUCKET, ?NEW_KEY_IN_NEW, RiakNodes, BagC),

%%     %% Assert blocks are in proper bags
%%     lager:info("Old blocks in the old bucket are in the master bag"),
%%     ok = rtcs_bag:assert_block_in_single_bag(?OLD_BUCKET, MOldInOld, RiakNodes, BagA),
%%     lager:info("New blocks in the old/new bucket are in one of non-master bags D or E"),
%%     [assert_block_bag(B, K, M, RiakNodes, [BagD, BagE]) ||
%%         {B, K, M} <- [{?OLD_BUCKET, ?NEW_KEY_IN_OLD, MNewInOld},
%%                       {?NEW_BUCKET, ?NEW_KEY_IN_NEW, MNewInNew}]],

%%     assert_gc_run(hd(CSNodes), UserConfig),
%%     [ok = rtcs_bag:assert_no_manifest_in_any_bag(B, K, RiakNodes) ||
%%         {B, K} <- [{?OLD_BUCKET, ?OLD_KEY_IN_OLD},
%%                    {?OLD_BUCKET, ?NEW_KEY_IN_OLD},
%%                    {?NEW_BUCKET, ?NEW_KEY_IN_NEW}]],
%%     %% [ok = rtcs_bag:assert_no_block_in_any_bag(B, M, RiakNodes) ||
%%     [ok = rtcs_bag:assert_no_block_in_any_bag(B, M, RiakNodes) ||
%%         {B, M} <- [{?OLD_BUCKET, MOldInOld},
%%                    {?OLD_BUCKET, MNewInOld},
%%                    {?NEW_BUCKET, MNewInNew}]],
%%     rtcs:assert_error_log_empty(1),
%%     pass.

%% assert_block_bag(Bucket, Key, Manifest, RiakNodes, [BagD, BagE]) ->
%%     BlockBag = case rtcs_bag:high_low({Bucket, Key, Manifest}) of
%%                    low  -> BagD;
%%                    high -> BagE
%%                end,
%%     ok = rtcs_bag:assert_block_in_single_bag(Bucket, Manifest, RiakNodes, BlockBag),
%%     ok.

%% setup_old_bucket_and_key(UserConfig, Bucket, Key) ->
%%     lager:info("creating bucket ~p", [Bucket]),
%%     ?assertEqual(ok, erlcloud_s3:create_bucket(Bucket, UserConfig)),
%%     Content = rand_content(),
%%     erlcloud_s3:put_object(Bucket, Key, Content, UserConfig),
%%     Content.

%% rand_content() ->
%%     crypto:rand_bytes(4 * 1024 * 1024).


%% assert_whole_content(Bucket, Key, ExpectedContent, Config) ->
%%     Obj = erlcloud_s3:get_object(Bucket, Key, Config),
%%     assert_whole_content(ExpectedContent, Obj).

%% assert_whole_content(ExpectedContent, ResultObj) ->
%%     Content = proplists:get_value(content, ResultObj),
%%     ContentLength = proplists:get_value(content_length, ResultObj),
%%     ?assertEqual(byte_size(ExpectedContent), list_to_integer(ContentLength)),
%%     ?assertEqual(byte_size(ExpectedContent), byte_size(Content)),
%%     ?assertEqual(ExpectedContent, Content).

%% assert_gc_run(CSNode, UserConfig) ->
%%     rtcs:gc(1, "set-interval infinity"),
%%     rtcs:gc(1, "set-leeway 1"),
%%     rtcs:gc(1, "cancel"),

%%     erlcloud_s3:delete_object(?OLD_BUCKET, ?OLD_KEY_IN_OLD, UserConfig),
%%     timer:sleep(2000),
%%     erlcloud_s3:delete_object(?OLD_BUCKET, ?NEW_KEY_IN_OLD, UserConfig),
%%     erlcloud_s3:delete_object(?NEW_BUCKET, ?NEW_KEY_IN_NEW, UserConfig),
%%     %% [erlcloud_s3:delete_object(B, K, UserConfig) ||
%%     %%     {B, K} <- [{?OLD_BUCKET, ?OLD_KEY_IN_OLD},
%%     %%                {?OLD_BUCKET, ?NEW_KEY_IN_OLD},
%%     %%                {?NEW_BUCKET, ?NEW_KEY_IN_NEW}]],

%%     %% Ensure the leeway has expired
%%     timer:sleep(2000),

%%     rt:setup_log_capture(CSNode),
%%     rtcs:gc(1, "batch 1"),
%%     true = rt:expect_in_log(CSNode,
%%                             "Finished garbage collection: \\d+ seconds, "
%%                             "\\d+ batch_count, \\d+ batch_skips, "
%%                             "\\d+ manif_count, \\d+ block_count"),
%%     ok.
