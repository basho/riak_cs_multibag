%% Copyright (c) 2015 Basho Technologies, Inc.  All Rights Reserved.

-module(mb_pg_test).

%% @doc `riak_test' module for testing multi bag proxy_get feature

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-include("riak_cs.hrl").

-define(BUCKET_PREFIX, "test").
-define(BUCKET_COUNT,  3).
-define(KEY_NORMAL,    "normal").
-define(KEY_MP,        "mp").

%%                   WEST:source                 EAST:site
%%              +->  Node1                       Node5 <-+
%%  bag-A       |      |    <-------repl------->   |     |
%%              |    Node2                       Node6   |
%%         CS1 -+                                        +- CS5
%%              |                                        |
%%  bag-B       +->  Node3  <-------repl-------> Node7 <-+
%%  bag-C       +->  Node4  <-------repl-------> Node8 <-+

confirm() ->
    Pairs = setup_clusters(),
    {{_WestALeader, WestANodes, _WestAName},
     {_EastALeader, EastANodes, _EastAName}} = hd(Pairs),

    UserWest = rtcs_admin:create_user(hd(WestANodes), 1),
    UserEast = rtcs_admin:aws_config(UserWest,[{port, rtcs_config:cs_port(hd(EastANodes))}]),

    lager:info("Initialize weights by zero, without multibag"),
    set_zero_weight(),
    [?assertEqual(ok, erlcloud_s3:create_bucket(
                        ?BUCKET_PREFIX ++ integer_to_list(K),
                        UserWest)) ||
        K <- lists:seq(1, ?BUCKET_COUNT)],
    Objects0 = upload_objects(UserWest, UserEast),

    lager:info("Update weights with non-zero values, transition to multibag"),
    set_weights(),
    lager:info("Assert proxy get for object uploaded BEFORE multibag transition..."),
    assert_proxy_get(UserWest, UserEast, Objects0),

    lager:info("Test proxy get for objects uploaded AFTER multibag transition..."),
    upload_and_assert_proxy_get(UserWest, UserEast),

    lager:info("Disable proxy_get and confirm it does not work actually."),
    [rtcs_exec:disable_proxy_get(rtcs_dev:node_id(WLeader), current, EName) ||
        {{WLeader, _WNodes, _WName}, {_ELeader, _ENodes, EName}} <- Pairs],
    assert_proxy_get_does_not_work(UserWest, UserEast),

    lager:info("Enable proxy_get again."),
    [rtcs_exec:enable_proxy_get(rtcs_dev:node_id(WLeader), current, EName) ||
        {{WLeader, _WNodes, _WName}, {_ELeader, _ENodes, EName}} <- Pairs],
    upload_and_assert_proxy_get(UserWest, UserEast),

    pass.

upload_and_assert_proxy_get(UserWest, UserEast) ->
    Objects = upload_objects(UserWest, UserEast),
    assert_proxy_get(UserWest, UserEast, Objects).

upload_objects(UserWest, _UserEast) ->
    lager:info("Upload objects at West"),
    Normals = [rtcs_object:upload(UserWest, normal,
                                  ?BUCKET_PREFIX ++ integer_to_list(K), ?KEY_NORMAL) ||
                  K <- lists:seq(1, ?BUCKET_COUNT)],
    MPs =  [rtcs_object:upload(UserWest, multipart,
                               ?BUCKET_PREFIX ++ integer_to_list(K), ?KEY_MP) ||
                  K <- lists:seq(1, ?BUCKET_COUNT)],
    NormalCopies = [begin
                        DstKey = K ++ "-copy",
                        rtcs_object:upload(UserWest, normal_copy, B, DstKey, K),
                        {B, DstKey, Content}
                    end || {B, K, Content} <- Normals],
    MPCopies = [begin
                    DstKey = K ++ "-copy",
                    rtcs_object:upload(UserWest, multipart_copy, B, DstKey, K),
                    {B, DstKey, Content}
                end || {B, K, Content} <- MPs],
    {Normals, MPs, NormalCopies, MPCopies}.

assert_proxy_get(_UserWest, UserEast, {Normals, MPs, NormalCopies, MPCopies}) ->
    lager:info("Try to download them from East..."),
    [rtcs_object:assert_whole_content(UserEast, B, K, Content) ||
        {B, K, Content} <- Normals],
    [rtcs_object:assert_whole_content(UserEast, B, K, Content) ||
        {B, K, Content} <- MPs],
    [rtcs_object:assert_whole_content(UserEast, B, K, Content) ||
        {B, K, Content} <- NormalCopies],
    [rtcs_object:assert_whole_content(UserEast, B, K, Content) ||
        {B, K, Content} <- MPCopies],
    lager:info("Got every object via proxy_get. Perfect."),
    ok.

assert_proxy_get_does_not_work(UserWest, UserEast) ->
    lager:info("Upload objects at West"),
    Normals = [rtcs_object:upload(UserWest, normal,
                                  ?BUCKET_PREFIX ++ integer_to_list(K), ?KEY_NORMAL) ||
                  K <- lists:seq(1, ?BUCKET_COUNT)],
    MPs =  [rtcs_object:upload(UserWest, multipart,
                               ?BUCKET_PREFIX ++ integer_to_list(K), ?KEY_MP) ||
                  K <- lists:seq(1, ?BUCKET_COUNT)],
    lager:info("Try to download them from East, all should fail..."),
    [?assertError({aws_error,{socket_error,retry_later}},
                  erlcloud_s3:get_object(B, K, UserEast)) ||
        {B, K, _Content} <- Normals],
    [?assertError({aws_error,{socket_error,retry_later}},
                  erlcloud_s3:get_object(B, K, UserEast)) ||
        {B, K, _Content} <- MPs],
    lager:info("Got connection close errors AS EXPECTED. Yay!."),
    ok.

setup_clusters() ->
    JoinFun = fun(Nodes) ->
                      [WA1, WA2, _WB1, _WC1, EA1, EA2, _EB1, _EC1] = Nodes,
                      rt:join(WA2, WA1),
                      rt:join(EA2, EA1)
              end,
    WBagConf = rtcs_bag:conf(2, 1, shared),
    EBagConf = rtcs_bag:conf(2, 5, shared),
    rtcs:set_conf(stanchion, WBagConf),
    rt:pmap(fun(N) ->
                    rtcs:set_conf({cs, current, N}, WBagConf)
            end, lists:seq(1, 4)),
    rt:pmap(fun(N) ->
                    rtcs:set_conf({cs, current, N}, EBagConf)
            end, lists:seq(5, 8)),
    {_AdminConfig, {RiakNodes, _CSs, _Stanchion}} =
        rtcs:setup_clusters(rtcs_config:configs([]), JoinFun, 8, current),

    [WestA1, WestA2, WestB, WestC, EastA1, EastA2, EastB, EastC] = RiakNodes,
    %% Name and connect v3 repl
    WestClusters = [{[WestA1, WestA2], "West-A"}, {[WestB], "West-B"}, {[WestC], "West-C"}],
    EastClusters = [{[EastA1, EastA2], "East-A"}, {[EastB], "East-B"}, {[EastC], "East-C"}],
    AllClusters = WestClusters ++ EastClusters,
    [repl_helpers:name_cluster(hd(Nodes), Name) || {Nodes, Name} <- AllClusters],
    [rt:wait_until_ring_converged(Nodes) || {Nodes, _Name} <- AllClusters],
    [repl_helpers:wait_until_13_leader(hd(Nodes)) || {Nodes, _} <- AllClusters],

    WestLeaders = [{rpc:call(hd(Nodes), riak_core_cluster_mgr, get_leader, []),
                    Nodes, Name} ||
                      {Nodes, Name} <- WestClusters],
    EastLeaders = [{rpc:call(hd(Nodes), riak_core_cluster_mgr, get_leader, []),
                    Nodes, Name} ||
                      {Nodes, Name} <- EastClusters],
    AllLeaders = WestLeaders ++ EastLeaders,
    [rpc:call(L, riak_repl_console, modes, [["mode_repl13"]]) || {L,_,_} <- AllLeaders],

    Pairs = lists:zip(WestLeaders, EastLeaders),
    [begin
         {ok, {_IP, EPort}} = rpc:call(hd(ENodes), application, get_env,
                                       [riak_core, cluster_mgr]),
         repl_helpers:connect_clusters13(WLeader, WNodes, EPort, EName),
         ?assertEqual(ok, repl_helpers:wait_for_connection13(WLeader, EName)),
         rt:wait_until_ring_converged(WNodes),
         repl_helpers:enable_realtime(WLeader, EName),
         repl_helpers:start_realtime(WLeader, EName),
         rtcs_exec:enable_proxy_get(rtcs_dev:node_id(WLeader), current, EName),
         Status = rpc:call(WLeader, riak_repl_console, status, [quiet]),
         case proplists:get_value(proxy_get_enabled, Status) of
             undefined -> ?assert(false);
             EnabledFor -> lager:info("PG enabled for cluster ~p",[EnabledFor])
         end,
         rt:wait_until_ring_converged(WNodes),
         rt:wait_until_ring_converged(ENodes),
         ok
     end || {{WLeader, WNodes, _WName}, {_ELeader, ENodes, EName}} <- Pairs],

    lager:info("Replication setup finished."),
    Pairs.

set_zero_weight() ->
    rtcs_bag:set_zero_weight(),
    [rtcs_bag:bag_refresh(N) || N <- lists:seq(1, 8)],
    ok.

set_weights() ->
    rtcs_bag:set_weights(shared),
    [rtcs_bag:bag_refresh(N) || N <- lists:seq(1, 8)],
    ok.
