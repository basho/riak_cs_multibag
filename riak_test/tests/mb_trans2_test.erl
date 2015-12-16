%% Copyright (c) 2015 Basho Technologies, Inc.  All Rights Reserved.

-module(mb_trans2_test).

%% @doc `riak_test' module for testing transition from single bag configuration
%% to multiple bag one, another pattern with `cs_suites'.

-export([confirm/0]).
-export([transition_to_mb/2,
         set_uniform_weights/1]).

confirm() ->
    rt_config:set(console_log_level, info),
    NodesInMaster = 2,
    SetupRes = setup_single_bag(NodesInMaster),
    {ok, InitialState} = cs_suites:new(SetupRes),
    {ok, EvolvedState} = cs_suites:fold_with_state(InitialState, history(NodesInMaster)),
    {ok, _FinalState}  = cs_suites:cleanup(EvolvedState),
    pass.

custom_configs() ->
    %% This branch is only for debugging this module
    [{riak, [{bitcask, [{max_file_size, 4*1024*1024}]}]},
     {cs,   [{riak_cs, [{leeway_seconds, 1}]}]}].

history(NodesInMaster) ->
    [
     {cs_suites, run,                 ["single-bag"]},
     {?MODULE  , transition_to_mb,    [NodesInMaster]},
     {cs_suites, run,                 ["mb-disjoint"]},
     {?MODULE  , set_uniform_weights, []},
     {cs_suites, run,                 ["mb-uniform"]}
    ].

setup_single_bag(NodesInMaster) ->
    rtcs:setupNxMsingles(NodesInMaster, 4, custom_configs(), current).

transition_to_mb(NodesInMaster, State) ->
    RiakNodes = cs_suites:nodes_of(riak, State),
    [StanchionNode] = cs_suites:nodes_of(stanchion, State),
    CsNodes = cs_suites:nodes_of(cs, State),
    NodeList = lists:zip(CsNodes, RiakNodes),
    BagConf = rtcs_bag:conf(NodesInMaster, disjoint),
    rtcs_exec:stop_cs_and_stanchion_nodes(NodeList, current),
    rtcs_exec:stop_stanchion(),
    %% Because there are noises from poolboy shutdown at stopping riak-cs,
    %% truncate error log here and re-assert emptiness of error.log file later.
    rtcs:truncate_error_log(1),

    rt:pmap(fun({_CSNode, RiakNode}) ->
                    N = rtcs_dev:node_id(RiakNode),
                    rtcs:set_conf({cs, current, N}, BagConf),
                    %% dev1 is the master cluster, so all CS nodes are configured as that
                    %% Also could be dev2, but not dev3 or later.
                    rtcs:set_advanced_conf({cs, current, N},
                                           [{riak_cs,
                                             [{riak_host, {"127.0.0.1", rtcs_config:pb_port(1)}}]}]),
                    rtcs_exec:start_cs(N)
            end, NodeList),
    rtcs:set_conf({stanchion, current}, BagConf),
    rtcs_exec:start_stanchion(),
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
