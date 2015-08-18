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
      rtcs_config:riak_config([{bitcask, [{max_file_size, 4*1024*1024}]}])},
     {cs, rtcs_config:cs_config([{leeway_seconds, 1}])}].

custom_configs(MultiBags) ->
    %% This branch is only for debugging this module
    [{riak,
      rtcs_config:riak_config([{bitcask, [{max_file_size, 4*1024*1024}]}])},
     {cs, rtcs_config:cs_config([{leeway_seconds, 1}],
                         [{riak_cs_multibag, [{bags, MultiBags}]}])},
     {stanchion, rtcs_config:stanchion_config([{bags, MultiBags}])}].

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
    rtcs_exec:stop_cs_and_stanchion_nodes(NodeList, current),
    rtcs_exec:stop_stanchion(),
    %% Because there are noises from poolboy shutdown at stopping riak-cs,
    %% truncate error log here and re-assert emptiness of error.log file later.
    rtcs:truncate_error_log(1),

    rt:pmap(fun({_CSNode, RiakNode}) ->
                    N = rt_cs_dev:node_id(RiakNode),
                    rtcs_config:update_cs_config(rtcs_config:get_rt_config(cs, current),
                                          N,
                                          proplists:get_value(cs, Configs),
                                          AdminCredential),
                    rtcs_exec:start_cs(N)
            end, NodeList),
    rtcs_config:update_stanchion_config(rtcs_config:get_rt_config(stanchion, current),
                                 proplists:get_value(stanchion, Configs),
                                 AdminCredential),
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
