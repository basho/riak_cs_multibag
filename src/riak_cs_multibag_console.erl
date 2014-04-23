%% @doc These functions are used by the riak-cs-bag command line script.

-module(riak_cs_multibag_console).

-export(['list-bags'/1, weight/1, 'weight-manifest'/1, 'weight-block'/1]).
-export([show_weights/1, show_weights_for_bag/2, refresh/1]).

-include("riak_cs_multibag.hrl").

-define(SAFELY(Code, Description),
        try
            Code
        catch
            Type:Reason ->
                ST = erlang:get_stacktrace(),
                io:format("~s failed:~n  ~p:~p~n  ~p",
                          [Description, Type, Reason, ST]),
                error
        end).

-define(SCRIPT_NAME, "riak-cs-multibag").

%%%===================================================================
%%% Public API
%%%===================================================================

'list-bags'(_Args) ->
    ?SAFELY(list_bags(), "List all bags").

weight([]) ->
    ?SAFELY(with_status(fun show_weights/1), "List all weights");
weight([BagId]) ->
    ?SAFELY(with_status(fun(Status) -> show_weights_for_bag(BagId, Status) end),
            "List weights for the bag");
weight([BagId, Weight]) ->
    ?SAFELY(set_weight(BagId, Weight), "Set weight for the bag");
weight(_) ->
    io:foramt("Invalid arguments"),
    error.

'weight-manifest'(Args) ->
    weight_by_type(manifest, Args).

'weight-block'(Args) ->
    weight_by_type(block, Args).

refresh(_Opts) ->
    ?SAFELY(handle_result(riak_cs_multibag_weight_updater:refresh()),
            "Refresh weight information").

%%%===================================================================
%%% Internal functions
%%%===================================================================

weight_by_type(Type, []) ->
    ?SAFELY(with_status(fun(Status) -> show_weights(Type, Status) end),
            io_lib:format("List all ~s weights", [Type]));
weight_by_type(Type, [BagId]) ->
    ?SAFELY(with_status(fun(Status) -> show_weights_for_bag(Type, BagId, Status) end),
            io_lib:format("List ~s weights for the bag", [Type]));
weight_by_type(Type, [BagId, Weight]) ->
    ?SAFELY(set_weight(Type, BagId, Weight),
            io_lib:format("Set ~s weight for the bag", [Type]));
weight_by_type(_Type, _) ->
    io:foramt("Invalid arguments"),
    error.

list_bags() ->
    [print_bag(Name, Type, BagId, Opts) ||
        {Name, Type, BagId, Opts} <- riak_cs_multibag:list_pool(request_pool)].

print_bag(_Name, _Type, BagId, Opts) ->
    {address, Address} = lists:keyfind(address, 1, Opts),
    {port, Port} =  lists:keyfind(port, 1, Opts),
    io:format("~s ~s:~B~n", [BagId, Address, Port]).

show_weights(Status) ->
    show_weights(manifest, Status),
    show_weights(block, Status).

show_weights(Type, Status) ->
    WeightInfoList = proplists:get_value(Type, Status),
    [io:format("~s (~s): ~B~n", [BagId, Type, Weight]) ||
        #weight_info{bag_id=BagId, weight=Weight} <- WeightInfoList].

show_weights_for_bag(BagId, Status) ->
    show_weights_for_bag(manifest, BagId, Status),
    show_weights_for_bag(block, BagId, Status).

show_weights_for_bag(Type, InputBagIdStr, Status) ->
    InputBagId = list_to_binary(InputBagIdStr),
    WeightInfoList = proplists:get_value(Type, Status),
    [io:format("~s (~s): ~B~n", [BagId, Type, Weight]) ||
        #weight_info{bag_id=BagId, weight=Weight} <- WeightInfoList,
        BagId =:= InputBagId].

set_weight(BagIdStr, WeightStr) ->
    BagId = list_to_binary(BagIdStr),
    Weight = list_to_integer(WeightStr),
    case lists:member(BagId, all_bag_ids()) of
        false ->
            io:format("Error: invalid bag ID~n"),
            error;
        _ ->
            riak_cs_multibag_weight_updater:set_weight(
              #weight_info{bag_id=BagId, weight=Weight})
    end.

set_weight(Type, BagIdStr, WeightStr) ->
    BagId = list_to_binary(BagIdStr),
    Weight = list_to_integer(WeightStr),
    case lists:member(BagId, all_bag_ids()) of
        false ->
            io:format("Error: invalid bag ID~n"),
            error;
        _ ->
            riak_cs_multibag_weight_updater:set_weight_by_type(
              Type, #weight_info{bag_id=BagId, weight=Weight})
    end.

with_status(Fun) ->
    case riak_cs_multibag_server:status() of
        {error, Reason} ->
            io:format("Error: ~p~n", [Reason]),
            error;
        {ok, Status} ->
            case proplists:get_value(initialized, Status) of
                false ->
                    io:format("Error: not initialized.~n"),
                    error;
                _ ->
                    Fun(Status)
            end
    end.

all_bag_ids() ->
    [BagId ||
        {_Name, _Type, BagId, _Opts} <- riak_cs_multibag:list_pool(request_pool)].

handle_result(ok) ->
    ok;
handle_result({ok, Result}) ->
    io:format("~p~n", [Result]),
    ok;
handle_result({error, Reason}) ->
    io:format("Error: ~p~n", [Reason]),
    ok.
