%% @doc These functions are used by the riak-cs-bag command line script.

-module(riak_cs_bag_console).

-export([status/1, input/1, refresh/1]).

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

-define(SCRIPT_NAME, "riak-cs-bag").

%%%===================================================================
%%% Public API
%%%===================================================================

%% @doc Show weight information for all bags
status(_Opts) ->
    ?SAFELY(get_status(), "Show current weight information").

refresh(_Opts) ->
    ?SAFELY(handle_result(riak_cs_bag_worker:refresh()),
            "Refresh weight information").

input(Args) ->
    ?SAFELY(handle_result(riak_cs_bag_worker:input(parse_input_args(Args))),
            "Updating the weight information").

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_status() ->
    handle_status(riak_cs_bag_server:status()).

handle_status({ok, Status}) ->
    %% TODO: More readable format
    io:format("~p~n", [Status]),
    ok;
handle_status({error, Reason}) ->
    io:format("Error: ~p~n", [Reason]).

parse_input_args([JsonString]) ->
    mochijson2:decode(JsonString).

handle_result(ok) ->
    ok;
handle_result({ok, Result}) ->
    io:format("~p~n", [Result]),
    ok;
handle_result({error, Reason}) ->
    io:format("Error: ~p~n", [Reason]),
    ok.
