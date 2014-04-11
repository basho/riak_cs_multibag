%% @doc The worker process to update weight information
%% periodically or when triggered by command.
%% Because GET/PUT may block, e.g. network failure, updating
%% task is done by the dedicated process.

-module(riak_cs_multi_bag_weight_updater).

-behavior(gen_server).

-export([start_link/1]).
-export([status/0, input/1, refresh/0, weights/0]).
-export([refresh_interval/0, set_refresh_interval/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("riak_cs_multi_bag.hrl").

-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {
          timer_ref :: reference() | undefined,
          %% Consecutive refresh failures
          failed_count = 0 :: non_neg_integer(),
          weights = [] :: [{riak_cs_multi_bag:pool_type(),
                            [{riak_cs_multi_bag:pool_key(),
                              riak_cs_multi_bag:weight_info()}]}],
          conn_open_fun :: fun(),
          conn_close_fun :: fun()
         }).

-define(SERVER, ?MODULE).
-define(REFRESH_INTERVAL, 900). % 900 sec = 15 min

start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Args, []).

status() ->
    gen_server:call(?SERVER, status).

refresh() ->
    gen_server:call(?SERVER, refresh).

input(Json) ->
    gen_server:call(?SERVER, {input, Json}).

weights() ->
    gen_server:call(?SERVER, weights).

refresh_interval() ->
    case application:get_env(riak_cs_multi_bag, weight_refresh_interval) of
        undefined -> ?REFRESH_INTERVAL;
        {ok, Value} -> Value
    end.

set_refresh_interval(Interval) when is_integer(Interval) andalso Interval > 0 ->
    application:set_env(riak_cs_multi_bag, weight_refresh_interval, Interval).

init(Args) ->
    {conn_open_mf, {OpenM, OpenF}} = lists:keyfind(conn_open_mf, 1, Args),
    {conn_close_mf, {CloseM, CloseF}} = lists:keyfind(conn_close_mf, 1, Args),
    {ok, #state{conn_open_fun=fun OpenM:OpenF/0, conn_close_fun=fun CloseM:CloseF/1}, 0}.

handle_call(status, _From, #state{failed_count=FailedCount} = State) ->
    {reply, {ok, [{interval, refresh_interval()}, {failed_count, FailedCount}]}, State};
handle_call(weights, _From, #state{weights = Weights} = State) ->
    {reply, {ok, Weights}, State};
handle_call({input, Json}, _From, State) ->
    case input(Json, State) of
        {ok, NewWeights} ->
            {reply, {ok, NewWeights}, State#state{weights=NewWeights}};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call(refresh, _From, State) ->
    case fetch_weights(State) of
        {ok, WeightInfoList, NewState} ->
            {reply, {ok, WeightInfoList}, NewState};
        {error, Reason, NewState} ->
            {reply, {error, Reason}, NewState}
    end;
handle_call(Request, _From, State) ->
    {reply, {error, {unknown_request, Request}}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(Event, State) when Event =:= refresh_by_timer orelse Event =:= timeout ->
    case refresh_by_timer(State) of
        {ok, _WeightInfoList, NewState} ->
            {noreply, NewState};
        {error, Reason, NewState} ->
            lager:error("Refresh of cluster weight information failed. Reason: ~p", [Reason]),
            {noreply, NewState}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

refresh_by_timer(State) ->
    case fetch_weights(State) of
        {ok, WeightInfoList, State1} ->
            State2 = schedule(State1),
            {ok, WeightInfoList, State2};
        {error, Reason, State1} ->
            State2 = schedule(State1),
            {error, Reason, State2}
    end.

%% Connect to default cluster and GET weight information
fetch_weights(#state{conn_open_fun=OpenFun, conn_close_fun=CloseFun} = State) ->
    case OpenFun() of
        {ok, Riakc} ->
            Result = riakc_pb_socket:get(Riakc, ?WEIGHT_BUCKET, ?WEIGHT_KEY),
            CloseFun(Riakc),
            handle_weight_info_list(Result, State);
        {error, _Reason} = E ->
            handle_weight_info_list(E, State)
    end.

handle_weight_info_list({error, notfound}, State) ->
    lager:debug("Bag weight information is not found"),
    {ok, [], State#state{failed_count = 0}};
handle_weight_info_list({error, Reason}, #state{failed_count = Count} = State) ->
    lager:error("Retrieval of bag weight information failed. Reason: ~p", [Reason]),
    {error, Reason, State#state{failed_count = Count + 1}};
handle_weight_info_list({ok, Obj}, State) ->
    %% TODO: How to handle siblings
    [Value | _] = riakc_obj:get_values(Obj),
    Weights = binary_to_term(Value),
    riak_cs_multi_bag_server:new_weights(Weights),
    {ok, Weights, State#state{failed_count = 0, weights = Weights}}.

schedule(State) ->
    IntervalMSec = refresh_interval() * 1000,
    Ref = erlang:send_after(IntervalMSec, self(), refresh_by_timer),
    State#state{timer_ref = Ref}.

input(Json, State) ->
    case json_to_weight_info_list(Json) of
        {ok, WeightInfoList} ->
            update_weight_info(WeightInfoList, State);
        {error, Reason} ->
            {error, Reason}
    end.

json_to_weight_info_list({struct, JSON}) ->
    case json_to_weight_info_list(JSON, []) of
        {ok, WeightInfoList} -> verify_weight_info_list_input(WeightInfoList);
        {error, Reason} -> {error, Reason}
    end.

json_to_weight_info_list([], WeightInfoList) ->
    {ok, WeightInfoList};
json_to_weight_info_list([{TypeBin, Bags} | Rest], WeightInfoList) ->
    case TypeBin of
        <<"manifest">> ->
            json_to_weight_info_list(manifest, Bags, Rest, WeightInfoList);
        <<"block">> ->
            json_to_weight_info_list(block, Bags, Rest, WeightInfoList);
        _ ->
            {error, {bad_request, TypeBin}}
    end.

json_to_weight_info_list(Type, Bags, RestTypes, WeightInfoList) ->
    case json_to_weight_info_list_by_type(Type, Bags) of
        {ok, WeightInfoListPerType} ->
            json_to_weight_info_list(RestTypes, [WeightInfoListPerType | WeightInfoList]);
        {error, Reason} ->
            {error, Reason}
    end.

json_to_weight_info_list_by_type(Type, Bags) ->
    json_to_weight_info_list_by_type(Type, Bags, []).

json_to_weight_info_list_by_type(Type, [], WeightInfoList) ->
    {ok, {Type, WeightInfoList}};
json_to_weight_info_list_by_type(Type, [Bag | Rest], WeightInfoList) ->
    case json_to_weight_info(Bag) of
        {ok, WeightInfo} ->
            json_to_weight_info_list_by_type(Type, Rest, [WeightInfo | WeightInfoList]);
        {error, Reason} ->
            {error, Reason}
    end.

json_to_weight_info({struct, Bag}) ->
    json_to_weight_info(Bag, #weight_info{}).

json_to_weight_info([], #weight_info{bag_id=Id, weight=Weight} = WeightInfo)
  when Id =/= undefined andalso Weight =/= undefined ->
    {ok, WeightInfo};
json_to_weight_info([], WeightInfo) ->
    {error, {bad_request, WeightInfo}};
json_to_weight_info([{<<"id">>, Id} | Rest], WeightInfo)
  when is_binary(Id) ->
    json_to_weight_info(Rest, WeightInfo#weight_info{bag_id = Id});
json_to_weight_info([{<<"weight">>, Weight} | Rest], WeightInfo)
  when is_integer(Weight) andalso Weight >= 0 ->
    json_to_weight_info(Rest, WeightInfo#weight_info{weight = Weight});
json_to_weight_info([{<<"free">>, Free} | Rest], WeightInfo) ->
    json_to_weight_info(Rest, WeightInfo#weight_info{free = Free});
json_to_weight_info([{<<"total">>, Total} | Rest], WeightInfo) ->
    json_to_weight_info(Rest, WeightInfo#weight_info{total = Total});
json_to_weight_info(Json, _WeightInfo) ->
    {error, {bad_request, Json}}.

verify_weight_info_list_input(WeightInfoList) ->
    %% TODO implement verify logic or consistency checks
    {ok, WeightInfoList}.

%% Connect to Riak cluster and overwrite weights at {riak-cs-bag, weight}
update_weight_info(WeightInfoList, #state{conn_open_fun=OpenFun, conn_close_fun=CloseFun}) ->
    case OpenFun() of
        {ok, Riakc} ->
            try
                get_weight_info(Riakc, WeightInfoList)
            after
                CloseFun(Riakc)
            end;
        {error, _Reason} = E ->
            E
    end.

get_weight_info(Riakc, WeightInfoList) ->
    Current = case riakc_pb_socket:get(Riakc, ?WEIGHT_BUCKET, ?WEIGHT_KEY) of
                  {error, notfound} ->
                      {ok, riakc_obj:new(?WEIGHT_BUCKET, ?WEIGHT_KEY)};
                  {error, Reason} ->
                      {error, Reason};
                  {ok, Obj} ->
                      {ok, Obj}
              end,
    put_weight_info(Riakc, WeightInfoList, Current).

put_weight_info(_Riakc, _WeightInfoList, {error, Reason}) ->
    lager:error("Retrieval of bag weight information failed. Reason: ~p", [Reason]),
    {error, Reason};
put_weight_info(Riakc, WeightInfoList, {ok, Obj}) ->
    NewObj = riakc_obj:update_value(
               riakc_obj:update_metadata(Obj, dict:new()),
               term_to_binary(WeightInfoList)),
    case riakc_pb_socket:put(Riakc, NewObj) of
        ok ->
            riak_cs_multi_bag_server:new_weights(WeightInfoList),
            {ok, WeightInfoList};
        {error, Reason} ->
            lager:error("Update of bag weight information failed. Reason: ~p", [Reason]),
            {error, Reason}
    end.
