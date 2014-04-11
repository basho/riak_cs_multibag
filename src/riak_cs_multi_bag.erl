%% @doc Support multi Riak clusters in single Riak CS system

-module(riak_cs_multi_bag).

-export([process_specs/0, pool_specs/1, pool_name_for_bag/2, choose_bag_id/1]).
-export([list_pool/0, list_pool/1]).
-export([tab_name/0, tab_info/0]).

-export_type([pool_key/0, pool_type/0, bag_id/0, weight_info/0]).

-define(ETS_TAB, ?MODULE).
-record(pool, {key :: pool_key(),
               type :: pool_type(), % for match spec
               ip :: string(),
               port :: non_neg_integer(),
               name :: atom(),
               sizes :: {non_neg_integer(), non_neg_integer()}}).

-include("riak_cs_multi_bag.hrl").

%% These types are defined also in riak_cs, but for compilation
%% declare them again
-type bag_id() :: undefined | binary().
-type pool_type() :: request_pool | bucket_list_pool.

-type pool_key() :: {pool_type(), bag_id()}.
-type weight_info() :: #weight_info{}.

-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

maybe_init(MasterPoolConfigs, Bags) ->
    init_ets(),
    ok = store_pool_records(MasterPoolConfigs, Bags),
    ok.

process_specs() ->
    BagServer = {riak_cs_multi_bag_server,
                 {riak_cs_multi_bag_server, start_link, []},
                 permanent, 5000, worker, [riak_cs_multi_bag_server]},
    %% Pass connection open/close information not to "explicitly" depends on riak_cs
    %% and to make unit test easier.
    %% TODO: better to pass these MF's by argument process_specs
    WeightUpdaterArgs = [{conn_open_mf, {riak_cs_utils, riak_connection}},
                         {conn_close_mf, {riak_cs_utils, close_riak_connection}}],
    WeightUpdater = {riak_cs_multi_bag_weight_updater,
                     {riak_cs_multi_bag_weight_updater, start_link,
                      [WeightUpdaterArgs]},
                     permanent, 5000, worker, [riak_cs_bag_worker]},
    [BagServer, WeightUpdater].

%% Return pool specs from application configuration.
%% This function assumes that it is called ONLY ONCE at initialization.
-spec pool_specs(term()) -> [{atom(), {non_neg_integer(), non_neg_integer()}}].
pool_specs(MasterPoolConfig) ->
    {ok, Bags} = application:get_env(riak_cs_multi_bag, bags),
    maybe_init(MasterPoolConfig, Bags),
    [record_to_spec(P) || P <- ets:tab2list(?ETS_TAB)].
%% Translate bag ID to pool name.
%% 'undefined' in second argument means buckets and manifests were stored
%% under single bag configuration.
-spec pool_name_for_bag(pool_type(), bag_id()) -> atom().
pool_name_for_bag(Type, undefined) ->
    default_bag_id(Type);
pool_name_for_bag(Type, BagId) when is_binary(BagId) ->
    case ets:lookup(?ETS_TAB, {Type, BagId}) of
        [] ->
            %% TODO: Misconfiguration? Should throw error?
            %% Another possibility is number of bags are reduced.
            undefined;
        [#pool{name = Name}] ->
            Name
    end.

-spec default_bag_id(pool_type()) -> undefined | bag_id().
default_bag_id(Type) ->
    case application:get_env(riak_cs, default_bag) of
        undefined ->
            undefined;
        {ok, DefaultBags} ->
            case lists:keyfind(Type, 1, DefaultBags) of
                false ->
                    undefined;
                {Type, BagId} ->
                    list_to_binary(BagId)
            end
    end.

%% Choose bag ID for new bucket or new manifest
-spec choose_bag_id(manifet | block) -> bag_id().
choose_bag_id(Type) ->
    {ok, BagId} = riak_cs_multi_bag_server:choose_bag(Type),
    BagId.

init_ets() ->
    ets:new(?ETS_TAB, [{keypos, 2}, named_table, protected,
                       {read_concurrency, true}]).

store_pool_records(MasterPoolConfig, Bags) ->
    store_pool_records(MasterPoolConfig, Bags, MasterPoolConfig).

store_pool_records(_, [], _) ->
    ok;
store_pool_records(OriginalMasterConfigs, [_|RestBags], []) ->
    store_pool_records(OriginalMasterConfigs, RestBags, OriginalMasterConfigs);
store_pool_records(OriginalMasterConfigs, [Bag | _] = Bags, [MasterConfig | RestMasterConfigs]) ->
    store_pool_record(Bag, MasterConfig),
    store_pool_records(OriginalMasterConfigs, Bags, RestMasterConfigs).

store_pool_record({BagId, IP, Port}, {PoolType, Sizes}) ->
    Name = list_to_atom(lists:flatten(io_lib:format("~s_~s", [PoolType, BagId]))),
    true = ets:insert(?ETS_TAB, #pool{key = {PoolType, list_to_binary(BagId)},
                                      type = PoolType,
                                      ip = IP,
                                      port = Port,
                                      name = Name,
                                      sizes = Sizes}).

record_to_spec(#pool{ip=IP, port=Port, name=Name, sizes=Sizes}) ->
    {Name, Sizes, {IP, Port}}.

list_pool() ->
    [{Name, Type, BagId} ||
        #pool{key={Type, BagId}, name=Name} <- ets:tab2list(?ETS_TAB)].

list_pool(PoolType) ->
    [{Name, Type, BagId} ||
        #pool{key={Type, BagId}, name=Name} <- ets:tab2list(?ETS_TAB),
        Type =:= PoolType].

%% For Debugging

tab_name() ->
    ?ETS_TAB.

tab_info() ->
    ets:tab2list(?ETS_TAB).

-ifdef(TEST).
%% ===================================================================
%% EUnit tests
%% ===================================================================

-endif.

