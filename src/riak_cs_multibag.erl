%% @doc Support multi Riak clusters in single Riak CS system

-module(riak_cs_multibag).

-export([process_specs/0, pool_specs/1, pool_name_for_bag/2, choose_bag_id/1]).
-export([list_pool/0, list_pool/1]).
-export([tab_name/0, tab_info/0]).

-export_type([pool_key/0, pool_type/0, bag_id/0, weight_info/0]).

-define(ETS_TAB, ?MODULE).
-record(pool, {key :: pool_key(),
               address :: string(),
               port :: non_neg_integer(),
               name :: atom(),
               sizes :: {non_neg_integer(), non_neg_integer()}}).

-include("riak_cs_multibag.hrl").

-type bag_id() :: undefined | binary().
-type pool_type() :: request_pool | bucket_list_pool.
-type pool_key() :: {pool_type(), bag_id()}.
-type weight_info() :: #weight_info{}.

maybe_init(MasterPoolConfigs, Bags) ->
    _Tid = init_ets(),
    ok = store_pool_records(MasterPoolConfigs, Bags),
    ok.

process_specs() ->
    BagServer = {riak_cs_multibag_server,
                 {riak_cs_multibag_server, start_link, []},
                 permanent, 5000, worker, [riak_cs_multibag_server]},
    %% Pass connection open/close information not to "explicitly" depends on riak_cs
    %% and to make unit test easier.
    %% TODO: Pass these MF's by argument process_specs
    WeightUpdaterArgs = [{conn_open_mf, {riak_cs_utils, riak_connection}},
                         {conn_close_mf, {riak_cs_utils, close_riak_connection}}],
    WeightUpdater = {riak_cs_multibag_weight_updater,
                     {riak_cs_multibag_weight_updater, start_link,
                      [WeightUpdaterArgs]},
                     permanent, 5000, worker, [riak_cs_multibag_weight_updater]},
    [BagServer, WeightUpdater].

%% Return pool specs from application configuration.
%% This function assumes that it is called ONLY ONCE at initialization.
-spec pool_specs(term()) -> [{atom(), {non_neg_integer(), non_neg_integer()}}].
pool_specs(MasterPoolConfig) ->
    {ok, Bags} = application:get_env(riak_cs_multibag, bags),
    maybe_init(MasterPoolConfig, Bags),
    [record_to_spec(P) || P <- ets:tab2list(?ETS_TAB)].

record_to_spec(#pool{address=Address, port=Port, name=Name, sizes=Sizes}) ->
    {Name, Sizes, {Address, Port}}.

%% Translate bag ID to pool name.
%% 'undefined' in second argument means buckets and manifests were stored
%% under single bag configuration, i.e. to the master bag.
-spec pool_name_for_bag(pool_type(), bag_id()) -> {ok, atom()} | {error, term()}.
pool_name_for_bag(_PoolType, undefined) ->
    {ok, undefined};
pool_name_for_bag(PoolType, BagId) when is_binary(BagId) ->
    case ets:lookup(?ETS_TAB, {PoolType, BagId}) of
        [] ->
            %% Misconfiguration
            {error, {no_pool, PoolType, BagId}};
        [#pool{name = Name}] ->
            {ok, Name}
    end.

%% Choose bag ID for new bucket or new manifest
-spec choose_bag_id(manifet | block) -> bag_id().
choose_bag_id(AllocType) ->
    {ok, BagId} = riak_cs_multibag_server:choose_bag(AllocType),
    BagId.

init_ets() ->
    ets:new(?ETS_TAB, [{keypos, #pool.key}, named_table, protected,
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

store_pool_record({BagId, Address, Port}, {PoolType, Sizes}) ->
    Name = list_to_atom(lists:flatten(io_lib:format("~s_~s", [PoolType, BagId]))),
    true = ets:insert(?ETS_TAB, #pool{key = {PoolType, list_to_binary(BagId)},
                                      address = Address,
                                      port = Port,
                                      name = Name,
                                      sizes = Sizes}).

list_pool() ->
    [{Name, PoolType, BagId, [{address, Address}, {port, Port}]} ||
        #pool{key={PoolType, BagId}, name=Name, address=Address, port=Port} <-
            ets:tab2list(?ETS_TAB)].

list_pool(TargetPoolType) ->
    [{Name, PoolType, BagId, [{address, Address}, {port, Port}]} ||
        #pool{key={PoolType, BagId}, name=Name, address=Address, port=Port} <-
            ets:tab2list(?ETS_TAB),
        PoolType =:= TargetPoolType].

%% For Debugging

tab_name() ->
    ?ETS_TAB.

tab_info() ->
    ets:tab2list(?ETS_TAB).
