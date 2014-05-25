-module(riak_cs_multibag_riak_client).

-behaviour(gen_server).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, format_status/2]).

-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").

-define(SERVER, ?MODULE).

-record(state, {
          master_pbc,
          manifest_pbc,
          block_pbc,

          manifest_bag,
          block_bag,

          bucket_name,
          bucket_obj,
          manifest,

          use_pool = true %% TODO Use riakc_pb_socket directly?
         }).

init([]) ->
    {ok, fresh_state()}.

handle_call(stop, _From, State) ->
    _ = do_cleanup(State),
    {stop, normal, ok, State};
handle_call(cleanup, _From, State) ->
    {reply, ok, do_cleanup(State)};
handle_call({get_bucket, BucketName}, _From, State) ->
    case do_get_bucket(State#state{bucket_name=BucketName}) of
        {ok, #state{bucket_obj=BucketObj} = NewState} ->
            {reply, {ok, BucketObj}, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call({set_bucket_name, BucketName}, _From, State) ->
    case do_get_bucket(State#state{bucket_name=BucketName}) of
        {ok, NewState} ->
            {reply, ok, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call({get_user, UserKey}, _From, State) ->
    case ensure_master_pbc(State) of
        {ok, #state{master_pbc=MasterPbc} = NewState} ->
            Res = riak_cs_riak_client:get_user_with_pbc(MasterPbc, UserKey),
            {reply, Res, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call({save_user, User, OldUserObj}, _From, State) ->
    case ensure_master_pbc(State) of
        {ok, #state{master_pbc=MasterPbc} = NewState} ->
            Res = riak_cs_riak_client:save_user_with_pbc(MasterPbc, User, OldUserObj),
            {reply, Res, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call(master_pbc, _From, State) ->
    case ensure_master_pbc(State) of
        {ok, #state{master_pbc=MasterPbc} = NewState} ->
            {reply, {ok, MasterPbc}, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call(manifest_pbc, _From, State) ->
    case ensure_manifest_pbc(State) of
        {ok, #state{manifest_pbc=ManifestPbc} = NewState} ->
            {reply, {ok, ManifestPbc}, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call({set_manifest_bag, ManifestBagId}, _From, State) ->
    %% TODO: When ManifestBagId is undefined, the "undefined" indicates "master".
    %% `undefined' for initial value (which means uninitialized, not default)
    %% is wrong, double meaning, confusing.
    case ensure_manifest_pbc(State#state{manifest_bag=ManifestBagId}) of
        {ok, NewState} ->
            {reply, ok, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call({set_manifest, Manifest}, _From, State) ->
    case ensure_block_pbc(State#state{manifest=Manifest}) of
        {ok, NewState} ->
            {reply, ok, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call(block_pbc, _From, State) ->
    case ensure_block_pbc(State) of
        {ok, #state{block_pbc=BlockPbc} = NewState} ->
            {reply, {ok, BlockPbc}, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call(Request, _From, State) ->
    Reply = {error, {'NOT_IMPLEMENTED_YET, THANK YOU!!!', Request}},
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, [_PDict, Status]) ->
    Fields = record_info(fields, state),
    [_Name | Values] = tuple_to_list(Status),
    lists:zip(Fields, Values).

%%% Internal functions

fresh_state() ->
    #state{}.

do_cleanup(State) ->
    stop_pbcs([{State#state.master_pbc, <<"master">>},
               {State#state.manifest_pbc, State#state.manifest_bag},
               {State#state.block_pbc, State#state.block_bag}]),
    fresh_state().

stop_pbcs([]) ->
    ok;
stop_pbcs([{undefined, _BagId} | Rest]) ->
    stop_pbcs(Rest);
stop_pbcs([{Pbc, BagId} | Rest]) when is_pid(Pbc) ->
    riak_cs_utils:close_riak_connection(pool_name(BagId), Pbc),
    stop_pbcs(Rest).

do_get_bucket(State) ->
    case ensure_master_pbc(State) of
        {ok, #state{master_pbc=MasterPbc, bucket_name=BucketName} = NewState} ->
            case riak_cs_riak_client:get_bucket_with_pbc(MasterPbc, BucketName) of
                {ok, Obj} ->
                    {ok, NewState#state{bucket_obj=Obj}};
                {error, _}=Error ->
                    Error
            end;
        {error, Reason} ->
            {error, Reason}
    end.

ensure_master_pbc(#state{master_pbc = MasterPbc} = State)
  when is_pid(MasterPbc) ->
    {ok, State};
ensure_master_pbc(#state{} = State) ->
    case riak_cs_utils:riak_connection(pool_name(master)) of
        {ok, MasterPbc} -> {ok, State#state{master_pbc=MasterPbc}};
        {error, Reason} -> {error, Reason}
    end.

ensure_manifest_pbc(#state{manifest_pbc = ManifestPbc} = State)
  when is_pid(ManifestPbc) ->
    {ok, State};
ensure_manifest_pbc(#state{manifest_bag = default, master_pbc=MasterPbc} = State) ->
    {ok, State#state{manifest_pbc=MasterPbc}};
ensure_manifest_pbc(#state{manifest_bag = master} = State) ->
    ensure_manifest_pbc(State#state{manifest_bag = <<"master">>});
ensure_manifest_pbc(#state{manifest_bag = BagId} = State)
  when is_binary(BagId) ->
    case riak_cs_utils:riak_connection(pool_name(BagId)) of
        {ok, Pbc} ->
            {ok, State#state{manifest_pbc = Pbc}};
        {error, Reason} ->
            {error, Reason}
    end;
ensure_manifest_pbc(#state{bucket_obj = BucketObj} = State) ->
    ManifestBagId = riak_cs_multibag_registrar:bag_id_from_bucket(BucketObj),
    case ManifestBagId of
        undefined ->
            ensure_manifest_pbc(State#state{manifest_bag = default});
        Bin when is_binary(Bin) ->
            ensure_manifest_pbc(State#state{manifest_bag = ManifestBagId})
    end.

ensure_block_pbc(#state{block_pbc = BlockPbc} = State)
  when is_pid(BlockPbc) ->
    {ok, State};
ensure_block_pbc(#state{block_bag = default, master_pbc=MasterPbc} = State) ->
    {ok, State#state{block_pbc=MasterPbc}};
ensure_block_pbc(#state{block_bag = BagId} = State)
  when is_binary(BagId) ->
    case riak_cs_utils:riak_connection(pool_name(BagId)) of
        {ok, Pbc} ->
            {ok, State#state{block_pbc = Pbc}};
        {error, Reason} ->
            {error, Reason}
    end;
ensure_block_pbc(#state{manifest=Manifest} = State)
  when Manifest =/= undefined ->
    BlockBagId = riak_cs_multibag_registrar:bag_id_from_manifest(Manifest),
    case BlockBagId of
        undefined ->
            ensure_block_pbc(State#state{block_bag=default});
        Bin when is_binary(Bin) ->
            ensure_block_pbc(State#state{block_bag=BlockBagId})
    end.

pool_name(master) ->
    request_pool_master;
pool_name(BagId) ->
    list_to_atom(lists:flatten(io_lib:format("request_pool_~s", [BagId]))).
