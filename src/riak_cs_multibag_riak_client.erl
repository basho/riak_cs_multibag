%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.

-module(riak_cs_multibag_riak_client).

-behaviour(gen_server).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, format_status/2]).

-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").

-define(SERVER, ?MODULE).
-define(DEFAULT_BAG, undefined).

%% Bag ID `undefined' represents objects were stored in single bag
%% configuration, and we use the default bag for the objects.
%% (currently, "default bag" means "master bag".)
%% To avoid confusion between `undefined's in record attributes
%% and ones for "default bag", set fresh values to `uninitialized'
%% in this record.
-record(state, {
          master_pbc   = uninitialized :: uninitialized | pid(),
          manifest_pbc = uninitialized :: uninitialized | pid(),
          block_pbc    = uninitialized :: uninitialized | pid(),

          manifest_bag = uninitialized :: uninitialized | ?DEFAULT_BAG | binary(),
          block_bag    = uninitialized :: uninitialized | ?DEFAULT_BAG | binary(),

          bucket_name  = uninitialized :: uninitialized | binary(),
          bucket_obj   = uninitialized :: uninitialized | term(), % riakc_obj:riakc_obj()
          manifest     = uninitialized :: uninitialized | term()  % lfs_manifest()
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
handle_call({set_manifest_bag, ManifestBagId}, _From, State)
  when ManifestBagId =:= ?DEFAULT_BAG orelse is_binary(ManifestBagId) ->
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
    Reply = {error, {invalid_request, Request}},
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
stop_pbcs([{uninitialized, _BagId} | Rest]) ->
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
ensure_manifest_pbc(#state{manifest_bag = ?DEFAULT_BAG, master_pbc=MasterPbc} = State) ->
    {ok, State#state{manifest_pbc=MasterPbc}};
ensure_manifest_pbc(#state{manifest_bag = BagId} = State)
  when is_binary(BagId) ->
    case riak_cs_utils:riak_connection(pool_name(BagId)) of
        {ok, Pbc} ->
            {ok, State#state{manifest_pbc = Pbc}};
        {error, Reason} ->
            {error, Reason}
    end;
ensure_manifest_pbc(#state{bucket_obj = BucketObj} = State)
  when BucketObj =/= uninitialized ->
    ManifestBagId = riak_cs_multibag:bag_id_from_bucket(BucketObj),
    ensure_manifest_pbc(State#state{manifest_bag = ManifestBagId}).

ensure_block_pbc(#state{block_pbc = BlockPbc} = State)
  when is_pid(BlockPbc) ->
    {ok, State};
ensure_block_pbc(#state{block_bag = ?DEFAULT_BAG, master_pbc=MasterPbc} = State) ->
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
  when Manifest =/= uninitialized ->
    BlockBagId = riak_cs_mb_helper:bag_id_from_manifest(Manifest),
    ensure_block_pbc(State#state{block_bag=BlockBagId}).

pool_name(BagId) ->
    riak_cs_riak_client:pbc_pool_name(BagId).
