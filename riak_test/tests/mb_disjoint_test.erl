%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.

-module(mb_disjoint_test).

%% @doc `riak_test' module for testing multi bag disjoint configuration

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-include("riak_cs.hrl").

-define(TEST_BUCKET_CREATE_DELETE, "riak-test-bucket-create-delete").

-define(TEST_BUCKET,   "riak-test-bucket").
-define(KEY_NORMAL,    "key_normal").
-define(KEY_MULTIPART, "key_multipart").

confirm() ->
    {UserConfig, {RiakNodes, _CSNodes, _Stanchion}} =
        rtcs:setupNxMsingles(1, 4, rtcs_bag:configs(rtcs_bag:bags(disjoint)),
                             current),
    rtcs_bag:set_weights(disjoint),

    lager:info("User is valid on the cluster, and has no buckets"),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(UserConfig)),

    assert_bucket_create_delete_twice(UserConfig),

    lager:info("creating bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, UserConfig)),

    ?assertMatch([{buckets, [[{name, ?TEST_BUCKET}, _]]}],
                 erlcloud_s3:list_buckets(UserConfig)),

    assert_object_in_expected_bag(RiakNodes, UserConfig, normal,
                                  ?TEST_BUCKET, ?KEY_NORMAL),
    assert_object_in_expected_bag(RiakNodes, UserConfig, multipart,
                                  ?TEST_BUCKET, ?KEY_MULTIPART),

    pass.

assert_bucket_create_delete_twice(UserConfig) ->
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET_CREATE_DELETE, UserConfig)),
    ?assertEqual(ok, erlcloud_s3:delete_bucket(?TEST_BUCKET_CREATE_DELETE, UserConfig)),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET_CREATE_DELETE, UserConfig)),
    ?assertEqual(ok, erlcloud_s3:delete_bucket(?TEST_BUCKET_CREATE_DELETE, UserConfig)),
    ok.

assert_object_in_expected_bag(RiakNodes, UserConfig, UploadType, B, K) ->
    {Bucket, Key, Content} = rtcs_object:upload(UserConfig, UploadType, B, K),
    rtcs_object:assert_whole_content(UserConfig, Bucket, Key, Content),
    [_BagA, _BagB, BagC, BagD, BagE] = RiakNodes,

    %% riak-test-bucket goes to BagC, definitely
    ManifestBag = BagC,
    {_UUID, M} = rtcs_bag:assert_manifest_in_single_bag(
                   Bucket, Key, RiakNodes, ManifestBag),

    BlockBag = case rtcs_bag:high_low({Bucket, Key, M}) of
                   low  -> BagD;
                   high -> BagE
               end,
    ok = rtcs_bag:assert_block_in_single_bag(Bucket, M, RiakNodes, BlockBag),
    ok.
