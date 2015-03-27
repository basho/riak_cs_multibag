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

    assert_object_in_expected_bag(RiakNodes, UserConfig, normal),
    assert_object_in_expected_bag(RiakNodes, UserConfig, multipart),

    pass.

assert_bucket_create_delete_twice(UserConfig) ->
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET_CREATE_DELETE, UserConfig)),
    ?assertEqual(ok, erlcloud_s3:delete_bucket(?TEST_BUCKET_CREATE_DELETE, UserConfig)),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET_CREATE_DELETE, UserConfig)),
    ?assertEqual(ok, erlcloud_s3:delete_bucket(?TEST_BUCKET_CREATE_DELETE, UserConfig)),
    ok.

assert_object_in_expected_bag(RiakNodes, UserConfig, UploadType) ->
    {Bucket, Key, Content} = upload(UserConfig, UploadType),
    assert_whole_content(Bucket, Key, Content, UserConfig),
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

upload(UserConfig, normal) ->
    Content = crypto:rand_bytes(mb(4)),
    erlcloud_s3:put_object(?TEST_BUCKET, ?KEY_NORMAL, Content, UserConfig),
    {?TEST_BUCKET, ?KEY_NORMAL, Content};
upload(UserConfig, multipart) ->
    Content = rtcs_multipart:multipart_upload(?TEST_BUCKET, ?KEY_MULTIPART,
                                              %% [mb(10), mb(5), mb(9) + 123, mb(6), 400],
                                              [mb(10), 400],
                                              UserConfig),
    {?TEST_BUCKET, ?KEY_MULTIPART, Content}.

mb(MegaBytes) ->
    MegaBytes * 1024 * 1024.

assert_whole_content(Bucket, Key, ExpectedContent, Config) ->
    Obj = erlcloud_s3:get_object(Bucket, Key, Config),
    assert_whole_content(ExpectedContent, Obj).

assert_whole_content(ExpectedContent, ResultObj) ->
    Content = proplists:get_value(content, ResultObj),
    ContentLength = proplists:get_value(content_length, ResultObj),
    ?assertEqual(byte_size(ExpectedContent), list_to_integer(ContentLength)),
    ?assertEqual(byte_size(ExpectedContent), byte_size(Content)),
    ?assertEqual(ExpectedContent, Content).
