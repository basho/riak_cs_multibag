# riak_cs_multibag internal document

Please refer *README.md* first. This document complement it.

## Special treatment of zero weight for transition from single bag to multibag

While rolling upgrading Riak CS nodes, there are two kinds of mixture and
we should treat them well, without service shutdown.

1. Mixture of CS versions with/without multibag support
2. Mixture of configuration of connections to every bags

Therefore two constraints arise.

1. No use of `bag_id` until every CS node is upgraded.
2. No use of bags other than master until each CS nodes has appropriate
   configuration of connections.

To fulfill these constraints, take "zero weights" state as special one.
If the sum of weights is zero, `riak_cs_multibag_server:choose_bag/1`
returns `undefined` regardless of `bags` configuration.
The `undefined` value makes Riak CS use master bag to store objects.

## Shrinking bags

Sketch of shrinking bags is in the issue #1 as a comment[1].

[1] https://github.com/basho/riak_cs_multibag/issues/1#issuecomment-40177624

## Why not use cluster ID instead of introducing bag?

First, operator can determine bag ID and can make it human friendly.
Then they can use such IDs to set weights.

Future issue:
Cluster IDs are determined automatically and are not-so-human-friendly strings.
But it's great not to bother operators hands.
If we could assign and change all weights in completely automatic way,
cluster IDs (or auto-generated IDs) would be great.

Second is to avoid corner cases for operation.
If we set cluster IDs which store blocks into manifests, they will be
replicated to other clusters by MDC. So Riak CS should know the
*mappings* of cluster IDs related by MDC.

There would be complications and corner cases such as:
- Implement Riak (riak_ee) API to get mappings.
  There are subtle points, for example cascading topology.
- If new MDC connection is established after Riak CS connected to a Riak,
  the new cluster ID should be known by the existing Riak CS.
  subscribe/publish mechanism or manual refresh of mappings or such are needed.
- If operator stop MDC repl, what happens? The mappings should holds
  all ever-connected-cluster-at-least-once cluster IDs? (like
  `erlang:nodes(known)`)
