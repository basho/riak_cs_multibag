# riak_cs_multibag

## Transition from single bag to multibag

While rolling upgrading Riak CS nodes, there are two kinds of mixture and
we should treat them well, without service shutdown.

1. Mixture of CS versions with/without multibag support
2. Mixture of configuration of connections to every bags

Therefore two contstraints arise.

1. No use of `bag_id` until every CS node is upgraded.
2. No use of bags other than master until each CS nodes has appropriate
   configuraion of connections.

To fullfil these constraings, take "zero weights" state as special one.
If the sum of weights is zero, `riak_cs_multibag_server:choose_bag/1`
returns `undefined` regardless of `bags` configuraion.
The `undefined` value makes Riak CS use master bag to store objects.

Steps to transit from single bag to multibag is as follows.
(Time flows from top to bottom)

```
| CS1            | CS2      | weights   | master   | bag-B          | bag-C          |
|----------------+----------+-----------+----------+----------------+----------------|
| normal         | normal   | N/A       | running  | unused         | unused         |
|                |          |           |          | start          | start          |
| stop           |          |           |          |                |                |
| upgrade        |          |           |          |                |                |
| add bags       |          |           |          |                |                |
| start          |          |           |          |                |                |
|                |          | set zeros |          | NOT YET chosen | NOT YET chosen |
| !KEEP OFFLINE! |          |           |          |                |                |
| !UNTIL HERE!   |          |           |          |                |                |
|                |          |           |          |                |                |
| can be online  |          |           |          |                |                |
|                | stop     |           |          |                |                |
|                | upgrade  |           |          |                |                |
|                | add bags |           |          |                |                |
|                | start    |           |          |                |                |
|                |          | set any   | chosen   | chosen         | chosen         |
```

TODO: include upgrade and restart of stanchion
