# riak_cs_multibag

## Configuration for multibug

The master bag connection information is specified by `riak_ip` and
`riak_port` in `riak_cs` app section as single bag configuration.

The connection information for additional bags are configured by
`bags` in `riak_cs_multibag` app section.

Weight information (see below) is stored in Riak (of the master bag)
and shared by all Riak CS nodes. Riak CS refreshes it in every
`weight_refresh_interval` seconds.
If you set weights and need immediate refresh, use a `refresh` command
(also see below).

```
 {riak_cs, [
              %% [snip]
              %% Riak node to which Riak CS accesses
              {riak_ip, "127.0.0.1"},
              {riak_pb_port, 10017 } ,
              %% [snip]
           ]},

 {riak_cs_multibag, [
              %% Connection pools for multiple bags
              {bags,
               [
                {"bag-A", "127.0.0.1", 10017},
                {"bag-B", "127.0.0.1", 10027},
                {"bag-C", "127.0.0.1", 10037}
               ]},

              %% How often weight infromation are fetched from Riak.
              %% Unit is in seconds, 900 means 15 minutes.
              {weight_refresh_interval, 900}
              ]},
```

`riak-cs-multibag` command shows the list of bags

Usage:
```
riak-cs-multibag list-bags
```

Example:

```
$ riak-cs-multibag list-bags
bag-B 127.0.0.1:10027
bag-A 127.0.0.1:10017
bag-C 127.0.0.1:10037
```

## Set, show, refresh weight information

By using `riak-cs-multibag` script, one can set, list, refresh weight information.
The weight information is stored in Riak (of the master bag) and shared between
all Riak CS nodes.

Usage for listing/showing weights:
```
riak-cs-multibag weight
riak-cs-multibag weight <bag id>
riak-cs-multibag weight-manifest
riak-cs-multibag weight-manifest <bag id>
riak-cs-multibag weight-block
riak-cs-multibag weight-block <bag id>
riak-cs-multibag refresh
```

Usage for setting weights:
```
riak-cs-multibag weight <bag id> <weight>
riak-cs-multibag weight-manifest <bag id> <weight>
riak-cs-multibag weight-block <bag id> <weight>
```

Usage for refreshing weights:
```
riak-cs-multibag refresh
```


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
