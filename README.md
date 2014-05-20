# Configuration and Operation for Riak CS's Multibag Functionality

One can setup Riak CS system to to store manifests and blocks in
multiple Riak clusters separately. With this functionality, Riak CS
achieves scalability for total storage size.

## Bag and bag ID

Multibag support introduces new terminology "bag".

A bag is a set of Riak Clusters those are interrelated by MDC replication.
- Without MDC replication, one bag is consists of one cluster.
- With MDC replication, one bag can include several clusters.

```

         +------------------- bag-A ---------------------+
         |                                               |
         |   +-----------+              +-----------+    |
         |   |           |              |           |    |
         |   |  Cluster  |--------------|  Cluster  |    |
         |   |           |              |           |    |
         |   +-----------+              +-----------+    |
         |                                               |
         +-----------------------------------------------+

         +------------------- bag-B ---------------------+
         |                                               |
         |   +-----------+              +-----------+    |
         |   |           |              |           |    |
         |   |  Cluster  |--------------|  Cluster  |    |
         |   |           |              |           |    |
         |   +-----------+              +-----------+    |
         |                                               |
         +-----------------------------------------------+

```

Bag ID is stored in bucket objects and manifest records.
The bag ID in a bucket objects specifies the bag which holds all manifests
in the bucket. Similarly, the bag ID in a manifest tells the bag which holds
all the blocks for the manifest.

The logic to choose bag ID for new buckets or new manifests are (somewhat)
randomized one with weights.
Weights are specified by operators for each bag.

## Objects and Bags

There is one special bag, the master bag.
The master bag is specified by `riak_ip` and `riak_port` in `riak_cs`
app section.

The master bag stores following objects
- users
- buckets
- results of storage calculation
- results of access statistics

Manifests and blocks are distributed to each bags with following
conditions.

1. Manifests which belongs to a single bucket are stored in
   the same bag. The bag ID for it is recorded in the bucket object.
2. Blocks of a single manifests (not key, but manifest) are
   stored in the same bag. The bag ID for it is recorded in the
   manifest.

Each bag also has its own GC bucket. In deleting objects, manifests
will go into the GC bucket of the same bag as they belong.

## Configuration for multibag

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

              %% How often weight information are fetched from Riak.
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

Note: In order to use master bag to store manifests or blocks, add its
connection information also in the `bags` list as well as `riak_ip` and
`riak_port`.

## Configuration for Stanchion

Stanchion should be configured to include connection pool information.
When you switch multibag from single bag systemor or add bags,
Stanchion should have the new configuration **first**, then riak_cs nodes
have similar changes.

Add `bags` like above riak_cs configuration to Stanchion's `app.config`.

```
 {stanchion, [
              ...
              %% Connection pools for multiple bags
              {bags,
               [
                {"bag-A", "127.0.0.1", 10017},
                {"bag-B", "127.0.0.1", 10027},
                {"bag-C", "127.0.0.1", 10037}
               ]},
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

First, upgrade, confugure and restart stanchion to the version with
multibag support beforehand.
Then follow the steps below to transit from single bag to multibag.

(Time flows from top to bottom)

```
| CS 1           | CS 2     | weights   | master  | bag-B        | bag-C        |
|----------------+----------+-----------+---------+--------------+--------------|
| normal         | normal   | N/A       | running | unused       | unused       |
|                |          |           | used    | start        | start        |
| stop           |          |           |         |              |              |
| upgrade        |          |           |         |              |              |
| add bags       |          |           |         |              |              |
| start          |          |           |         |              |              |
|                |          | set zeros |         | NOT YET used | NOT YET used |
| !KEEP OFFLINE! |          |           |         |              |              |
| !UNTIL HERE!   |          |           |         |              |              |
|                |          |           |         |              |              |
| can be online  |          |           |         |              |              |
|                | stop     |           |         |              |              |
|                | upgrade  |           |         |              |              |
|                | add bags |           |         |              |              |
|                | start    |           |         |              |              |
|                |          |           |         |              |              |
|                |          | set any   | used    | used         | used         |
```

## Adding bags

Adding more bags to already multibag-enabled system is rather straightforward.
First set up new Riak clusters for bags and add their connection information
to Riak CS's `bags` configuration.
Finally, set weights of new bags by command. That's all.

```
| CS 1     | CS 2     | weights      | master  | existing bags | new bags |
|----------+----------+--------------+---------+---------------+----------|
| running  | running  |              | running | running       | N/A      |
|          |          |              | used    | used          |          |
|          |          |              |         |               | start    |
|          |          |              |         |               | setup    |
| stop     |          |              |         |               |          |
| upgrade  |          |              |         |               |          |
| add bags |          |              |         |               |          |
| start    |          |              |         |               |          |
|          | stop     |              |         |               |          |
|          | upgrade  |              |         |               |          |
|          | add bags |              |         |               |          |
|          | start    |              |         |               |          |
|          |          |              |         |               |          |
|          |          | set new bags |         | used          | used     |
```
