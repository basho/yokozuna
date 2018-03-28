
# Introduction

In Yokozuna versions prior to 2.0.4 (e.g., Riak 2.0.7), update operations on Solr (notably, `add` and `delete` operations) are synchronous blocking operations and are performed once-at-a-time in Solr.  In particular, calls to the `yz_kv:index/3` Erlang function block until the associated data is written to Solr, and each such call results in an HTTP POST with a single Solr operation.

Yokozuna version 2.0.4 introduces batching and asynchronous delivery of Solr operations.  The primary objective of this work is to decouple update operations in Solr from the Riak vnodes that are responsible for managment of replicas of Riak objects across the cluster.  Without batching and asynchronous delivery, Riak vnodes have to wait for Solr operations to complete, sometimes an inordinate amount of time, which can have an impact on read and write operations in a Riak cluster, even for Riak objects that aren't being indexed through Yokozuna!  This feature is intended to free up Riak vnodes to do other work, thus allowing Riak vnodes to service requests from more clients concurrently, and increasing operational throughput throughout the cluster.

De-coupling indexing operations from Riak vnode activity, however introduces some complexity into the system, as there is now the possibility for indeterminate latency between the time that an object is available in Riak versus its availability in Yokozuna and Solr.  This latency can introduce  divergence between the Riak AAE trees stored in Riak/KV and the AAE trees stored in Yokozuna, with which the Riak/KV trees are compared.

This document describes the batching and asynchronous delivery subsystem introduced in Yokozuna 2.0.4, from both the end-users' and implementors' point of view, as well as the methods by which divergence between Riak and Solr is mitigated.


# Overview

The Yokozuna batching subsystem introduces two sets of additional `gen_server` processes into each Riak node, a set of "workers", and a set of "helpers".  The job of the worker processes is to enqueue updates from components inside of Riak -- typically Riak vnodes, but also sometimes parts of the Yokozuna AAE subsystem -- and to make enqueued objects available to helper processes, which, in turn, are responsible for dispatching batches of operations to Solr.

For each vnode and Solr index on a Riak node, there is a pair of helper and worker processes which are responsible for enqueing and dispatching batches of operations into Solr.  For example, on a Riak node with 12 vnodes and 5 indices, there will be 60 such pairs.  Each pair of helper and worker processes has an associated supervisor, which oversees the lifecycle of each pair of `gen_server` processes.  Each such supervisor is itself supervised by a supervisor for the entire batching subsytem, which in turn is supervised by the supervision hierarchy for the Yokozuna application.

When enqueuing an object, a worker and helper process pair is selected based on the Solr index (core) with which the operation is associated, along with the Riak partition on which the Riak object is stored.  The associated helper process will periodically dequeued batches of objects from a worker process, translate the objects into a set of Solr "operations", and dispatch those operations in batches of HTTP requests to the Solr server running on the same node.

The following diagram illustrates the relationship between Riak vnodes, Yokozuna workers and helpers, and Apache Solr:

![YZ Batching Overview](https://raw.githubusercontent.com/basho/yokozuna/docs/yz-batching-overview.png)

Each helper process is stateless; a helper's only role is to dequeue batches from the workers, to transform those batches into something that Solr can understand, and to dispatch the transformed batches to Solr (and report the results back to the worker process, on whose behalf it is doing the work).

The worker processes, on the other hand, maintain the queues of objects that are ready for update in Solr, in addition to managing the run-time behavior between the batching subsystem and the collection of vnodes and other processes that are communicating with the workers.

## Batching Parameters

When an update message is sent to a worker process, it is immediately enqueued in the worker associated with the index and Riak partition for which the operation is destined.

The Yokozuna batching subsystem provides configuration parameters that drive batching benavior, including configuration of:

* minimum batch size (default: 1);
* maximum batch size (default: 100);
* flush interval (default: 1 second)

When an update message is enqueued, and if the total number of enqueued messages (including the new message) is at least the specified minimum batch size, then the helper process associated with that index and worker is notified that a batch is ready for processing.  The helper then requests a batch from the worker, which delivers a batch back to the helper that is no larger than the maximum batch size.  The helper then translates the batch into a format suitable for Solr, and dispatches the batch to Solr via a single HTTP POST.

If when enqueing an update operation the number of batched messages is smaller than the configured minimum batch size, then a timer is set based on the configured flush interval, which will guarantee that any enqueued batches smaller than the configured minimum will be flushed to Solr within the specified interval.

## Backpressure

Each worker process is configured with a high water mark (1000, by default), which represents the total number of messages that may be enqueued in a given worker process before calls into the batching subsystem (update/index) will block calling vnodes.  If the total number of enqueued messages exceeds this threshold, calling vnodes (and parts of the AAE subsystem) will block until data is successfully written to Solr, or it is purged, in a manner described below.

This way, the batching subsystem exerts back-pressure on the vnode and AAE systems, in the case where Solr is being driven beyond its operational capacity.

## Failure Scenarios, Error Thresholds, and Retry

In most desirable scenarios, batches that are dequeued from worker processes are immediately translated and dispatched to Solr with no error.

In the undesirable cases where Solr becomes unresponsive (e.g., data corruption, excessively long garabage collection), Yokozuna has a mechanism for detecting transient failure in the HTTP POST requests, and for tracking how frequently failures occur for a given Solr core.  If too many errors occur within a given time window, then the Yokozuna batching system will re-enqueue failed messages for subsequent delivery, once the Solr core again becomes responsive.  This error threshold prevents Yokozuna from unecessarily adding load to Solr, if for some reason a particular search index has become unresponsive.

## Purge Strategies

If a Solr core has become unresponsive and the specified error threshold has been traversed, and if, in addition, the high water mark has been exceeded for a particular `yz_solrq_worker` process, then the yokozuna batching system has a mechanism for automatically purging enqueued entries, so as to allow vnodes to continue servicing requests, as well as to allow update operations to occur for indices that are not in a pathological state.

The yokozuna batching subsystem supports 3 different purge strategies:

* `purge_one` (default behavior): Purge the oldest entry from the `yz_solrq_worker`;
* `purge_index`: Purge all entries from the `yz_solrq_worker`;
* `off`: Perform no purges.  This has the effect of blocking vnodes indefinitely, and is not recommended for production servers.

Note that purging enqueued messages should be considered safe, as long as Active Anti-Entropy (AAE) is enabled, as the AAE subsystem will eventually detect missing entries in Solr, and will correct for the difference.

## Anti-Entropy

Yokozuna supports Active Anti-Entropy by maintaining a set of AAE trees in parallel with the Riak/KV AAE trees stored on the local node.  Periodically, comparable AAE trees are exchanged locally between the Riak/KV AAE subsystem and the Yokozuna AAE subsystem.  If any differences are detected, then the data in Solr is corrected.  Specifically, if any data is detected in the YZ AAE tree that is not in the Riak/KV AAE tree, a Solr delete operation is generated.  Conversely, if data is missing in a YZ AAE tree, or the hashes are not the same, then the entry is Solr is updated.  In this sense, the data in Riak/KV is "canonical".

In both Riak/KV and Yokozuna, the AAE trees are representations of data that is stored in Riak/KV and Solr, respectively.  They are not part of the Riak/KV or Solr data, but are instead auxiliary data structures (and persitent storage) alongside the actual data.  In the case of Yokozuna, the YZ AAE trees are, with respect to the VNode, asynchronosly updated in the helper process, which may occur many milliseconds after the VNode has sent the message to the batching subsystem.

Because the Yokozuna batching subsystem introduces latency between the time that data is written to the Riak/KV AAE trees and the Yokozuna AAE trees, there is an increased likelihood that the AAE trees will diverge.

Yokozuna manages this divergence by attempting to ensure that the Riak/KV and YZ AAE trees are as close as possible to being in sync.  It minimizes differences between hash trees during a given hash tree exchange by first taking a snapshot of the Riak/KV hashtree, and then draining all enqueued messages for a given Riak exchange partition to Solr.  Once drained, the YZ AAE trees are up to date, and the YZ AAE tree can then be compared with the snapshot of the Riak/KV AAE tree.

There is a small window between the time that the Riak/KV snapshot is taken and the drain of enqueued data is initiated in which data can be written to the YZ AAE hash tree but is missed in the Riak/KV snapshot, but in experiments these occurrences are minimal and are corrected in the next round of exchanges.

### Non-indexing operations

Recall that a Riak object is indexed in Solr if it is in a bucket which is associated with a search index, and that, in particular, it is entirely possible for a Riak cluster to contain some buckets that are associated with search indices, and others that are not.

In order for Riak/KV and YZ AAE trees to be comparable, they must represent the same replica sets, where a replica set is determined by its initial Riak partition and replication factor (`n_val`).  Because AAE trees are ignorant of buckets -- they are based entirely on the ring topology and replication factor, YZ AAE trees need to contain entries not only for Riak objects that are indexed in Solr, but also Riak object that are not.  If YZ AAE trees did not contain hashes for entries that are not indexed in Solr, the comparison with Riak/KV AAE trees would always show data missing in Solr, and thus repairs would always be attempted.

# Commands, Configuration, and Statistics

The batching subsystem is designed to be primarily invisible to the user, except perhaps for improved throughput under high load.  However, some parameters of the batching subsystem are tunable via Cuttlefish configuration properties in the `riak.conf` configuration file (or, alternatively, via the `advanced.config` file).  Changes to this file require a restart of Riak (or, alternatively, of just the Yokozuna application, via the Riak console).

The batching subsystem also introduces a set of statistics, which provides operators visibility into such measurements as available queue capacity, averages and histograms for batch sizes, AAE activity, and so forth.

This section describes the commands, configuration parameters, and statistics of the batching subsystem, from the user's point of view.

## Commands

The `riak-admin` command may be used to control the participation of a node in distributed query.  This command can be useful, for example, if a node is down for repair or reindexing.  The node can be temporarily removed from coverage plans, so that it is not consulted as part of a distributed query.

Here are sample usages of this command:

    shell$ riak-admin set search.dist_query=off      # disable distributed query for this node
    shell$ riak-admin set search.dist_query=on       # enable distributed query for this node
    shell$ riak-admin show search.dist_query         # get the status of distributed query for this node

> Note. that even if a node is removed from a distributed query, it's search endpoint may still be consulted for query.  If distributed query is disabled on the node and the search endpoint is used for query, only the other available nodes in the cluster will be consulted as part of a distributed query.

Using this command will only temporarily enable or disable distributed query until explicitly disabling or re-enabling via the same command, or after restart.  See the `search.dist_query` configuration setting to control a node's participation in drributed queries across restarts of a Riak server.

## Configuration

The behavior of the batching subsystem may be controlled via the following Cuttlefish configuration parameters, as defined in `riak.conf`.  Consult the Cuttlefish schema (TODO add link) for the associated configuration settings in the Riak `advanced.config` file.

* `search.queue.batch.minimum` (default: 10) The minimum batch size, in number of Riak objects. Any batches that are smaller than this amount will not be immediately flushed to Solr, but are guaranteed to be flushed within the value specified in `search.queue.batch.flush_interval`.

* `search.queue.batch.maximum` (default: 500)  The maximum batch size, in number of Riak objects. Any batches that are larger than this amount will be split, where the first `search.queue.batch.maximum` objects will be flushed to Solr, and the remaining objects enqueued for that index will be retained until the next batch is delivered.  This parameter ensures that at most `search.queue.batch.maximum` objects will be delivered into Solr in any given request.

* `search.queue.batch.flush_interval` (default: 500ms)  The maximum delay between notification to flush batches to Solr.  This setting is used to increase or decrease the frequency of batch delivery into Solr, specifically for relatively low-volume input into Riak.  This setting ensures that data will be delivered into Solr in accordance with the `search.queue.batch.maximum` and `search.queue.batch.maximum` settings within the specified interval.  Batches that are smaller than `search.queue.batch.maximum` will be delivered to Solr within this interval.  This setting will generally have no effect on heavily loaded systems.

* `search.queue.high_watermark` (default: 1000)  The queue high water mark.  If the total number of queued messages in a Solrq worker instance exceeds this limit, then the calling vnode will be blocked until the total number falls below this limit.  This parameter exercises flow control between Riak and the Yokozuna batching subsystem, if writes into Solr start to fall behind.

* `search.index.error_threshold.failure_count` (default: 3)  The number of failures within the specified `search.index.error_threshold.failure_interval` before writes into Solr will be short-circuited.  Once the error threshold is crossed for a given Riak index (i.e., Solr core), Yokozuna will make no further attempts to write to Solr for objects destined for that index until the error is reset.

* `search.index.error_threshold.failure_interval` (default: 5 seconds)  The window of time in which `search.index.error_threshold.failure_count` failures will cause the error threshold for a given Riak index to be crossed, and for writes to that index to be short-circuited.

* `search.index.error_threshold.reset_interval` (default: 30 seconds)  The amount of time it takes for a an error error threashold traversal associated with a Solr core to reset.  If `search.index.error_threshold.failure_count` failures occur within `search.index.error_threshold.failure_interval`, requests to Solr for that core are short-circuited for this interval of time.

* `search.queue.high_watermark.purge_strategy` (default: `purge_one`)  The high watermrk purge strategy.  If a Solr core threshold is traversed, and if the number of enqueued messages in a solr worker exceeds `search.queue.high_watermark`, then Yokozuna will use the defined purge strategy to purge enqueued messages.  Valid values are `purge_one`, `purge_index`, and `off`.

* `search.anti_entropy.throttle` (default: on)  Whether the throttle for Yokozuna active anti-entropy is enabled.

* `search.anti_entropy.throttle.$tier.solrq_queue_length`  Sets the throttling tiers for active anti-entropy. Each tier is a minimum solrq queue size and a time-delay that the throttle should observe at that size and above. For example:

        search.anti_entropy.throttle.tier1.solrq_queue_length = 0
        search.anti_entropy.throttle.tier1.delay = 0ms
        search.anti_entropy.throttle.tier2.solrq_queue_length = 40
        search.anti_entropy.throttle.tier2.delay = 5ms

    will introduce a 5 millisecond sleep for any queues of length 40 or higher. If configured, there must be a tier which includes a mailbox size of 0. Both `.solrq_queue_length` and `.delay` must be set for each tier.  There is no limit to the number of tiers that may be specified.

* `search.anti_entropy.throttle.$tier.delay`  See above.

* `search.dist_query` (Default: on)  Enable or disable this node in distributed query plans.  If enabled, this node will participate in distributed Solr queries.  If disabled, the node will be excluded from yokozuna cover plans, and will therefore never be consulted in a distributed query.  Note that this node may still be used to execute a query.  Use this flag if you have a long running administrative operation (e.g., reindexing) which requires that the node be removed from query plans, and which would otherwise result in inconsistent search results.

The following options are hidden from the default `riak.conf` file:

* `search.queue.drain.timeout` (default: 1 minute)  The amount of time to wait before a drain operation times out.  If a drain times out during an AAE exchange, the exchange is cancelled and retried at a later time.

* `search.queue.drain.cancel.timeout` (default: 5 seconds)  The amount of time to wait before a drain cancel times out.  If a drain cancel times out during an AAE exchange, the entity responsible for draining is forcibly terminated, and the exchange is cancelled and retried at a later time.

* `search.queue.drain.enable` (default: true)  When enabled, enqueued Riak objects are drained prior to an AAE exchange, in order to minimize AAE activity, and during shutdown. If AAE is disabled, this setting only has an effect on shutdown behavior. Users generally have little reason to disable draining.

* `search.ibrowse_max_sessions` (default: 100)  The value to use for the `max_sessions` configuration for the ibrowse process used by Yokozuna.

* `search.ibrowse_max_pipeline_size` (default: 1) The value to use for the `max_pipeline_size` configuration for the ibrowse process used by Yokozuna.


## Statistics

The Yokozuna batching subsystem maintains a set of statistics that provide visibility into the run-time characteristics of the components that make up the system.  These statistics are accessible via the standard Riak stats interfaces and can be monitored through standard enterprise management tools.

* `search_index_throughput_(count|one)`  The total number of operations that have been delivered to Solr, per Riak node, and the number of operations that have been delivered to Solr within the metric measurement window.

* `search_index_latency_(min|mean|max|median|95|99|999)`  The minimum, mean, maximum, median, 95th percentile, 99th percentile, and 99.9th percentile measurements of indexing latency, as measured from the time it takes to send a batch to Solr to the time the response is received from Solr, divided by the batch size.

* `search_index_fail_(count|one)`  The total count of failed attempts to index, per Riak node, and the count of index failures within the metric measurement window.

* `search_queue_batch_throughput_(count|one)`  The total number of batches delivered into Solr, per Riak node, and the number of batches that have been indexed within the metric measurement window.

* `search_queue_batch_latency_(min|mean|max|median|95|99|999)`  The minimum, mean, maximum, median, 95th percentile, 99th percentile, and 99.9th percentile measurements of batch latency, as measured from the time it takes to send a batch to Solr to the time the response is received from Solr.

* `search_queue_batchsize_(min|mean|max|median)`  The minimum, mean, maximum, and median measurements of the batch size across all indices and Solrq worker processes.

* `search_queue_hwm_purged_(count|one)`  The total number of purged objects, and the number of purged objects within the metric measurement window.

* `search_queue_total_length`  The total length of the existing queues.  This measurement is the sum of the lengths of each indexq in each solrq worker process.

* `search_blockedvnode_(count|one)`  The total count of vnodes that have been blocked, per Riak node, and the count of blocked vnodes within the metric measurement window.  VNodes are blocked when a Solrq worker exceeds it high water mark, as defined by the `solrq_queue_hwm` configuration setting.

* `search_queue_drain_(count|one)`  The total number of drain operations, and the number of drain operations within the metric measurement window.

* `search_queue_drain_fail_(count|one)`  The total number of drain failures, and the number of drain failures within the metric measurement window.

* `search_queue_drain_timeout_(count|one)`  The total number of drain timeouts, and the number of drain timeouts within the metric measurement window.

* `search_queue_drain_latency_(min|mean|max|median|95|99|999)`  The minimum, mean, maximum, median, 95th percentile, 99th percentile, and 99.9th percentile measurements of drain latency, as measured from the time it takes to initiate a drain to the time the drain is completed.

* `search_detected_repairs_count`  The total number of AAE repairs that have been detected when comparing YZ and Riak/KV AAE trees.  Note that this statistic is a measurement of the differences found in the AAE trees; there may be some latency between the time the trees are compared and the time that the repair is written to Solr.

* `search_index_bad_entry_(count|one)`  The number of writes to Solr that have resulted in an error due to the format of the data (e.g., non-unicode data) since the last restart of Riak.

* `search_index_extract_fail_(count|one)`  The number of failures that have occurred extracting data into a format suitable to insert into Solr (e.g., badly formatted JSON) since the last start of Riak.

The following statistics refer to query operations, and are not impacted by Yokozuna batching.  They are included here for completeness:

* `search_query_throughput_(count|one)`  The total count of queries, per Riak node, and the count of queries within the metric measurement window.

* `search_query_latency_(min|mean|max|median|95|99|999)`  The minimum, mean, maximum, median, 95th percentile, 99th percentile, and 99.9th percentile measurements of querying latency, as measured from the time it takes to send a request to Solr to the time the response is received from Solr.

* `search_query_fail_(count|one)`  The total count of failed queries, per Riak node, and the count of query failures within the metric measurement window.
