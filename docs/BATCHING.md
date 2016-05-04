
# Introduction

In Yokozuna versions prior to 2.0.4, update operations on Solr (notably, `add` and `delete` operations) are synchronous blocking operations and are performed once-at-a-time in Solr.  In particular, calls to the `yz_kv:index/3` Erlang function block until the associated data is written to Solr, and each such call results in an HTTP POST with a single Solr operation.

Yokozuna version 2.0.4 introduces batching and asynchronous delivery of Solr operations.  The primary objective of this work is to decouple update operations in Solr from the Riak vnodes that are responsible for managment of replicas of Riak objects across the cluster.  Without batching and asynchronous delivery, Riak vnodes have to wait for Solr operations to complete, sometimes an inordinate amount of time, which can have an impact on read and write operations in a Riak cluster, even for Riak objects that aren't being indexed through Yokozuna!  This feature is intended to free up Riak vnodes to do other work, thus allowing Riak vnodes to service requests from more clients concurrently, and increasing operational throughput throughout the cluster.

De-coupling indexing operations from Riak vnode activity, however introduces some complexity into the system, as there is now the possibility for indeterminate latency between the time that an object is available in Riak versus its availability in Yokozuna and Solr.  This latency can introduce  divergence between the Riak AAE trees stored in Riak/KV and the AAE trees stored in Yokozuna, with which the Riak/KV trees are compared.

This document describes the batching and asynchronous delivery subsystem introduced in Yokozuna 2.0.4, from both the end-users' and implementors' point of view, as well as the methods by which divergence between Riak and Solr is mitigated.


# Overview

The Yokozuna batching subsystem introduces two sets of additional `gen_server` processes into each Riak node, a set of "workers", and a set of "helpers".  The job of the worker processes is to enqueue updates from components inside of Riak -- typically Riak vnodes, but also sometimes parts of the Yokozuna AAE subsystem -- and to make enqueued objects available to helper processes, which, in turn, are responsible for dispatching batches of operations to Solr.

For each Riak node, there is a fixed, but configurable, number of helper and worker processes, and by default, 10 of each are created.  A change to the number of workers or helpers requires a restart of the Yokozuna OTP application.

When enqueuing an object, a worker process is selected by hashing the Solr index (core) with which the operation is associated, along with the Riak bucket and key (bkey).  This hash, modulo the number of workers, determines the worker process on which the datum is enqueued.  When a batch of objects is dequeued from a worker process, a helper is randomly selected from among the set of configured helpers using a uniform PRNG.  These algorithms for selecting workers and helpers is designed to provide as even a distribution of load across all workers and helpers as possible.

Once a batch is dequeued from a worker, the Riak objects are transformed into Solr operations, and a single HTTP request is formed, containing a batch of Solr operations (encoded in a JSON payload).  The batch is then delivered to Solr.

The following diagram illustrates the relationship between Riak vnodes, Yokozuna workers and helpers, and Apache Solr:

![YZ Batching Overview](https://raw.githubusercontent.com/basho/yokozuna/feature-solr-batching-rebased/docs/yz-batching-overview.png)

Each helper process is stateless; a helper's only role is to dequeue batches from the workers, to transform those batches into something that Solr can understand, and to dispatch the transformed batches to Solr (and report the results back to the worker process, on whose behalf it is doing the work).

The worker processes, on the other hand, maintain the queues of objects that are ready for update in Solr, in addition to managing the run-time behavior between the batching subsystem and the collection of vnodes and other processes that are communicating with the workers.

In order to batch operations into a single HTTP POST into Solr, all operations must be organized under the same Solr core, i.e. Riak search index.  As a consequence, each Yokozuna worker process maintains a table (`dict`) of "indexq" structures, keyed off the Riak search index.  These indexq structures include, most critically, the enqueued object

Indexq structures are created on-demand in each worker process, as data is added to the system.  In most cases, all worker processes will contain entries for each Riak serch index, but it is sometimes possible for a worker process to be missing an entry for given index because, for example, it has not yet seen an object that needs to be updated for a given search index.  This is expected behavior.

The relationship between Riak search indices and indexq structures within each worker process is illustrated in the following diagram:

![YZ Batching Worker](https://raw.githubusercontent.com/basho/yokozuna/feature-solr-batching-rebased/docs/yz-batching-worker.png)

## Batching Parameters

When an update message is sent to a worker process, it is immediately enqueued in the indexq structure associated with the index for which the operation is destined.

The Yokozuna batching subsystem provides configuration parameters that drive batching benavior, including configuration of:

* minimum batch size (default: 1);
* maximum batch size (default: 100);
* flush interval (default: 1 second)

When an update message is enqueued, and if the total number of enqueued messages (including the new message) is at least the specified minimum batch size, then the helper process associated with that index and worker is notified that a batch is ready for processing.  The helper then requests a batch from the worker, which delivers a batch back to the helper that is no larger than the maximum batch size.  The helper then translates the batch into a format suitable for Solr, and dispatches the batch to Solr via a single HTTP POST.

If when enqueing an update operation the number of batched messages is smaller than the configured minimum batch size, then a timer is set based on the configured flush interval, which will guarantee that any enqueued batches smaller than the configured minimum will be flushed to Solr within the specified interval.

## Backpressure

Each worker process is configured with a high water mark (10000, by default), which represents the total number of messages that may be enqueued across all indexq structures in a given worker process before calls into the batching subsystem (update/index) will block calling vnodes.  If the total number of enqueued messages exceeds this threshold, calling vnodes (and parts of the AAE subsystem) will block until data is successfully written to Solr, or it is purged, in a manner described below.

This way, the batching subsystem exerts back-pressure on the vnode and AAE systems, in the case where Solr is being driven beyond its operational capacity.

## Failure Scenarios, Error Thresholds, and Retry

In most desirable scenarios, batches that are dequeued from worker processes are immediately translated and dispatched to Solr with no error.

In the undesirable cases where Solr becomes unresponsive (e.g., data corruption, excessively long garabage collection), Yokozuna has a mechanism for detecting transient failure in the HTTP POST requests, and for tracking how frequently failures occur for a given Solr core.  If too many errors occur within a given time window, then the Yokozuna batching system will re-enqueue failed messages for subsequent delivery, once the Solr core again becomes responsive.  This error threshold prevents Yokozuna from unecessarily adding load to Solr, if for some reason a particular search index has become unresponsive.

## Purge Strategies

If a Solr core has become unresponsive and the specified error threshold has been traversed, and if, in addition, the high water mark has been exceeded, then the yokozuna batching system has a mechanism for automatically purging enqueued entries, so as to allow vnodes to continue servicing requests, as well as to allow update operations to occur for indices that are not in a pathological state.

The yokozuna batching subsystem supports 4 different purge strategies:

* `purge_one` (default behavior): Purge the oldest entry from a randomly selected indexq structure among the set of search indexes which have crossed the error threshold;
* `purge_index`: Purge all entries from a randomly selected indexq structure among the set of search indexes which have crossed the error threshold;
* `purge_all`: Purge all entries from all indexq structure among the set of search indexes which have crossed the error threshold;
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

# Configuration and Statistics

The batching subsystem is designed to be primarily invisible to the user, except perhaps for improved throughput under high load.  However, some parameters of the batching subsystem are tunable via Cuttlefish configuration properties in the `riak.conf` configuration file (or, alternatively, via the `advanced.config` file).  Changes to this file require a restart of Riak (or, alternatively, of just the Yokozuna application, via the Riak console).

The batching subsystem also introduces a set of statistics, which provides operators visibility into such measurements as available queue capacity, averages and histograms for batch sizes, AAE activity, and so forth.

This section describes the configuration parameters and statistics of the batching subsystem, from the user's point of view.

## Configuration

The behavior of the batching subsystem may be controlled via the following Cuttlefish configuration parameters, as defined in `riak.conf`.  Consult the Cuttlefish schema (TODO add link) for the associated configuration settings in the Riak `advanced.config` file.

* `search.queue.batch.minimum` (default: 1) The minimum batch size, in number of Riak objects. Any batches that are smaller than this amount will not be immediately flushed to Solr, but are guaranteed to be flushed within the value specified in `search.queue.batch.flush_interval`.

* `search.queue.batch.maximum` (default: 100)  The maximum batch size, in number of Riak objects. Any batches that are larger than this amount will be split, where the first `search.queue.batch.maximum` objects will be flushed to Solr, and the remaining objects enqueued for that index will be retained until the next batch is delivered.  This parameter ensures that at most `search.queue.batch.maximum` objects will be delivered into Solr in any given request.

* `search.queue.batch.flush_interval` (default: 1 second)  The maximum delay between notification to flush batches to Solr.  This setting is used to increase or decrease the frequency of batch delivery into Solr, specifically for relatively low-volume input into Riak.  This setting ensures that data will be delivered into Solr in accordance with the `search.queue.batch.maximum` and `search.queue.batch.maximum` settings within the specified interval.  Batches that are smaller than `search.queue.batch.maximum` will be delivered to Solr within this interval.  This setting will generally have no effect on heavily loaded systems.

* `search.queue.high_watermark` (default: 10000)  The queue high water mark.  If the total number of queued messages in a Solrq worker instance exceeds this limit, then the calling vnode will be blocked until the total number falls below this limit.  This parameter exercises flow control between Riak and the Yokozuna batching subsystem, if writes into Solr start to fall behind.

* `search.queue.worker_count` (default: 10)  The number of solr queue workers to instantiate in the Yokozuna application.  Solr queue workers are responsible for enqueing objects for insertion or update into Solr. Increasing the number of solr queue workers distributes the queuing of objects, and can lead to greater throughput under high load, potentially at the expense of smaller batch sizes.

* `search.queue.helper_count` (default: 10)  The number of solr queue helpers to instantiate in the Yokozuna application.  Solr queue helpers are responsible for delivering batches of data into Solr.  Increasing the number of solr queue helpers may increase concurrent writes into Solr.

* `search.index.error_threshold.failure_count` (default: 3)  The number of failures within the specified `search.index.error_threshold.failure_interval` before writes into Solr will be short-circuited.  Once the error threshold is crossed for a given Riak index (i.e., Solr core), Yokozuna will make no further attempts to write to Solr for objects destined for that index until the error is reset.

* `search.index.error_threshold.failure_interval` (default: 5 seconds)  The window of time in which `search.index.error_threshold.failure_count` failures will cause the error threshold for a given Riak index to be crossed, and for writes to that index to be short-circuited.

* `search.index.error_threshold.reset_interval` (default: 30 seconds)  The amount of time it takes for a an error error threashold traversal associated with a Solr core to reset.  If `search.index.error_threshold.failure_count` failures occur within `search.index.error_threshold.failure_interval`, requests to Solr for that core are short-circuited for this interval of time.

* `search.queue.high_watermark.purge_strategy` (default: `purge_one`)  The high watermrk purge strategy.  If a Solr core threshold is traversed, and if the number of enqueued messages in a solr worker exceeds `search.queue.high_watermark`, then Yokozuna will use the defined purge strategy to purge enqueued messages.  Valid values are `purge_one`, `purge_index`, `purge_all`, and `off`.


The following options are hidden from the default `riak.conf` file:

* `search.queue.drain.timeout` (default: 1 minute)  The amount of time to wait before a drain operation times out.  If a drain times out during an AAE exchange, the exchange is cancelled and retried at a later time.

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

* `search_queue_capacity`  The capacity of the existing queues, expressed as a integral percentage value between 0 and 100.  This measurement is based on the ratio of equeued objects and the configured high water mark.

* `search_blockedvnode_(count|one)`  The total count of vnodes that have been blocked, per Riak node, and the count of blocked vnodes within the metric measurement window.  VNodes are blocked when a Solrq worker exceeds it high water mark, as defined by the `solrq_queue_hwm` configuration setting.

* `search_queue_drain_(count|one)`  The total number of drain operations, and the number of drain operations within the metric measurement window.

* `search_queue_drain_fail_(count|one)`  The total number of drain failures, and the number of drain failures within the metric measurement window.

* `search_queue_drain_timeout_(count|one)`  The total number of drain timeouts, and the number of drain timeouts within the metric measurement window.

* `search_queue_drain_latency_(min|mean|max|median|95|99|999)`  The minimum, mean, maximum, median, 95th percentile, 99th percentile, and 99.9th percentile measurements of drain latency, as measured from the time it takes to initiate a drain to the time the drain is completed.

* `search_detected_repairs_count`  The total number of AAE repairs that have been detected when comparing YZ and Riak/KV AAE trees.  Note that this statistic is a measurement of the differences found in the AAE trees; there may be some latency between the time the trees are compared and the time that the repair is written to Solr.

The following statistics refer to query operations, and are not impacted by Yokozuna batching.  They are included here for completeness:

* `search_query_throughput_(count|one)`  The total count of queries, per Riak node, and the count of queries within the metric measurement window.

* `search_query_latency_(min|mean|max|median|95|99|999)`  The minimum, mean, maximum, median, 95th percentile, 99th percentile, and 99.9th percentile measurements of querying latency, as measured from the time it takes to send a request to Solr to the time the response is received from Solr.

* `search_query_fail_(count|one)`  The total count of failed queries, per Riak node, and the count of query failures within the metric measurement window.


# Implementation Notes

This section describes the internal components that form the batching subsystem.  It is targeted primarily at a developer audience.  By convention, the Erlang modules that form this subsystem contain the `solrq` moniker in their names.

In the remainder of this document, we generalize update and delete operations that are posted to solr as simply "Solr operations", without considering whether the operations originated as the result of a Riak put operations, delete operation, or AAE and/or read repair.

## Supervision Tree

The Solrq subsystem contains a supervision hierarchy that branches off the `yz_general_sup` supervisor in the Yokozuna supervision tree.  The `yz_solrq_sup` is the top-level supervisor in this hierarchy.

The `yz_solrq_sup` supervisor monitors a pool of Solrq workers and a pool of Solrq helpers.  The objects in this pool are OTP registered gen_server processes, taking on the names `yz_solrq_i` and `yz_solrq_helper_j`, respectively, where i and j are padded integer values in the range {1..n} and {1..m}, respectively, where n and m are defined in config via the `num_solrq` and `num_solrq_helpers` Yokozuna configuration properties, respectively.  These values are configurable, but both default to 10.

The following diagram illustrates this supervision hierarchy:

![YZ Batching Supervision Tree](https://github.com/basho/internal_wiki/blob/master/images/yokozuna/yz-solrq-supervision-tree.png)

There will generally be a `yz_solrq_sup` per `yokozuna` application instance.

> **Note.**  The `yz_solr_sup` module also provides an entrypoint API for components in the batching subsystem.  Beware that these APIs are simply convenience functions through a common module, and that all calls through these APIs are sequential Erlang functions, and are not executed in the context of the `yz_solrq_sup` supervisor OTP process.

## Solrq Component Overview

The `yz_solrq_sup`, `yz_solrq`, and `yz_solrq_helper` processes form the major components in the Yokozuna batching subsystem.

When a `riak_kv_vnode` services a put or delete request, an API call is made into the `yz_solrq_sup` module to locate a `yz_solrq` instance from the pool of Solrq workers, by taking the (portable) hash of the index and the bucket and key (bkey) associated with the Riak object, and "dividing" that hash space by the number of workers in the pool, n:

    hash({Index, BKey}) mod n -> yz_solrq_i

This way, we get a roughly even distribution of load to all workers in the pool, assuming an even distribution of writes/deletes to indices and partitions.

Once a worker is located, the Riak Object and associated operational data is enqueued onto the queue associated with the index on the worker.  (The internal structure of the Solrq worker process is discussed below in more detail.)

It is the job of the `yz_solrq_helper` process to periodically pull batches of data that have been enqueued on the worker queues, to prepare the data for Solr, including extracting fields via Yokozuna extractors, as well as translation to Solr operations, and to dispatch Solr operations to Solr via HTTP POST operations.

The `yz_solrq_helper` instances form a pool, as well, and an instance of a helper is selected by using a uniform random distribution.

The following diagram illustrates the relationship between these components:

![YZ Batching Overview](https://github.com/basho/internal_wiki/blob/master/images/yokozuna/yz-batching-overview.png)

The following subsections describe these components in more detail.

### The Solrq Worker Process(es)

The Yokozuna Solrq batching subsystem maintains a fixed-size (but configurable) pool of worker processes, which are responsible for enqueuing messages to be delivered to Solr.

The Solrq worker processes provide caching of (add/delete) operations written to Solr.  As gen_server processes, they maintain state about what data is cached for batching, as well as book keeping information about how the queues are configured.  The Solrq helper processes (see below) coordinate with the worker processes, and do the actual writes to Solr.  But it is the worker processes that are the first line of caching, between Yokozuna and Solr.

Solr workers are gen_server processes that form a pool of batching/caching resources.  As a pool of resources, they provide increased throughput under high load, as indexing/delete operations can be distributed evenly across workers in the pool.  However, when posting an operation (add/delete) to Solr, all operations (or any batched operations thereof) need to take place under the umbrella of a single Solr index (or "core").  Specifically, the HTTP POST operation to Solr to update an index contains the Solr index as part of its URL, even if that POST operation contains multiple documents.

As a consequence, the Solrq workers must partition the operations by index, so that when batches are delivered to Solr, each batch is POSTed under the URL of a single core.

Each Solrq process manages this partitioning by maintaining a dictionary of index -> `indexq` mappings, where the index key is a Solr index name, and an `indexq` is a data structure that holds the queue of operations to be POSTed to Solr under that index.

> **Note.**  This mapping is encpasulated in an Erlang `dict` structure, but the population of entries in this dictionary are done lazily.  Initially, the dictionary on a `yz_solrq` instance is empty, and as operations are performed on an index, entries are added.  Eventually, a given worker instance may contain an entry for each index configured by the user, but due to the luck of the draw, there may be some solrq instances that do not contain entries for some indices (e.g., if objects map to a strict subset of partitons on a given node, for example).

The `indexq` structure contains book keeping information, such as the queue of data for that index, its length, cached configuration information for that queue, such as its minimum and maximum batch size, and other state information described in more detail below.

The Yokozuna Solrq worker process is illustrated in the following diagram:

![YZ Solrq Worker](https://github.com/basho/internal_wiki/blob/master/images/yokozuna/yz-solrq-worker.png)

### Solrq Helper Process(es) and Batching Protocol

The `yz_solrq_helper` process is a stateless `gen_server` OTP process, whose only role is to dequeue data from `yz_solrq` instances, to dispatch the data to Solr, and to reply back to the worker process when a batch has completed.  As a stateless object, there is not much to describe about a helper process, except for the protocol of messages that are sent between the `riak_kv_vnode`, `yz_solrq` worker, and `yz_solrq_helper` processes.

When an indexing operation (add/delete) is requested from a `riak_kv_vnode`, a `yz_solrq` worker process is located (as described above), and a syncronous index message is delivered to the worker, via a `gen_server:call`:

    {index, Index, {BKey, Docs, Reason, P}}

If the total number of queued messages for this worker is not above the configured high water mark (See the Backpressure section, below), an `ok` is immediately delivered back to the vnode, and the requested data is enqueued onto the `yz_solrq` worker, keyed off the supplied index.

If the number of queued messages is above the configured minimum (default: 1), and if there are currently no pending helpers who have been told to request a batch, the `yz_solrq` worker will locate a `yz_solrq_helper` as described above, and send it an asynchronous message (via `gen_server:cast`), telling it that the worker is ready to deliver a batch:

    {ready, Index, QPid}

> **Note.**  This asynchronous message is delivered via a cast to the worker.  No further work is required on the part of the worker, and it does not synchronously wait for a response from the helper.

Upon receipt of the `ready` message, the selected helper will send an asynchronous message back to the worker, requesting a batch for the specified index:

    {request_batch, Index, HPid}

Upon receipt of this message, the worker will select a batch of messages to deliver, typically less than or equal to the size of the configured maximum batch size, and send a batch message back to the helper:

    {batch, Index, BatchMax, QPid, Entries}

> **Note.**  The number of entries in the batch may exceed the configured batch maximum in the case where the queues are being drained.  See the "Draining" section below for more information.

> **Comment.** I'd clear up what async means here. We're still "synchronously" waiting for the solr http update to finish, via an ibrowse call, before we return the cast message, right?

Upon receipt of the `batch` message, the helper dispatches the batch (or batches, in the case of draining) to Solr, and then sends an asynchronous message back to the worker, indicating that the batch (or batches) have completed, the number of messages that were delivered into Solr, and a return status, indicating whether the writes to Solr were successful or not:

    {batch_complete, NumDelivered, Result}

> *Note.*  The `batch_complete` message is new, and has not been shipped with any of the patches to date.

Upon receipt of the `batch_complete` message, the worker will:

* Decrement the number of queued messages by the number of delivered messages
* Unblock any waiting vnodes if the total number of queued messages is below the high water mark
* Mark the pending state of the batch back to false, so that new batches for the same index can proceed
* Request a helper, if there is still data left in the queues to be flushed and if the number of queued messages is above the configured minimum.
* Re-queue (but pre-pend) any undelivered messages, in case of any failures in delivery

The Yokozuna batching protocol between vnodes, workers, and helpers is illustrated in the following diagram.

![YZ Solrq Batching Protocol](https://github.com/basho/internal_wiki/blob/master/images/yokozuna/yz-batching-sequence.png)



### Flushing

In the above scenario, data enqueued for a specific index is written to Solr as soon as the number of queued messages exceeds the configured minimum, which defaults to 1.  Hence, in many cases, enqueued messages get sent almost immediately after being enqueued, but asynchronously from the perspective of vnodes.

> *Note*.  This does not entail that batch sizes are always 1; because the messaging protocol between workers and helpers is asynchronous, more messages may arrive in the queue between the time that a worker is notified that a batch is ready and the time that the batch is actually retrieved from the queues.  In heavily loaded systems, the queues typically grow in size in this time.

What happens, however, if the user configures a relatively high minimum batch size, but data drips into the queues at a relatively slow rate?

In this case, the batching subsystem will set a timer, specified by the `solrq_delayms_max` configuration setting (default: 1000ms).  If no data has been flushed within this time interval (which would happen, for example, as the result of a write into the queue), then a batch will automatically be initiated, along the lines of the protocol described above.

## Backpressure

Each `yz_solrq` worker maintains a record of how many messages are enqueued in all of the `indexq` structures held in the worker.  When data is sent to a worker from a vnode, the worker will increment the count of queued messages.  If the total count exceeds the configured high water mark (`solrq_queue_hwm`; default: 10000), a reply to the calling vnode is deferred until the total count of queued messages again falls below this limit.

The number of enqueued messages is decremented once the `batch_complete` message is received from the associated helper process, where the message received back from the helper contains the number of messages that have been delivered to Solr.

> **Note.**  In previous implementations of the Yokozuna batching patch, the total number of enqueued messages was decremented when a batch was delivered to the helper for dispatch to the worker, but this behavior has changed with the introduction of the `batch_complete` message.

> **Note.**  Setting the `solrq_batch_min` and `solrq_batch_max` values to 1 results in immediate backpressure on the vnode, until the message has been successfully delivered to Solr.  This special case of the Yokozuna batching patch simulates the `dw` semantics of the pre-batching Yokozuna implementation.

### Fuses

Yokozuna batching makes use of the [Fuse](https://github.com/jlouis/fuse) library, an OTP application which supports a circuit-breaker pattern.  Every time some condition applies (at the discretion of the application), a fuse is "melted" slightly.  If some configured number of melts occur within a configured time interval, then the fuse "blows", and applications can then modify their behavior, with the knowledge that some operation is likely to be fruitless.  Fuses eventually "heal", after a configured amount of time, presumably while not under load.

Yokozuna uses this pattern in its handling of communication with Solr.  For each Solr index, a fuse is created, which represents the connection to Solr.  If requests to Solr continuously fail (for a given index), the fuse for that index is blown, and any subsequent Solr requests for that index are short-circuited.  Once a fuse heals, batching resumes.

The Fuse library makes use of Erlang alarm handling, whereby fuse events (fuse "trips" and "heals") are delivered through Erlang alarm handlers.  The `yz_events` event handler subscribes to these events, and will notify the batching subsystem when a fuse blows or heals.

If a fuse trips, this has the effect of effectively pausing the delivery of any batches to Solr, for a given Solr index (the index associated with the tripped fuse).  Once a fuse heals, delivery of batches is resumed.

If delivery of batches to Solr is paused, then it is possible that enqueued messages will pile up, potentially reaching the configured high water mark (and thus causing back-pressure on the calling vnodes).  If the `purge_blown_indices` yokozuna configuration variable is set to true (its default value), then when the high water mark is reached, then entries will be discarded on indices that are blocked.  A warning will be logged to the system logs, indicating which index has had data discarded (but not the data contents).

> Note.  The rationale for defaulting this value to true is that AAE is also enabled by default on Riak systems, and that any divergence between Riak and Yokozuna can be eventually repaired by AAE.

If the `purge_blown_indices` yokozuna configuration variable is set to false, then no data will be automatically purged from indices that have blown fuses, and the only way to resume batching for *all* indices (and therefore to relieve back pressure on the vnodes on the Riak node) is for the fuse to heal.  For pathological indices, this may take a long time!  This variable should only be set to false if Yokozuna AAE is not enabled, and if Solr indices are known to be well-behaved.  Otherwise, there is a non-trivial risk that all of Riak can wedge while fuses heal.

## Draining

Some applications have the need to not only to periodically flush idle queues, but also to completely drain the contents of all queues on demand, or at least all of the messages associated with a given Riak partition.  These applications include stopping the Yokozuna application (to ensure everything in memory is flushed to Solr), as well as the YZ AAE subsystem, which, when exchanging hash trees between Riak K/V and Yokozuna, snapshots the Riak K/V hash tree, drains the queues, and then snapshots the Yokozuna hash tree, in order to minimize divergence between the two hash trees.

Draining is provided as a `drain` function in the `yz_solrq_mgr` module, which, when invoked, will spawn and monitor a `gen_fsm` process, `yz_solrq_drain_fsm`.  The role of the FSM is to trigger a drain on all of the `yz_solrq` workers in the pool, and then to wait for all of the drains to complete.  Once all of the queues have been drained, the FSM terminates, and the calling process will receive a `'DOWN'` message from the FSM it is monitoring, and the drain function can then return to the caller.  If not all of the queues have drained within a configured timeout, the drain function will return `{error, timeout}`.

In the current implementation, there may only be one Drain FSM active at time.  An attempt to call the `drain` funtion while a drain is active will result in a return value of `{error, in_progress}`

The `yz_solrq_drain_fsm` has two states:

* *prepare* In this state, the Drain FSM will iterate over all `yz_solrq` instances, generate a token (via `erlang:make_ref()`) for each instance, and send the `{drain, DPid, Token, Partition}` message to each worker, where `DPid` is the PID of the drain FSM, and `Token` is the generated token, and `Partition` is the desired Riak partition to drain (or `undefined`, if all messages should be drained).  It will then enter the *wait* state.

* *wait*  In this state, the Drain FSM will wait for `{drain_complete, Token}` messages back from each `yz_solrq` worker instance.  Once all delivered tokens have been received, the Drain FSM will terminate normally.

When a `yz_solrq` worker receives a `{drain, DPid, Token, Partition}` message, it will iterate over all of it `indexq` structures, set the `draining` flag on each structure, and initiate the batching protocol with an associated helper, as described above.  However, unlike the normal batching case, when the worker is in the draining state, it will deliver *all* of its enqueued messages that match the specified partition (or all messages, if `Partition` is `undefined`) to the helper, along with the configured maximum batch size.  In this case, the helper will iterate over all of the messages it has been sent, and form batches of messages for delivery to Solr.  For example, if the batch size is 100 and 1027 messages have been enqueued, the helper will end up sequentially delivering 10 batches of 100, and then an 11th of size 27.

While in the draining state, and messages that get enqueued get put onto a special "auxiliary queue" (`aux_queue`), which prevents them from getting sent in any batches to Solr.  This prevents messages new messages delivered during the drain phase from getting written to Solr.  When the drain for an `indexq` is completed, the `draining` flag is reset to false, and any messages on the `aux_queue` are moved back to the normal queue, for subsequent dispatch into Solr.

Once the batch has completed and a `batch_complete` message is sent back to the worker, the worker records which index has been drained.  Once all of the indices have been drained in the worker, it sends the `{drain_complete, Token}` message back to the Drain FSM, thus completing the drain for that `yz_solrq` worker instance.  It will not, however, continue with batching until it receives a `batch_complete` message back from the `yz_solrq_drain_fsm`.  This blocks writes into Solr until all queues have been drained.

The `yz_solrq_drain_fsm` will stay in the waiting state until all of the tokens it has delivered to the `yz_solrq` instances have been returned.  Once all of the tokens are returned, it will update the YZ index hashtree before firing off a `drain_complete` message to all of the `solrqs`, indicating that they can proceed with normal batching operations.  Any messages that have been cached on the auxiliary queues are then moved to the normal queue, and batching proceeds as usual.  The `yz_solrq_drain_fsm` then terminates, indicating to the caller that all queues have drained.

The relationship between the caller of the `drain` function (in this example, `yz_exchange_fsm`), the `yz_solrq_drain_fsm`, and the Solrq workers and helpers is illustrated in the following sequence diagram:

![YZ Solrq Draining](https://github.com/basho/internal_wiki/blob/master/images/yokozuna/yz-solrq-draining.png)
