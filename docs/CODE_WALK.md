Code Walk
=========

A walk through various parts of the code to gain a deeper
understanding of Yokozuna as a developer or operator.

Module Index
------------

A one line summary of every module.  This table may lag behind the
code.  Please file an issue if you notice modules missing.

| Module                    | Description                                       |
|---------------------------|---------------------------------------------------|
|`yokozuna`                 |Provides the Erlang API for other applications to call. |
|`yz_app`                   |Responsible for starting and stopping the Yokozuna application. |
|`yz_cover`                 |Builds the cover plan used at query time.  Ensures that query results don't include multiple replicas of same object.  This is needed for correct results. |
|`yz_doc`                   |Creates an Erlang data structure representing a Solr document.  Currently a list of `{Field::binary(), Value::binary()}` pairs |
|`yz_entropy`               |Iterates the entropy data stored in Solr.  Used to rebuild Yokozuna AAE hashtrees. |
|`yz_entropy_mgr`           |Manages the various AAE tasks such as building and exchanging hashtrees. |
|`yz_events`                |Track various events such as ring changes and Yokozuna index creation.  Events are how cluster state is disseminated. |
|`yz_exchange_fsm`          |Performs hashtree exchange between Yokozuna and KV.  Re-indexes objects that have missing or divergent indexes. |
|`yz_extractor`             |Determines mapping between the object content-type and extractor to use.  Extractors convert object value to list of field-value pairs to be indexed. |
|`yz_general_sup`           |Supervisor for all processes besides the Solr process. |
|`yz_index`                 |Administration of indexes such as creation, removal and listing. |
|`yz_index_hashtree`        |Keeps track of Yokozuna hashtree, one Erlang process per partition. |
|`yz_index_hashtree_sup`    |Supervisor for all the hashtree processes (`yz_index_hashtree`). |
|`yz_json_extractor`        |Extractor for JSON data (`application/json`). |
|`yz_kv`                    |Interface to Riak KV.  By funneling all calls to KV code through one module it's easier to refactor as KV changes. |
|`yz_misc`                  |Miscellaneous functions which have no obvious home.  E.g. determining the delta (difference) between two `ordsets()`. |
|`yz_noop_extractor`        |Default extractor when there is no mapping for object's content-type.  Produces no field-value pairs. |
|`yz_pb_admin`              |Handles administration requests sent via protocol buffers.  Things such as index creation and schema uploading. |
|`yz_pb_search`             |Handles search requests sent via protocol buffers.  Speaks the original Riak Search protobuff interface, not full Solr.  E.g. features like facets are still not supported via protobuffs, but available over HTTP. |
|`yz_schema`                |Schema administration such as fetching and uploading.  Also performs verification for existence of special fields.  These fields are required for Yokozuna to function properly.  They all start with `_yz`.  See the [default schema][ds].  |
|`yz_solr`                  | Functions for making requests to Solr via HTTP. |
|`yz_solr_proc`             |Overseer of external Solr JVM process.  If the JVM crashes this process will attempt to restart it.  It will also shutdown the JVM on Riak exit. |
|`yz_solr_sup`              |Supervisor for Solr process (`yz_solr_proc`). |
|`yz_stat`                  |Track statistics for various operations such as index and query latency. |
|`yz_stat_worker`           |Sidejob worker for updating stat counters. |
|`yz_sup`                   |Supervise the Yokozuna process tree. |
|`yz_text_extractor`        |Extractor for text data (`text/plain`). |
|`yz_wm_extract`            |HTTP resource for testing extractors.  Send data as you would a Riak Object and it returns JSON encoded field-value pairs. |
|`yz_wm_index`              |HTTP resource for index administration. |
|`yz_wm_schema`             |HTTP resource for schema administration. |
|`yz_wm_search`             |HTTP resource for querying.  Presents the same interface as a Solr server so that existing Solr clients may be used to query Yokozuna. |
|`yz_xml_extractor`         |Extractor for XML data (`application/xml`). |

Process Tree
------------

The following is the entire Yokozuna process tree.

![Yokozuna Process Tree](http://data.riakcs.net:8080/yokozuna/yz-tree.png)

Write Path (Index Path)
-----------------------

### Pseudo Steps ###

1. Client request arrives via Webmachine (HTTP) or riak_api (Protobuffs) processes.

2. A new put coordinator (`riak_kv_put_fsm`) is created.

3. The request is sent to 3 KV VNodes.

4. The KV VNode writes the object to disk.

5. The Yokozuna index hook is called (`yz_kv:index`).

6. Check if:
   a. Yokozuna is enabled
   b. Indexing capability is enabled
   c. The bucket has an associated index
   d. The node is NOT a fallback for the partition

7. Extract special fields from object:
   a. unique identifier (`_yz_id`), type + name + key + logical partition
   b. bucket type (`_yz_rt`)
   c. bucket name (`_yz_rb`)
   d. key (`_yz_rk`)
   e. entropy data (`_yz_ed`)
   f. logical partition number (`_yz_pn`)
   g. logical first partition number (`_yz_fpn`)
   h. node name (`_yz_node`)
   i. vtag (`_yz_vtag`)

8. Extract tags from object.

9. Extract set of field-value pairs for each sibling in the object. A union of special fields, tags, and field-value pairs create a document. Each sibling is its own document.

10. Convert each document from Erlang proplist to JSON binary. i.e. `[{Field :: binary(), Value :: binary()}]` => `<<"{\"field\":\"value\", ...}">>`

11. Send index operation to Solr via HTTP.

12. Update Yokozuna hashtree.


[ds]: https://github.com/basho/yokozuna/blob/develop/priv/default_schema.xml#L112
