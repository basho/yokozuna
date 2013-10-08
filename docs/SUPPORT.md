Supporting Yokozuna
===================

This document is meant for those supporting Yokozuna in a production
environment.  It attempts to go over various facets of Yokozuna which
might be of interest to someone diagnosing problems.  It should also
be useful to developers.

Limitations
----------

Various limitations of Yokozuna and Solr to be aware of.

### Ring Size ###

Yokozuna currently has issues with larger rings.  All benchmarks and
testing to this day has been done on a ring size of 64 or less.  Ring
sizes of 128 or 256 **MAY** work but at some point queries will just
flat out fail.

This is due to filtering that Yokozuna needs to perform to avoid
overlapping replicas.  It passes a filter query to Solr via a query
string in the URL of the HTTP request.  The more partitions the longer
the URL becomes and eventually it becomes too long and the Jetty
container rejects the request.  Yokozuna uses logical partition
numbers which helps but there is a limit.

A simple solution is to sent the query to Solr via HTTP POST.  This
moves the `fq` param into the request body, bypassing the URL length
limit.

An even better solution would be to remove the filter query
altogether.  This is much harder to accomplish as it requires creating
disjoint Solr cores.  E.g. for every index you could create a core per
partition like `<index_name>_<partition num>` but this would greatly
increase the number of Solr cores and would require querying `1/N *
RING_SIZE` shards.

### Cluster Size ###

Yokozuna uses _doc-based partitioning_.  To query a complete index it
requires a coverage query.  The larger the cluster the more nodes that
must be considered.  This makes Yokozuna suspect for "larger" clusters
for some definition of larger.  There are two main questions.

1. What does the function of cluster size to latency look like?

2. What is the typical cluster size where Yokozuna query latency just
becomes untenable?

The answers to both these questions is going to depend on the amount
of data and the type of query.  But, given the same query and same
amount of data you can compare performance of various cluster sizes.

To this day, the largest cluster Yokozuna has been tested against was
8 nodes.  There is little to no idea what the latency function looks
like as size increases.  The max cluster size is also unknown.

There is some low-hanging fruit in this area.  Yokozuna makes use of
Riak Core's coverage planner.  This planner is currently hard-coded to
optimize for the lest amount of partitions/vnodes.  The thinking is
that less vnodes means faster 2i & list-keys operations (I'm not
convinced).  Yokozuna has no physical partitions or vnodes.  This
behavior actively works against it.  Optimizing for the least amount
of nodes would reduce latency.  This doesn't solve the real problem
that doc-based is harder to scale but it gives more headroom and
raises the maximum cluster size limit.

### Deep Pagination of Large Result Sets ###

Solr currently has issue paginating deep into a large result set.  Use
cases like paging through 1 million results may have latency and
memory issues.  The deeper you page into the result set the more
expensive the query becomes.

If deep pagination must absolutely be done then page size should be
experimented with.  For page size `S` Solr will request `S` results
from each node.  If 8 nodes are queried with page size of 1000 then
8000 results will returned to the coordinator in the first stage of the
distributed query.  Experimenting with page sizes between 10-10,000 is
recommended.

The following issue is tracking proposals for improving deep
pagination in Solr.

https://issues.apache.org/jira/browse/SOLR-1726

Statistics
----------

Yokozuna has some basic statistics that may be used to quickly assess
overall index and query health.

|Name                    |Summary                                        |
|------------------------|-----------------------------------------------|
| index/fail             | The number of index failures in last 60 seconds and total since Riak started. |
| index/latency          | Index latency -- from extraction of bucket-key to return from Solr. |
| index/throughput       | The number of successful index operations in last 60 seconds and total since Riak started. |
| search/fail            | The number of search failures in last 60s and total since Riak started. |
| search/latency         | Search latency.  For protobuff this includes everything after parameter extraction from the request.  For HTTP it starts once the request has been deemed valid. |
| search/throughput      | The number of successful search operations in last 60 seconds and total since Riak started. |


Module Index
------------

A one line summary of every module.  This table may lag behind the
code.  Please file an issue if you notice modules missing.

<table>
  <tr>
    <th>Module</th>
    <th>Summary</th>
  </tr>
  <tr>
    <td>yokozuna</td>
    <td>Provides the Erlang API for other applications to call.</td>
  </tr>

  <tr>
    <td>yz_app</td>
    <td>Responsible for starting and stopping the Yokozuna
    application</td>
  </tr>

  <tr>
    <td>yz_cover</td>
    <td>Builds the cover plan used at query time.  Ensures that query results don't include multiple replicas of same object.  This is needed for correct results.</td>
  </tr>

  <tr>
    <td>yz_doc</td>
    <td>Creates an Erlang data structure representing a Solr document.  Currently a list of `{Field::binary(), Value::binary()}` pairs</td>
  </tr>

  <tr>
    <td>yz_entropy</td>
    <td>Iterates the entropy data stored in Solr.  Used to rebuild Yokozuna AAE hashtrees.</td>
  </tr>

  <tr>
    <td>yz_entropy_mgr</td>
    <td>Manages the various AAE tasks such as building and exchanging hashtrees.</td>
  </tr>

  <tr>
    <td>yz_events</td>
    <td>Track various events such as ring changes and Yokozuna index creation.  Events are how cluster state is disseminated.</td>
  </tr>

  <tr>
    <td>yz_exchange_fsm</td>
    <td>Performs hashtree exchange between Yokozuna and KV.  Re-indexes objects that have missing or divergent indexes</td>
  </tr>

  <tr>
    <td>yz_extractor</td>
    <td>Determines mapping between the object content-type and extractor to use.  Extractors convert object value to list of field-value pairs to be indexed.</td>
  </tr>

  <tr>
    <td>yz_index</td>
    <td>Administration of indexes such as creation, removal and listing.</td>
  </tr>

  <tr>
    <td>yz_index_hashtree</td>
    <td>Keeps track of Yokozuna hashtree, one Erlang process per partition.</td>
  </tr>

  <tr>
    <td>yz_json_extractor</td>
    <td>Extract for JSON data (application/json).</td>
  </tr>

  <tr>
    <td>yz_kv</td>
    <td>Interface to Riak KV.  By funneling all calls to KV code through one module it's easier to refactor as KV changes.</td>
  </tr>

  <tr>
    <td>yz_misc</td>
    <td>Miscellaneous functions which have no obvious home.  E.g. determining the delta (difference) between two `ordsets()`.</td>
  </tr>

  <tr>
    <td>yz_noop_extractor</td>
    <td>Default extractor when there is no mapping for object's content-type.  Produces no field-value pairs.</td>
  </tr>

  <tr>
    <td>yz_pb_admin</td>
    <td>Handles administration requests sent via protocol buffers.  Things such as index creation and schema uploading</td>
  <tr>

  <tr>
    <td>yz_pb_search</td>
    <td>Handles search requests sent via protocol buffers.  Speaks the original Riak Search protobuff interface, not full Solr.  E.g. features like facets are still not supported via protobuffs, but available over HTTP.</td>
  </tr>

  <tr>
    <td>yz_schema</td>
    <td>Schema administration such as fetching and uploading.  Also performs verification for existence of special fields.  These fields are required for Yokozuna to function properly.  They all start with `_yz`.  See the <a href="https://github.com/basho/yokozuna/blob/develop/priv/default_schema.xml">default schema</a>.</td>
  </tr>

  <tr>
    <td>yz_solr</td>
    <td>Functions for making requests to Solr via HTTP.
  </tr>

  <tr>
    <td>yz_solr_proc</td>
    <td>Overseer of external Solr JVM process.  If the JVM crashes this process will attempt to restart it.  It will also shutdown the JVM on Riak exit.</td>
  </tr>

  <tr>
    <td>yz_stat</td>
    <td>Track statistics for various operations such as index and query latency</td>
  </tr>

  <tr>
    <td>yz_sup</td>
    <td>Supervise the Yokozuna process tree.</td>
  </tr>

  <tr>
    <td>yz_text_extractor</td>
    <td>Extractor for text data (text/plain).</td>
  </tr>

  <tr>
    <td>yz_wm_extract</td>
    <td>HTTP resource for testing extractors.  Send data as you would a Riak Object and it returns JSON encoded field-value pairs.</td>
  </tr>

  <tr>
    <td>yz_wm_index</td>
    <td>HTTP resource for index administration</td>
  </tr>

  <tr>
    <td>yz_wm_schema</td>
    <td>HTTP resource for schema administration</td>
  </tr>

  <tr>
    <td>yz_wm_search</td>
    <td>HTTP resource for querying.  Presents the same interface as a Solr server so that existing Solr clients may be used to query Yokozuna.</td>
  </tr>

  <tr>
    <td>yz_xml_extractor</td>
    <td>Extractor for XML data (application/xml).</td>
  </tr>
</table>

[ds]: https://github.com/basho/yokozuna/blob/develop/priv/default_schema.xml#L112
