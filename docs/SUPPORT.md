Supporting Yokozuna
===================

This document is meant for those supporting Yokozuna in a production
environment.  It attempts to go over various facets of Yokozuna which
might be of interest to someone diagnosing problems.  It should also
be useful to developers.

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
    <td>yz_kv<td>
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
    <td>Schema administration such as fetching and uploading.  Also performs verification for existence of special fields.  These fields are required for Yokozuna to function properly.  They all start with `_yz`.  See the [default schema][ds].</td>
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
