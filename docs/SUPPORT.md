Supporting Yokozuna
===================

This document is meant for those supporting Yokozuna in a production
environment.  It attempts to go over various facets of Yokozuna which
might be of interest to someone diagnosing problems.  It should also
be useful to developers.

Important Concepts
----------------

### Index ###

An index is both a physical entity and logical namespace.  It acts as
a logical container to store index entries for objects.  Each index is
comprised of its own set of files on disk.  In contrast to Riak KV,
where a _bucket_ is logical only.

Each index may have 0, 1 or more buckets associated with it.  Unlike
Riak Search, creating an index does not implicitly associate a bucket
with it.  This must be done as a second step.  Thus, all indexes start
with 0 associated buckets.

To associate a bucket with an index the bucket property `yz_index` must
be set.  It's value should be the name of the index you wish to
associate.  In order to disassociate a bucket you use the sentinel
value `_dont_index_`.

Many buckets can be associated with the same index.  This is useful
for logically partitioning data into different KV buckets which are of
the same type of data.  E.g. if a user wanted to store event objects
but logically partition them in KV by using a date as the bucket name.

A bucket CANNOT be associated with many indexes.  I.e. the `yz_index`
property must be a single name, not a list.

See the [ADMIN][] page for details on creating an index.

[ADMIN]: https://github.com/basho/yokozuna/blob/master/docs/ADMIN.md

### Extractors ###

There is a tension between Riak KV and Solr when it comes to data.
Riak KV treats object values as opaque.  There is an associated
content-type which is simply treated as metadata to be returned to the
user.  Otherwise the user wouldn't know what type of data it is!
Solr, on the other hand, wants semi-structure data.  More specifically
it wants a flat collection of field-value pairs.  "Flat" means that a
field's value cannot be a nested structure of field-value pairs.  The
values are treated as-is (non-composite is another way to say it).

Because of this mismatch between KV and Solr, Yokozuna must act as a
mediator between the two.  It must have a way to inspect a KV object
and create a structure which Solr can ingest for indexing.  In Solr
this structure is called a _document_.  This task of creating a Solr
document from a Riak object is the job of the _extractor_.  To perform
this task two things must be considered.

NOTE: This isn't quite right, the fields created by the extractor are
only a subset of the fields created.  Special fields needed for
Yokozuna to properly query data and tagging fields are also created.
This call happens inside `yz_doc:make_doc`.

1. Does an extractor exist to map the content-type of the object to a
   Solr document?

2. If so, how is the object's value mapped from one to the other?
   E.g. the value may be `application/json` which contains nested
   objects.  This must somehow be transformed into a flat structure.

The first question is answered by the _extractor mapping_.  By default
Yokozuna ships with extractors for several common data types.  Below
is a table of this default mapping.

|Content Type          |Erlang Module          |
|----------------------|-----------------------|
|application/json      |`yz_json_extractor`    |
|application/xml       |`yz_xml_extractor`     |
|text/plain            |`yz_text_extractor`    |
|text/xml              |`yz_xml_extractor`     |
|N/A                   |`yz_noop_extractor`    |

The answer to the second question is a function of the implementation
of the extractor module.  Every extractor must conform to the
following Erlang specification.

```erlang
-spec(ObjectValue::binary(), Options::proplist()) -> fields() | {error, term()}.
-type field_name() :: atom() | binary().
-type field_value() :: binary().
-type fields() :: [{field_name(), field_value()}]
```

The value of the object is passed along with options specific to each
extractor.  Assuming the extractor correctly parses the value if will
return a list of fields, which are name-value pairs.

The text extractor is the simplest one.  By default it will use the
object's value verbatim and associate if with the field name `text`.
E.g. an object with the value "How much wood could a woodchuck chuck
if a woodchuck could chuck wood?" would result in the following fields
list.

```erlang
[{text, <<"How much wood could a woodchuck chuck if a woodchuck could chuck wood?">>}]
```

An object with the content type `application/json` is a little
trickier.  JSON can be nested arbitrarily.  That is, the key of a
top-level object can have an object as a value, and this object can
have another object nested inside, an so on.  Yokozuna's JSON
extractor must have some method of converting this arbitrary nesting
into a flat list.  It does this by concatenating nested object fields
with a separator.  The default separator is `_`.  An example should
make this more clear.

Below is JSON that represents a person, what city they are from and
what cities they have traveled to.

```javascript
{"name":"ryan",
 "info":{"city":"Baltimore",
         "visited":["Boston", "New York", "San Francisco"]}}
```

Below is the field list that would be created by the JSON extract.

```erlang
[{<<"info_visited">>,<<"San Francisco">>},
 {<<"info_visited">>,<<"New York">>},
 {<<"info_visited">>,<<"Boston">>},
 {<<"info_city">>,<<"Baltimore">>},
 {<<"name">>,<<"ryan">>}]
```

Some key points to notice.

* Nested objects have their field names concatenated to form a field
  name.  The default field separator is `_`.  This can be modified.

* Any array causes field names to repeat.  This will require that your
  schema defines this field as multi-valued.

The XML extractor works in very similar fashion to the JSON extractor
except it also has element attributes to worry about.  To see the
document created for an object, without actually writing the object,
you can use the extract HTTP endpoint.  This will do a dry-run
extraction and return the document structure as `application/json`.

```
curl -XPUT -H 'content-type: application/json' \
  'http://localhost:8098/extract' --data-binary @object.json
```

### Events ###

### AAE ###

### Schemas ###

Architecture
------------

This section gives an overview of Yokozuna's architecture.


Yokozuna is an Erlang application
---------------------------------

Yokozuna is an Erlang OTP "application".  In the same way that Riak
Core, Riak KV, and Webmachine are applications.  A collection of
processes cooperate to perform tasks such as 

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
