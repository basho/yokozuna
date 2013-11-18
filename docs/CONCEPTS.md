Yokozuna Concepts
==========

This document goes over important concepts in Yokozuna.  This will not
cover every detail of every concept.  Instead it is meant as an
overview of the most important concepts.

Yokozuna is an Erlang Application
---------------------------------

In Erlang OTP an "application" is a group of modules and Erlang
processes which together perform a specific task.  The word
application is confusing because most people think of an application
as an entire program such as Emacs or Photoshop.  But Yokozuna is just
a sub-system in Riak itself.  Erlang applications are often
stand-alone but Yokozuna is more like an appendage of Riak.  It
requires other subsystems like Riak Core and KV, but also extends
their functionality by providing search capabilities for KV data.

The point of Yokozuna is to bring more sophisticated and robust query
and search support to Riak.  Many people consider Lucene, and programs
built on top of it such as Solr, as _the standard_ for open source
search.  There are many successful stories built atop Lucene/Solr and
it sets the bar for the feature set developers and users expect.
Meanwhile, Riak has a great story as a highly-available, distributed,
key-value store.  Yokozuna takes advantage of the fact that Riak
already knows how to do the distributed bits, extending its feature
set with that of Solr's.  It takes the strength of each, and combines
them.

Yokozuna is a mediator between Riak and Solr.  There is nothing
stopping a user from deploying these two programs separately but then
they would be responsible for the glue.  That glue can be tricky to
write.  It needs to deal with monitoring, querying, indexing, and
dissemination of information.

Yokozuna knows how to listen for changes in KV data and make the
appropriate changes to indexes that live in Solr.

Yokozuna knows how to take a user query on any node and convert it to
a Solr Distributed Search which will correctly cover the entire index
without overlap in replicas.

Yokozuna knows how to take index creation commands and disseminate
that information across the cluster.

Yokozuna knows how to communicate and monitor the Solr OS process.

Etc.

Solr/JVM OS Process
-------------------

Every node in the Riak cluster has a corresponding operating system
(OS) process running a JVM which is hosting Solr on the Jetty
application server.  This OS process is a child of the Erlang OS
process running Riak.

Yokozuna has a `gen_server` process which monitors the JVM OS process.
The code for this server is in `yz_solr_proc`.  When the JVM process
crashes this server crashes, causing its supervisor to restart it.
If there is more than 1 restart in 45 seconds, the entire Riak
node will be shut down -- if Yokozuna is enabled and Solr cannot
function for some reason then the Riak node needs to go down so that
the user will notice and take corrective action.

Conversely, the JVM process monitors the Riak process.  If for any
reason Riak goes down hard (e.g. a segfault) the JVM process
will also exit.  This double monitoring along with the crash semantics
means that neither process may exist without the other.  They are either
both up or both down.

All other communication between Yokozuna and Solr is performed via
HTTP, including querying, indexing, and administration commands.
The ibrowse Erlang HTTP client is used to manage these communications
as both it and the Jetty container hosting Solr pool HTTP connections,
allowing for reuse. Moreover, since there is no `gen_server` involved
in this communication, there's no serialization point to bottleneck.

Indexes
----------

An index, stored as a set of files on disk, is a logical namespace
that contains index entries for objects. Each such index maintains
its own set of files on disk -- a critical difference from Riak KV in
which a bucket is a purely logical entity, and not physically disjoint
at all.

Indices may be associated with zero or more buckets. At creation time,
however, each index has no associated buckets -- unlike Riak Search
Yokozuna indices do not implicitly create bucket associations, meaning
that this must be done as a separate configuration step.

To associate a bucket with an index the bucket property `yz_index`
must be set to the name of the index you wish to associate.
Conversely, in order to disassociate a bucket you use the sentinel
value `_dont_index_`.

Many buckets can be associated with the same index.  This is useful
for logically partitioning data into different KV buckets which are of
the same type of data, for example if a user wanted to store event objects
but logically partition them in KV by using a date as the bucket name.

A bucket CANNOT be associated with many indexes -- the `yz_index`
property must be a single name, not a list.

See the [ADMIN][] page for details on creating an index.

[ADMIN]: https://github.com/basho/yokozuna/blob/master/docs/ADMIN.md

Extractors
----------

There is a tension between Riak KV and Solr when it comes to data --
Riak KV treats object values as opaque.  While KV does maintain an
associated content-type, it is simply treated as metadata to be
returned to the user to provide context for interpreting the returned
object; otherwise the user wouldn't know what type of data it is!
Solr, on the other hand, wants semi-structured data; specifically a
flat collection of field-value pairs.  "Flat" here means that a
field's value cannot be a nested structure of field-value pairs, the
values are treated as-is (non-composite is another way to say it).

Because of this mismatch between KV and Solr, Yokozuna must act as a
mediator between the two: it must have a way to inspect a KV object
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
is a table of this default mapping:

|Content Type          |Erlang Module          |
|----------------------|-----------------------|
|application/json      |`yz_json_extractor`    |
|application/xml       |`yz_xml_extractor`     |
|text/plain            |`yz_text_extractor`    |
|text/xml              |`yz_xml_extractor`     |
|N/A                   |`yz_noop_extractor`    |

The answer to the second question is a function of the implementation
of the extractor module.  Every extractor must conform to the
following Erlang specification:

```erlang
-spec(ObjectValue::binary(), Options::proplist()) -> fields() | {error, term()}.
-type field_name() :: atom() | binary().
-type field_value() :: binary().
-type fields() :: [{field_name(), field_value()}]
```

The value of the object is passed along with options specific to each
extractor.  Assuming the extractor correctly parses the value it will
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
with a separator.  The default separator is `.`.  An example should
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
[{<<"info.visited">>,<<"San Francisco">>},
 {<<"info.visited">>,<<"New York">>},
 {<<"info.visited">>,<<"Boston">>},
 {<<"info.city">>,<<"Baltimore">>},
 {<<"name">>,<<"ryan">>}]
```

Some key points to notice.

* Nested objects have their field names concatenated to form a field
  name.  The default field separator is `.`.  This can be modified.

* Any array causes field names to repeat.  This will require that your
  schema defines this field as multi-valued.

The XML extractor works in very similar fashion to the JSON extractor
except it also has element attributes to worry about.  To see the
document created for an object, without actually writing the object,
you can use the extract HTTP endpoint.  This will do a dry-run
extraction and return the document structure as `application/json`.

```
curl -XPUT -H 'content-type: application/json' \
  'http://localhost:8098/yz/extract' --data-binary @object.json
```

Schemas
----------

Every index must have a schema, which is a collection of field
names and types.  For each document stored, every field must have a
matching name in the schema, used to determine the field's type, which
in turn determines how a field's value will be indexed.

Currently, Yokozuna makes no attempts to hide any details of the Solr
schema: a user creates a schema for Yokozuna just as she would for
Solr.  Here is the general structure of a schema.


```xml
<?xml version="1.0" encoding="UTF-8" ?>
<schema name="my-schema" version="1.5">
  <fields>
    <!-- field definitions go here -->
  </fields>

  <!-- DO NOT CHANGE THIS -->
  <uniqueKey>_yz_id</uniqueKey>

  <types>
    <!-- field type definitions go here -->
  </types>
</schema>
```

The `<fields>` element is where the field name, type, and overriding
options are declared.  Here is an example of a field for indexing dates.

```xml
<field name="created" type="date" indexed="true" stored="true"/>
```

The corresponding date type is declared under `<types>` like so.

```xml
<fieldType name="date" class="solr.TrieDateField" precisionStep="0" positionIncrementGap="0"/>
```

For a complete reference of the Solr schema, the included types, and
analyzers, refer to the [Solr 4.4 reference guide][solr440-ref].

Yokozuna comes bundled with a [default schema][ds] called
`_yz_default`.  This is an extremely general schema which makes heavy
use of dynamic fields -- it is intended for development and
testing.  In production, a schema should be tailored to the data being
indexed.

AAE
----------

Active Anti-Entropy (AAE) is the process of discovering and correcting
entropy (divergence) between the data stored in Riak's key-value
backend and the indexes stored in Solr.  The impetus for AAE is that
failures come in all shapes and sizes -- disk failure, dropped
messages, network partitions, timeouts, overflowing queues, segment
faults, power outages, etc.  Failures range from obvious to
invisible. Failure prevention is fraught with failure, as well. How do
you prevent your prevention system from failing?  You don't. Code for
detection, not prevention.  That is the purpose of AAE.

Constantly reading and re-indexing every object in Riak could be quite
expensive.  To minimize the overall cost of detection AAE make use of
hashtrees.  Every partition has a pair of hashtrees; one for KV and
another for Yokozuna.  As data is written the hashtrees are updated in
real-time.

Each tree stores the hash of the object.  Periodically a partition is
selected and the pair of hashtrees is _exchanged_.  First the root
hashes are compared.  If equal then there is no more work to do.  You
could have millions of keys in one partition and verifying they
**ALL** agree takes the same time as comparing two hashes.  If they
don't match then the root's children are checked and this process
continues until the individual discrepancies are found.  If either
side is missing a key or the hashes for a key do not match then
_repair_ is invoked on that key.  Repair converges the KV data and its
indexes, removing the entropy.

Since failure is inevitable, and absolute prevention impossible, the
hashtrees themselves may contain some entropy. For example, what if
the root hashes agree but a divergence exists in the actual data?
Simple, you assume you can never fully trust the hashtrees so
periodically you _expire_ them.  When expired, a tree is completely
destroyed and the re-built from scratch.  This requires folding all
data for a partition, which can be expensive and take some time.  For
this reason, by default, expiration occurs after one week.

For an in-depth look at Riak's AAE process watch Joseph Blomstedt's
[screencast][aae-sc].

Analysis & Analyzers
--------------------

Analysis is the process of breaking apart (analyzing) text into a
stream of tokens.  Solr allows many different methods of analysis,
an important fact because different field values may represent
different types of data.  For data like unique identifiers, dates, and
categories you want to index the value verbatim -- it shouldn't be
analyzed at all.  For text like product summaries, or a blog post,
you want to split the value into individual words so that they may be
queried individually.  You may also want to remove common words,
lowercase words, or perform stemming.  This is the process of
_analysis_.

Solr provides many different field types which analyze data in
different ways, and custom analyzer chains may be built by stringing
together XML in the schema file, allowing custom analysis for each
field.  For more information on analysis see the
[Solr 4.4 reference guide][solr440-ref].

Tagging
-------

Tagging is the process of adding field-value pairs to be indexed via
Riak Object metadata.  It is useful in two scenarios.

1. The object being stored is opaque but your application has metadata
   about it that should be indexed, for example storing an image with
   location or category metadata.

2. The object being stored is not opaque, but additional indexes must
   be added _without_ modifying the object's value.

See [TAGGING][] for more information.

Coverage
--------

Yokozuna uses _doc-based partitioning_.  This means that all index
entries for a given Riak Object are co-located on the same physical
machine. To query the entire index all partitions must be
contacted. Adjacent partitions keep replicas of the same object.
Replication allows the entire index to be considered by only
contacting a subset of the partitions. The process of finding a
covering set of partitions is known as _coverage_.

Each partition in the coverage plan has an owning node. Thus a plan
can be thought of as a unique set of nodes along with a covering set
of partitions. Yokozuna treats the node list as physical hostnames and
passes them to Solr's distributed search via the `shards`
parameter. Partitions, on the other hand, are treated logically in
Yokozuna. All partitions for a given node are stored in the same
index; unlike KV which uses _partition_ as a physical separation. To
properly filter out overlapping replicas the partition data from the
cover plan is passed to Solr via the filter query (`fq`) parameter.

Calculating a coverage plan is handled by Riak Core. It can be a very
expensive operation as much computation is done symbolically, and the
process amounts to a knapsack problem.  The larger the ring the more
expensive.  Yokozuna takes advantage of the fact that it has no
physical partitions by computing a coverage plan asynchronously every
few seconds, caching the plan for query use.  In the case of node
failure or ownership change this could mean a delay between cluster
state and the cached plan.  This is, however, a good trade-off given
the performance benefits, especially since even without caching there
is a race, albeit one with a smaller window.

[aae-sc]: http://coffee.jtuple.com/video/AAE.html

[ds]: https://github.com/basho/yokozuna/blob/v0.9.0/priv/default_schema.xml

[solr440-ref]: http://archive.apache.org/dist/lucene/solr/ref-guide/apache-solr-ref-guide-4.4.pdf

[TAGGING]: https://github.com/basho/yokozuna/blob/develop/docs/TAGGING.md
