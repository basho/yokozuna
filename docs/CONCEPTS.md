Yokozuna Concepts
==========

This document goes over important concepts in Yokozuna.  This will not
cover every detail of every concept.  Instead it is meant as an
overview of the most important concepts.

Index
----------

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

Extractors
----------

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

Schemas
----------

Every index must have a schema.  The schema is a collection of field
names and types.  For each document stored, every field must have a
matching name in the schema.  This name is used to determine the type.
The type information determines how a field's value will be indexed.

Currently, Yokozuna makes no attempts to hide any details of the Solr
schema.  A user creates a schema for Yokozuna just as she would for
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

For a complete reference of the Solr schema, its included types, and
analyzers refer to the [Solr 4.4 reference guide][solr440-ref].

Yokozuna comes bundled with a [default schema][ds] called
`_yz_default`.  This is an extremely general schema which makes heavy
use of dynamic fields.  It is intended for development purposes and
testing.  In production a schema should be tailored to the data being
indexed.

AAE
----------

Active Anti-Entropy (AAE) is the process of discovering and correcting
entropy (divergence) between the data stored in Riak's key-value
backend and the indexes stored in Solr.  The impetus for AAE is that
failures come in all types.  Disk failure, dropped messages, network
partitions, timeouts, overflowing queues, segment faults, power
outages, etc.  Failures range from obvious to invisible.  Prevention
of failures is fraught with failures as well.  How do you prevent your
prevention system from failing?  You don't.  Code for detection, not
prevention.  That is the purpose of AAE.

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

As I said, failure is immanent and absolute prevention impossible.  So
what if the hashtrees themselves contain entropy?  E.g. what if the
root hashes agree but a divergence exists in the actual data?  Simple,
you assume you can never fully trust the hashtrees so periodically
you _expire_ them.  When expired a tree is completely destroyed and
the re-built from scratch.  This requires folding all data for a
partition.  It can be expensive and take some time.  This is why, by
default, expiration occurs after a week.

For an in-depth look at Riak's AAE process watch Joseph Blomstedt's
[screencast][aae-sc].

Analysis & Analyzers
--------------------

Analysis is the process of breaking apart (analyzing) text into a
stream of tokens.  Solr allows many different methods of analysis.
This is important because different field values may represent
different types of data.  For data like unique identifiers, dates, and
categories you want to index the value verbatim.  It shouldn't be
analyzed at all.  For text like product summaries or a blog post
you want to split the value into individual words so that they may be
queried individually.  You may also want to remove common words,
lowercase words, or performing stemming.  This is the process of
_analysis_.

Solr provides many different field types which analyze data in
different ways.  Custom analyzer chains may be built by stringing
together XML in the schema file.  This allows custom analysis for each
field.  For more information on analysis see the
[Solr 4.4 reference guide][solr440-ref].

Tagging
-------

Tagging is the process of adding field-value pairs to be indexed via
the Riak Object metadata.  It is useful in two scenarios.

1. The object being stored is opaque but your application has metadata
   about it that should be indexed.  E.g. storing an image with
   location or category metadata.

2. The object being stored is not opaque but additional indexes must
   be added without modifying the object's value.

See [TAGGING][] for more information.


TODO: Events?, Coverage

[aae-sc]: http://coffee.jtuple.com/video/AAE.html

[ds]: https://github.com/basho/yokozuna/blob/v0.9.0/priv/default_schema.xml

[solr440-ref]: http://archive.apache.org/dist/lucene/solr/ref-guide/apache-solr-ref-guide-4.4.pdf

[TAGGING]: https://github.com/basho/yokozuna/blob/develop/docs/TAGGING.md
