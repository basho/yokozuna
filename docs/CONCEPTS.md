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

Every index has one and only one associated schema.  The schema
describes each field that may be stored in an index.  Most
importantly, it describes the name of the field and how it should be
analyzed and indexed.  Currently Yokozuna makes no attempts to hide
any details of the Solr schema.  That is, a user creates a schema for
Yokozuna just as she would for Solr.  It contains a set of fields, a
set of field types, and a unique key.

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

TODO: AAE, Analyzing, Events?, Tags, Coverage
