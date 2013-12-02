Administration
==========

## Index Creation

Before Riak data may be indexed an _index_ must be created.
The easiest way to accomplish this is to use the HTTP index
resource.

### HTTP Index Admin

To create a new index, PUT to `/search/index` path, suffixed with your
index name, with a content type of `application/json`.  The JSON
content itself is optional and allows specifying a schema to use
besides the default schema.

```bash
curl -i -XPUT http://localhost:8098/search/index/my_index \
  -H 'content-type: application/json' \
  -d '{"schema":"my_schema"}'
```

A `204 No Content` should be returned if successful, or a `409
Conflict` code if the index already exists.

To get information about the index, issue a GET request to the same URL.

```bash
curl http://localhost:8098/search/index/my_index | jsonpp
{
  "name":"my_index",
  "schema":"my_schema"
}
```

If you leave off the index name from the GET request, all installed
indexes will be installed as a JSON array.

Finally, when you are done with the index, you can issue a DELETE
method with an index name to remove the index.

```bash
curl -XDELETE http://localhost:8098/search/index/my_index
```

Index Association
-----------------

In order for bucket data to be indexed an index must be associated
with it. An index may be associated at the type or bucket level. If
applied at the type level all buckets under that type will inherit the
index association. If set at the bucket level the association applies
only to that bucket. If applied at both levels then the bucket-level
association will override. There are two main patterns for structuring
index association: one-to-one and one-to-many.

### One-to-One ###

In this structure each bucket has it's own index. This should be used
when the data in each bucket is different and can't share a common
schema. It's also useful for physical separation of indexes as each
index maps to its own Solr Core. This potentially allows lower
latency queries and efficient deletion of index data. Here's an
example of creating a data type with two buckets containing data
different enough that they warrant separate indexes.

**N.B.**: Currently bucket-type create takes JSON on command line but
  that is slated to change by the official Riak 2.0 release.

```
riak-admin bucket-type create data '{"props":{}}'

riak-admin bucket-type activate data

curl -XPUT -H 'content-type: application/json' 'http://localhost:10018/search/index/people -d '{"schema":"people.xml"}'

curl -XPUT -H 'content-type: application/json' 'http://localhost:10018/search/index/events -d '{"schema":"events.xml"}'

curl -XPUT -H 'content-type: application/json' 'http://localhost:10018/types/data/buckets/people/props' -d '{"props":{"search_index":"people"}}'

curl -XPUT -H 'content-type: application/json' 'http://localhost:10018/types/data/buckets/events/props' -d '{"props":{"search_index":"events"}}'
```

### Many-to-One ###

In this structure each bucket inherits the index associated to its
type. Thus each bucket shares the same index. This structure should be
used when the bucket data is the same but a logical separation in the
Riak key-space is desired. Here is an example where the `people` type
groups people under a specific bucket based on type.

```
riak-admin bucket-type create people '{"props":{"search_index":"people"}}'

riak-admin bucket-type activate people

curl -XPUT -H 'content-type: application/json' 'http://localhost:10018/search/index/people -d '{"schema":"people.xml"}'

curl ... 'http://localhost:10018/types/people/buckets/maryland/keys/ryan_zezeski' ...
curl ... 'http://localhost:10018/types/people/buckets/oregon/keys/eric_redmond' ...
```
