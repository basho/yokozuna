Administration
==========

## Index Creation

Before Riak data may be indexed an _index_ must be created.
The easiest way to accomplish this is to use the HTTP index
resource.

**NOTE:** Currently the index name is a 1:1 mapping with a KV bucket
          name. This may eventually change to a 1:M mapping from index
          to bucket.

### HTTP Index Admin

To create a new index, PUT to `/search/index` path, suffixed with your
index name, with a content type of `application/json`.  The JSON
content itself is optional and allows specifying a schema to use
besides the default schema.

```bash
curl -i -XPUT http://localhost:8098/search/index/my_bucket \
  -H 'content-type: application/json' \
  -d '{"schema":"my_schema"}'
```

A `204 No Content` should be returned if successful, or a `409 Conflict` code if the index already exists.

To get information about the index, issue a GET request to the same URL.

```bash
curl http://localhost:8098/search/index/my_bucket | jsonpp
{
  "name":"my_bucket",
  "bucket":"my_bucket",
  "schema":"my_schema"
}
```

If you leave off the index name from the GET request, all installed
indexes will be installed as a JSON array.

Finally, when you are done with the index, you can issue a DELETE
method with an index name to remove the index.

```bash
curl -XDELETE http://localhost:8098/search/index/my_bucket
```
