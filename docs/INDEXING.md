Indexing
==========

To use Yokozuna with Riak, you're required to first create an index. There
are two methods for achieving that: by executing erlang code on a running
Riak cluster, or by using the HTTP Index API.

A second step is to connect a Riak bucket to that index, by attaching a
post-commit hook that populates the index when a write occurs.

Currently the index name is a 1:1 mapping with the bucket name. This
may eventually change to a 1:M mapping from index to bucket.


## Manual Index Install

Once your Riak/Yokozuna cluster is installed and running, attach a console
to one of the nodes.

```bash
bin/riak attach
```

In this example, because of the 1:1 (index:bucket) mapping, we assume
that `my_index` equals `my_bucket`.

First, you need to create the index, which internally, creates a
Solr index with that name.

```bash
yz_index:create("my_index").
```

If you wish to use a different schema (the default is "_yz_default"),
you can add it as a second parameter. That schema must be installed
as a solr xml file

```bash
yz_index:create("my_index", <<"my_schema">>).
```

In order for all Riak writes to populate the index, you must also install
a postcommit hook.

```bash
yz_kv:install_hook(<<"my_bucket">>).
```

You also have the option to instead us the HTTP Index API.


## HTTP Index Install

This is a simpler resource for managing Yokozuna indexes over HTTP.

As mentioned above, there is currently a 1:1 mapping between the index name
and bucket name. Also, all HTTP Index commands are based on the `/yz/index`
url path.

To create a new index, PUT to `/yz/index` path, suffixed with your bucket/index
name. You can optionally pass in some JSON content to specify a schema name.
The schema defaults to `_yz_default`, but you can reference any solr schema xml
file you have installed.

A PUT also requires a `Content-Type: application/json` header.

```bash
curl -XPUT http://localhost:8098/yz/index/my_bucket \
  -H 'content-type: application/json' \
  -d '{"schema":"my_schema"}'
```

A `204 No Content` should be returned if successful, or a `409 Conflict` code if the index already exists.

To get information about the index, issue a GET request to the same url.

```bash
curl http://localhost:8098/yz/index/my_bucket | jsonpp
{
  "name":"my_bucket",
  "bucket":"my_bucket",
  "schema":"my_schema"
}
```

If you leave off the index name from the GET request, all installed indexes
will be installed as a JSON array.

Finally, when you are done with the index, you can issue a DELETE method
with an index name. This will both remove the index, as well as remove
the postcommit hook from the bucket.

```bash
curl -XDELETE http://localhost:8098/yz/index/my_bucket
```


## Testing the Index

With the index set up, it's time to test it.

```bash
curl -XPUT 'http://localhost:8098/riak/my_bucket/name' \
  -H 'content-type: text/plain' \
  -d "Ryan Zezeski"
```
