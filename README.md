Yokozuna
==========

**NOTICE:** Yokozuna is in alpha stage.  It is under active
  development.  Breaking changes could occur at any moment.  In a
  future release it will replace Riak Search.

_Yokozuna_ - Horizontal rope.  The top rank in sumo, usually
translated _Grand Champion_.  The name comes from the rope a yokozuna
wears.

The goal of the Yokozuna application is to integrate Apache Solr with
Riak in order to find the "top rank" documents for a given query.


Getting Started
----------

### Installing ###

See the [INSTALL][] doc for installing Riak-Yokozuna and forming a
development cluster.

### Creating an Index ###

An _index_ must be created for bucket data to be indexed under.  Many
buckets may use the same index.  To create an index via the HTTP
interface the following.

```
curl -XPUT -i 'http://localhost:10018/search/index/my_index'
```

### Create Bucket Type, and Associate Index ###

**N.B.** This is a breaking change in the 0.12.0 version. Previously
associating an index was done by updating a bucket's properties.

A bucket type must be created and the `search_index` field must be set
either at the type-level properties, as shown here, or name-level.

```
riak-admin bucket-type create my_type '{"props":{"search_index":"my_index"}}'
riak-admin bucket-type activate my_type
```

### Index Some Data ###

Indexing data is a matter of writing data to KV.  Currently Yokozuna
knows hows to index plain text, XML, and JSON.

```
curl -H 'content-type: text/plain' -X PUT 'http://localhost:10018/types/my_type/buckets/my_bucket/keys/name' -d "Ryan Zezeski"
```

### Searching ###

The syntax for querying Yokozuna is the same as querying a single
instance of Solr.  Yokozuna actually uses Solr's distributed search
API but that is hidden for you.  This means you don't have to worry
about where your shards are located.

```
curl 'http://localhost:10018/search/my_index?q=text:Ryan'
```

The canonical Solr URL is also supported allowing the use of an
existing Solr client to query Yokozuna.

```
curl 'http://localhost:10018/solr/my_index/select?q=text:Ryan'
```

[INSTALL]: https://github.com/basho/yokozuna/blob/develop/docs/INSTALL.md
