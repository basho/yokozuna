Yokozuna
==========

**NOTICE:** Yokozuna is in alpha stage.  It is under active
  development.  Breaking changes could occur at any moment.  In a
  future release it will replace Riak Search.

_Yokozuna_ - Horizontal rope.  The top rank in sumo, usually
translated _Grand Champion_.  The name comes from the rope a yokozuna
wears.

The goal of the yokozuna application is to integrate Apache Solr with
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

    curl -XPUT -i 'http://localhost:10018/yz/index/my_index'

### Associating a Bucket with an Index ###

A bucket property must be added to tell Yokozuna where to store the
indexes for a given bucket.

    curl -XPUT -i -H 'content-type: application/json' 'http://localhost:10018/buckets/my_bucket/props' -d '{"props":{"yz_index":"my_index"}}'

### Index Some Data ###

Indexing data is a matter of writing data to KV.  Currently Yokozuna
knows hows to index plain text, XML, and JSON.

    curl -H 'content-type: text/plain' -X PUT 'http://localhost:10018/buckets/name_of_bucket/keys/name' -d "Ryan Zezeski"

### Searching ###

The syntax for querying Yokozuna is the same as querying a single
instance of Solr.  Yokozuna actually uses Solr's distributed search
API but that is hidden for you.  This means you don't have to worry
about where your shards are located.  This also means you should be
able to use any off-the-shelf Solr client to query Yokozuna.

    curl 'http://localhost:10018/search/name_of_index?q=text:Ryan'

[INSTALL]: https://github.com/basho/yokozuna/blob/master/docs/INSTALL.md
