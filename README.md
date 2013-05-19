Yokozuna [![travis](https://secure.travis-ci.org/basho/yokozuna.png)](http://travis-ci.org/basho/yokozuna)
==========

**NOTICE:** This is a prototype.  It is in developmental stages and is
  not recommended for production use at this time.  Breaking changes
  could be made on any commit.  After more testing and verification
  has been performed this will be considered as a replacement for Riak
  Search.

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

An _index_ must be created in order for Yokozuna to index data.

Currently the index name is a 1:1 mapping with the bucket name. This
may eventually change to a 1:M mapping from index to bucket.

You can create an index via the HTTP interface.

    curl -XPUT -i -H 'content-type: application/json' http://localhost:10018/yz/index/name_of_index

Optionally, you may create an index from the console.

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

[INSTALL][]: https://github.com/basho/yokozuna/blob/master/docs/INSTALL.md
