Yokozuna [![travis](https://secure.travis-ci.org/rzezeski/yokozuna.png)](http://travis-ci.org/rzezeski/yokozuna)
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

Since Yokozuna is a prototype it takes more work to get running than
vanilla Riak.  If you are comfortable building Riak from source then
you should be able to complete these steps.

### Building ###

1. Clone my fork of Riak's Yokozuna branch

        git clone -b rz-yokozuna-2 git://github.com/basho/riak.git
        cd riak

2. Verify that you have ant (>= 1.8.2) installed. It is needed to build solr.

        ant -version

3. Setup and compile yokozuna. Setup will replace riak_kv with
   the correct YZ branch. Compile will take a long time because
   it must clone Solr source and build it.  The first time you do
   this is the longest as Ivy may have to download all the
   dependencies if you don't regularly develop on Java projects.

        make yz-setup

4. Make a stage rel or stage devrel.  At this point it's no different
   from building a vanilla Riak release.

        make stagedevrel

### Running ###

Yokozuna is merely an extension to Riak.  This means that the basics
of running and administrating Riak are no different than a vanilla
build.  The following instructions assume a devrel.

1. Start the nodes.  After this has complete `ps` should show 4
   `beam.smp` processes and 4 `java` processes.  The Solr instances
   should be listening on ports 7981-7984.

        for d in dev/dev*; do $d/bin/riak start; done
        for d in dev/dev*; do $d/bin/riak ping; done

2. Join the nodes.  I am using the force (`-f`) option here as this is
   just a development release.  In a production environment you should
   use the new cluster staging commands as they are more efficient
   when doing multiple operations and can save you from making costly
   mistakes.

        for d in dev/dev{2,3,4}; do $d/bin/riak-admin join -f dev1@127.0.0.1; done

### Creating an Index ###

An _index_ must be created in order for Yokozuna to index data.

Currently the index name is a 1:1 mapping with the bucket name. This
may eventually change to a 1:M mapping from index to bucket.

You can create an index via the HTTP interface.

    curl -XPUT -i -H 'content-type: application/json' http://localhost:8091/yz/index/name_of_index

Optionally, you may create an index from the console.

### Index Some Data ###

Indexing data is a matter of writing data to KV.  Currently Yokozuna
knows hows to index plain text, XML, and JSON.

    curl -H 'content-type: text/plain' -X PUT 'http://localhost:8091/buckets/name_of_bucket/keys/name' -d "Ryan Zezeski"

### Searching ###

The syntax for querying Yokozuna is the same as querying a single
instance of Solr.  Yokozuna actually uses Solr's distributed search
API but that is hidden for you.  This means you don't have to worry
about where your shards are located.  This also means you should be
able to use any off-the-shelf Solr client to query Yokozuna.

    curl 'http://localhost:8091/search/name_of_index?q=text:Ryan'
