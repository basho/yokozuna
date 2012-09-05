Yokozuna
==========

**NOTICE:** This is an experiment and prototype.  The copyright is
owned by Basho but is not an official repository.  Currently, this is
a side project of the author and not supported in any way by Basho.

**USE AT YOUR OWN RISK**

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

1. Clone my fork of Riak.

        git clone git://github.com/rzezeski/riak.git
        cd riak

2. Checkout the Yokozuna branch.

        git checkout rz-yokozuna

3. Download the dependencies.

        make deps

4. Replace the Riak Core and KV dependencies with my forks and
   checkout the Yokozuna branch.

        cd deps
        rm -rf riak_core
        git clone git://github.com/rzezeski/riak_core.git
        (cd riak_core && git checkout rz-yokozuna)

        rm -rf riak_kv
        git clone git://github.com/rzezeski/riak_kv.git
        (cd riak_kv && git checkout rz-yokozuna)


5. Compile.  This will take a long time because it must clone Solr
   source and build it.  The first time you do this is the longest as
   Ivy may have to download all the dependencies if you don't
   regularly develop on Java projects.

        cd ..
        make

6. Make a stage rel or stage devrel.  At this point it's no different
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

This command must be run from the Riak console--you must first attach
to it.

    ./dev/dev1/bin/riak attach

An _index_ must be created in order for Yokozuna to index data.
Currently the index name is a 1:1 mapping with the bucket name.  I
may change this me a 1:M mapping from index to bucket.

    yz_index:add_to_ring("name_of_index").

### Install the Post-commit Hook ###

Yokozuna hooks into KV using a post-commit hook.  This may change
soon.  Remember, the bucket and index name must be the same.

    yz_kv:install_hook(<<"name_of_bucket">>).

### Index Some Data ###

Indexing data is a matter of writing data to KV.  At the moment
Yokozuna is hard coded to treat all objects as `text/plain`.  The
value of an object is stored under the `text` field in the Yokozuna
schema.  This will become more sophisticated as I iterate.

    curl -H 'content-type: text/plain' -X PUT 'http://localhost:8091/riak/name_of_bucket/name' -d "Ryan Zezeski"

### Searching ###

The syntax for querying Yokozuna is the same as querying a single
instance of Solr.  Yokozuna actually uses Solr's distributed search
API but that is hidden for you.  This means you don't have to worry
about where your shards are located.  This also means you should be
able to use any off-the-shelf Solr client to query Yokozuna.

    curl 'http://localhost:8091/search/name_of_index?q=text:Ryan'
