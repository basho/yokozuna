Yokozuna
==========

_Yokozuna_ - Horizontal rope.  The top rank in sumo, usually
translated _Grand Champion_.  The name comes from the rope a yokozuna
wears.

The goal of the yokozuna application is to integrate Apache Solr with
Riak in order to find the "top rank" documents for a given query.


Index Mappings & Cores
----------

Solr has the notion of a [core] [solr_core] which allows multiple
indexes to live under the same Solr/JVM instance.  This is useful
because it allows isolation of index files as well as schemas and
configuration.  Yokozuna exposes the notion of cores as _indexes_.

Each index has a unique name and maps to **one** core.  However, there
is a second mapping from alias to index name.  Many aliases may exist
for a given index.  This allows a decoupling of external names and
index names.  For example, when integrating with Riak this allows the
index name to be separate from the bucket name and thus allows many
buckets to map to the same index.

<TODO: insert diagram here of mapping from external app name, to
unique index name, to Solr core>

### Riak Integration

When indexing a Riak object yokozuna will use the bucket name to
look-up the index name.

[solr_core]: http://wiki.apache.org/solr/CoreAdmin
