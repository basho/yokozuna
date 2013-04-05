Yokozuna Release Notes
==========

0.5.0
----------

The fifth pre-release of Yokozuna.

### Features

* [YZ-78][]: Bump to [Solr 4.2.0][solr-420]. Use custom binary package
  to avoid need to compile Solr when building.  Thus reduced time and
  complexity of build.

* [YZ-71][]: Allow passing of JVM options via `app.config`.  The
  following options were added as defaults.
  * XX:+UseStringCache
  * XX:+UseCompressedStrings
  * XX:+UseCompressedOops
  * Xms1g
  * Xmx1g

* [YZ-47][]: Add kill switch for both index and search.

* [YZ-36][], [YZ-81][]: Take appropriate action during index flag
  transitions.

* [YZ-49][]: Allow Yokozuna to have it's own AAE settings but fallback
  to KV.

* [YZ-80][]: Trim down default schema.

* [YZ-83][]: Add dynamic fields for all langs in default schema.

* [YZ-76][]: Add filter query compression.  A 17% improvement in
  throughput was found in one benchmark.

### Bugs/Misc

* [YZ-50][]: Add tests for other languages.

* [YZ-74][]: Make scripts more portable.

* [YZ-51][]: Rename app and sup modules to follow naming convention.

* [52e56c][]: Fix Travis CI build.

* [YZ-69][]: Fix port numbers in README.  Thanks timdoug.

* [YZ-82][]: Fix links in docs.  Thanks kyleslattery.

* [YZ-15][]: Provide extractors access to full riak object.  Closed
  without change.

* [YZ-52][]: Ring event + downed solr proc causes issues.  Closed
  without change.

* [acf462][], [1e870a][]: Make verify script more robust.  This script
  is used for verifying the correctness of Yokozuna.

* [01f4e6][]: Rename `uniqueKey` from `id` to `_yz_id`.

[YZ-15]: https://github.com/basho/yokozuna/pull/15
[YZ-36]: https://github.com/basho/yokozuna/pull/36
[YZ-47]: https://github.com/basho/yokozuna/pull/47
[YZ-49]: https://github.com/basho/yokozuna/pull/49
[YZ-50]: https://github.com/basho/yokozuna/pull/50
[YZ-51]: https://github.com/basho/yokozuna/pull/51
[YZ-52]: https://github.com/basho/yokozuna/pull/52
[YZ-69]: https://github.com/basho/yokozuna/pull/69
[YZ-71]: https://github.com/basho/yokozuna/pull/71
[YZ-74]: https://github.com/basho/yokozuna/pull/74
[YZ-76]: https://github.com/basho/yokozuna/pull/76
[YZ-78]: https://github.com/basho/yokozuna/pull/78
[YZ-80]: https://github.com/basho/yokozuna/pull/80
[YZ-81]: https://github.com/basho/yokozuna/pull/81
[YZ-82]: https://github.com/basho/yokozuna/pull/81
[YZ-83]: https://github.com/basho/yokozuna/pull/83

[acf462]: https://github.com/basho/yokozuna/commit/acf4627992053f7b08859d885bf6710ba47d3b77
[1e870a]: https://github.com/basho/yokozuna/commit/1e870a6178119e32215a986391fef4e3fc762bdb
[52e56c]: https://github.com/basho/yokozuna/commit/52e56cd4199ea89fb0d8ffe005a491b57bfa9b9e
[01f4e6]: https://github.com/basho/yokozuna/commit/01f4e66f45a83d676f3d4dde4b31484b6474fd18

[solr-420]: http://lucene.apache.org/solr/4_2_0/changes/Changes.html


0.4.0
----------

The fourth pre-release of Yokozuna.  This is a small release but
important because it bases Yokozuna against the official Riak 1.3.0
release.

* Potentially major performance improvements in Solr's distributed
  search ([yz_26][]).  Improvement will depend on environment but
  non-trivial performance increase has been reproduced on two
  different clusters on different hardware and operating systems.  An
  upstream patch to Solr has been proposed as well, see [SOLR-4509][].

* A couple of integration tests were added for schema and index
  administration ([yz_65][]).

* This release is based on Riak 1.3.0.  It uses the same tags as 1.3.0
  as opposed to basing off the current state of master as done in
  previous releases.  This means that 0.4.0 is Riak 1.3.0 with the
  Yokozuna bits added along with two small changes to Riak to
  integrate the two.

* Yokozuna is now under the [Basho organization on GitHub][yz_basho],
  where it belongs.  This does not change the prototype status but
  does mean that Yokozuna is a step closer to being integrated with
  mainline Riak.

[yz_basho]: https://github.com/basho/yokozuna

[yz_26]: https://github.com/basho/yokozuna/pull/26

[yz_65]: https://github.com/basho/yokozuna/pull/65

[SOLR-4509]: https://issues.apache.org/jira/browse/SOLR-4509


0.3.0
----------

The third pre-release of Yokozuna.

* Listen on canonical Solr URL ([yz_39][]).

* Added new target (`yz-setup`) to Riak branch to make it easier to
  build Yokozuna/Riak from source ([d32a20][]).

* Disable Solr realtime get.  It is not used by Yokozuna and greatly
  reduced write performance when enabled ([9e7e4f][]).

* Return a better error message when searching nonexistent index ([yz_33][]).

* Don't require a `content-type` header when creating an index with no
  body ([74492c][]).

* Use Solr's JSON update instead of XML update.  One benchmark showed
  approximately 15% improvement in write throughput ([yz_31][]).

* Port the latest AAE updated from Riak KV to Yokozuna ([yz_30][], [yz_61][]).

* More robust AAE ([510d30][]  & [892bf5][]).

* Pre-create the default schema ([7a2167][]).

* Add basic support for storing and retrieving of schemas via
  HTTP. Thanks to Marcel Neuhausler ([yz_42][]).

* Don't overwrite modified Solr Core config files ([yz_14][]).

* Update to [Solr 4.1.0][solr41_relnotes] ([yz_45][]).

* Update to the latest Riak.


[d32a20]: https://github.com/rzezeski/yokozuna/commit/d32a20c26eb8fc092c71dbc3879552433a9930b5

[9e7e4f]: https://github.com/rzezeski/yokozuna/commit/9e7e4f4577575ce4d1f5e8b9356d46f0f787b5c9

[74492c]: https://github.com/rzezeski/yokozuna/commit/74492cd86e27aa3d18c159194734466c6f67b29a

[510d30]: https://github.com/rzezeski/yokozuna/commit/510d30d7beea3d4a7cca2b0b327f171567b0042c

[892bf5]: https://github.com/rzezeski/yokozuna/commit/892bf585d0bbece6c305b0251f5820926b8e224d

[7a2167]: https://github.com/rzezeski/yokozuna/commit/7a21679a28087f2222261c70958d7eeb06cbdb65

[yz_14]: https://github.com/rzezeski/yokozuna/pull/14

[yz_30]: https://github.com/rzezeski/yokozuna/pull/30

[yz_31]: https://github.com/rzezeski/yokozuna/pull/33

[yz_33]: https://github.com/rzezeski/yokozuna/pull/33

[yz_39]: https://github.com/rzezeski/yokozuna/pull/39

[yz_42]: https://github.com/rzezeski/yokozuna/pull/42

[yz_45]: https://github.com/rzezeski/yokozuna/pull/45

[yz_61]: https://github.com/rzezeski/yokozuna/pull/61

[solr41_relnotes]: http://lucene.apache.org/solr/4_1_0/changes/Changes.html


0.2.0
----------

This is the second pre-release of Yokozuna.

### Active Anti-Entropy

[Active Anti-Entropy][yz_aae] is a background process which constantly
seeks out divergence between Riak data and indexes stored by Yokozuna.
This process is made efficient by the use of hashtrees which are
updated as data comes in.  In the case were there is little to no
divergence the work performed is unnoticeable.  When divergence is
detected [read-repair][rr] is invoked and re-indexing occurs.

[rr]: http://docs.basho.com/riak/latest/references/appendices/concepts/Riak-Glossary/#Read-Repair

[yz_aae]: https://github.com/rzezeski/yokozuna/commit/7a32c6fce7b3d30de0f5f3f0d7a9a6ac29460f9f

### Sibling Support

Eric Redmond added [sibling support][yz_4].  Now, when [allow_mult][]
is enabled and siblings occur Yokozuna will index all object versions.
Likewise, when siblings are resolved the obsolete indexes will be
cleaned up.

[allow_mult]: http://docs.basho.com/riak/latest/references/appendices/concepts/Vector-Clocks/#Siblings

[yz_4]: https://github.com/rzezeski/yokozuna/pull/4

### Auto-Suffix JSON Extractor

Dan Reverri added a [JSON extractor][yz_16] that automatically adds
type suffixes to each field.  This allows you to use the default
schema with existing data.  That is, you don't have to create a custom
schema or add field suffixes yourself.

[yz_16]: https://github.com/rzezeski/yokozuna/pull/16

### Benchmark Scripts

A collection of scripts have been created under `misc/bench/bin`.
They are used for automating the process of running benchmarks with
Basho Bench, collecting various metrics, transforming the raw data,
and finally visualizing it via d3js.  Currently these scripts assume
the target cluster is running on SmartOS.  The driver scripts,
i.e. the scripts that run the benchmark, have only been tested on OSX.

### Misc

* Use semver rebar plugin ([3bff0c][]).

* Fix type issue in JSON extractor ([f2d798][]).

* Allow POST select ([yz_23][]).

* Send/Return Solr Headers ([yz_29][]).

* Remove shell wrapper around JVM ([yz_18][]).

* Remove unused files from included Solr distribution ([ab1303][]).

[3bff0c]: https://github.com/rzezeski/yokozuna/commit/3bff0c3a1407f5054838437ba534ae422c9d3bb5

[ab1303]: https://github.com/rzezeski/yokozuna/commit/ab130361f36e0f2f1b1307d5b99060a2c01d5648

[f2d798]: https://github.com/rzezeski/yokozuna/commit/f2d798c25b43dad820bdde7995c4219a2d892622

[yz_18]: https://github.com/rzezeski/yokozuna/issues/18

[yz_23]: https://github.com/rzezeski/yokozuna/issues/23

[yz_29]: https://github.com/rzezeski/yokozuna/issues/29


0.1.0
----------

This is the first pre-release of Yokozuna.  It provides automatic
distribution and replication of Solr indexes.  It integrates with Riak
KV providing near-real-time indexing of KV objects as they are
written.  Following are some of the major features included in this
release.

### Tight Integration With Solr

Yokozuna comes pre-bundled with Solr 4.0.0 running in the Jetty
container.  Yokozuna handles the basic administration of Solr such as
starting and stopping Solr, creating Cores, sharding the documents,
replicating the documents, and performing distributed queries.

To the client Yokozuna acts as a single Solr instance.  Queries are
sent to Yokozuna the same as a single Solr instance.  Yokozuna
determines which shards need to be contacted and performs a
distributed Solr query.  The results are returned verbatim to the
client.  This means if [Solr distributed search][sds] supports it then
Yokozuna supports it.  Furthermore, existing Solr clients may be used
to query Yokozuna.  There is a [simple example][se] of using the SolrJ
client.

### Administration of Indexes via HTTP

To start indexing KV data an index must be created and a hook
installed on the bucket.  This can be done in one step via HTTP.
Learn more at [ADMIN.md][admin].

### Text, XML and JSON Extractors

Before Yokozuna can index a KV object it must first convert it to a
Solr document.  This release includes support for text, XML and JSON.
To see how Yokozuna is extracting data the HTTP extract resource can
be used for testing.  It returns the field-value pairs in JSON format.

    curl -XPUT -H 'content-type: application/json' 'http://localhost:8098/extract' -d '...SOME JSON...' | jsonpp

### Tagging

Tagging is the ability to add indexes via Riak KV metadata.  This is
useful when storing binary data or for indexing data about the data.
More information can be found in [TAGGING.md][tagging].

### EC2 Support

A Yokozuna AMI is provided to make it easier to try.  More information
can be be found in [EC2.md][ec2].

[admin]: https://github.com/rzezeski/yokozuna/blob/7abbc3f7430373a58fdefaa65731759344e86cc7/docs/ADMIN.md

[ec2]: https://github.com/rzezeski/yokozuna/blob/7abbc3f7430373a58fdefaa65731759344e86cc7/docs/EC2.md

[sds]: http://wiki.apache.org/solr/DistributedSearch#Distributed_Searching_Limitations

[se]: https://github.com/rzezeski/yokozuna/blob/7abbc3f7430373a58fdefaa65731759344e86cc7/priv/java/com/basho/yokozuna/query/SimpleQueryExample.java

[tagging]: https://github.com/rzezeski/yokozuna/blob/7abbc3f7430373a58fdefaa65731759344e86cc7/docs/TAGGING.md
