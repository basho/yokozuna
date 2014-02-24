Yokozuna Release Notes
==========

0.14.0
------

### Features ###

* [288][], [294][] - Upgrade to Solr 4.6.1.

### Bug Fixes/Misc ###

* [234][], [278][] - Store schemas in cluster metadata instead of
  KV. This is a breaking change.

* [249][], [274][] - Fix `econnrefused` at startup by making a more
  robust "index exists" check.

* [293][] - Flush hashtrees during shutdown. Add test to verify to
  unnecessary AAE repair after cluster restart.

* [301][] - Mark hashtrees as built immediately after start if no data
  has been indexed. This allows exchange to begin immediately rather
  than waiting hours for trees to build.

* [308][] - Various AAE ports. Allow use of `never` to mark trees as
  never expired. Clear pending exchanges when switching to manual
  exchange mode. Don't clear trees until the lock has been grabbed to
  perform a rebuild; this allows exchange to happen all the way up
  until tree rebuild. Add ability to programmatically expire all trees
  on a node.

* [258][], [275][] - Fixed handoff hook in mixed cluster (1.4/2.0)
  scenario.

* [273][], [277][] - Create `search_index` atom during application
  start. This prevents crashes when Yokozuna is disabled.

* [276][] - Create a `solr` namespace for the Solr related
  configurations. This is a breaking change.

* [281][] - Add standardized test and dialyzer targets via `tools.mk`
  file.

* [282][] - Port various changes make to riak_kv in the 1.4.x release
  series such as new ring and bucket interfaces.

* [289][] - Move the index list from the ring to cluster
  metadata. This is a breaking change.

* [283][] - Better essential test: use random test queries and
  transition from 2 to 4 nodes to make sure cluster sizes less than
  `n_val` still produce correct results.

* [284][] - Treat compiler warnings as errors.

* [285][] - Fix argument order of calls to `yz_kv:dont_index` which is
  important for AAE to function optimally.

* [286][] - Add `xref` target to Makefile.

* [233][], [287][] - Replace hacky `maybe_wait` with check in handoff
  hook to wait for indexes to be created on joining nodes before
  starting ownership handoff.

* [295][] - Add notion of `n_val` to indexes. This decouples Yokozuna
  Indexes from KV Buckets and fixes a bug in query plan
  calculation. This is a breaking change.

* [303][] - Add a bucket property validation hook to make sure that
  buckets are associated with indexes that exist and have the same
  `n_val`.

* [311][] - Update previous hash on even tick to prevent syncing the
  index list too often.

* [313][] - Don't require the `text` field. Change the Solr ping
  handler to use `_yz_id` field. Change Solr select handler to not
  provide a default field. This is a breaking change for some users.

* [314][], [315][] - Build custom Yokozuna Solr plugins as a process
  separate from the main build and upload them to s3. This way the
  `javac` version can be more easily controlled and the main build can
  simply download jar files.

### Breaking Changes ###

#### Configuration ([276][]) ####

A solr sub-namespace was added to the Solr related configuration
options.

* `search.solr_port` -> `search.solr.port`
* `search.solr_jmx_port` -> `searchs.solr.jmx_port`
* `search.solr_jvm_opts` -> `search.solr.jvm_options`

#### Internal Index Changes ([289][], [295][]) ####

Both the internal datastructures and storage mechanism for indexes
have changed. As Yokozuna is not 1.0 yet there is no upgrade path from
previous releases. A rolling upgrade from an early release such as
Riak-pre11 may encounter failures.

#### Schema Storage ([278][]) ####

Schema storage has moved from KV Buckets to Cluster Metadata. Schemas
are now also stored gzip compressed. As Yokozuna is not 1.0 yet there
is no upgrade path from old schema storage to new. A rolling upgrade
from an early release such as Riak-pre11 may encounter failures.

#### Removal of df ([313][]) ####

The select handler, which services queries, no longer sets an implicit
`df=text`. Previously, if one sent a query with just terms and no
fields then the select handler would use a default field of
`text`. Anyone relying on this behavior would require a `text` field
in their schema. Rather than require everyone to add a `text` field to
their schema the implicit parameter was removed. If a user wishes to
set a default field they may do so by passing `df` with the request.

[233]: https://github.com/basho/yokozuna/issues/233
[234]: https://github.com/basho/yokozuna/pull/234
[249]: https://github.com/basho/yokozuna/issues/249
[253]: https://github.com/basho/yokozuna/issues/253
[258]: https://github.com/basho/yokozuna/issues/258
[273]: https://github.com/basho/yokozuna/issues/273
[274]: https://github.com/basho/yokozuna/pull/274
[275]: https://github.com/basho/yokozuna/pull/275
[276]: https://github.com/basho/yokozuna/pull/276
[277]: https://github.com/basho/yokozuna/pull/277
[278]: https://github.com/basho/yokozuna/pull/278
[281]: https://github.com/basho/yokozuna/pull/281
[282]: https://github.com/basho/yokozuna/pull/282
[283]: https://github.com/basho/yokozuna/pull/283
[284]: https://github.com/basho/yokozuna/pull/284
[285]: https://github.com/basho/yokozuna/pull/285
[286]: https://github.com/basho/yokozuna/pull/286
[287]: https://github.com/basho/yokozuna/pull/287
[288]: https://github.com/basho/yokozuna/issues/288
[289]: https://github.com/basho/yokozuna/pull/289
[293]: https://github.com/basho/yokozuna/pull/293
[294]: https://github.com/basho/yokozuna/pull/294
[295]: https://github.com/basho/yokozuna/pull/295
[301]: https://github.com/basho/yokozuna/pull/301
[303]: https://github.com/basho/yokozuna/pull/303
[308]: https://github.com/basho/yokozuna/pull/308
[311]: https://github.com/basho/yokozuna/pull/311
[313]: https://github.com/basho/yokozuna/pull/313
[314]: https://github.com/basho/yokozuna/issues/314
[315]: https://github.com/basho/yokozuna/pull/315


0.13.0
------

This release brings an upgrade to Solr, support for indexing Riak Data
Structures, the ability to reload indexes via `riak attach`, and
includes a query performance boost.

### Features ###

* [199][], [232][], [243][] - Upgrade to Solr 4.6.0.

* [241][], [254][] - Add ability to configure Solr start-up wait in `riak.conf`.

* [250][] - Add support for indexing Riak Data Structures (aka
  CRDTs). These are data structures for Riak, such as sets and maps,
  which automatically resolve themselves when siblings occur. This
  means you can think in terms of common data structures, not worry
  about sibling resolution, and query the data with Solr.

* [260][] - Add ability to reload indexes. This is used to pick up
  schema modifications, such as adding a field. This patch does not
  provide and client facing APIs for this functionality. That will be
  done in a separate patch.

* [239][], [265][] - Shutdown if Java executable cannot be found on
  the path.

### Performance ###

* [271][] - Add custom search component to perform per-node filter
  queries and remove the `_yz_node` field. One micro benchmark showed
  a 17% increase in queries per second.

### Bugs/Misc ###

* [245][], [257][] - Delete `core.properties` file during Solr Core
  creation to avoid indefinite errors after a bad schema has been
  used.

* [247][] - Fix logging of Solr stdout/stderr.

* [255][] - Fix dialyzer errors.

* [256][] - Speedup riak tests.

* [262][] - Renamed `solr_jvm_args` to `solr_jvm_opts`.

* [264][] - Fix the security test.

* [267][] - Reestablish check to remove indexes for non owned data.

### Documentation ###

* [244][] - Use one-to-one mapping example in README.

[199]: https://github.com/basho/yokozuna/issues/199
[232]: https://github.com/basho/yokozuna/pull/232
[239]: https://github.com/basho/yokozuna/issues/239
[241]: https://github.com/basho/yokozuna/pull/241
[243]: https://github.com/basho/yokozuna/pull/243
[244]: https://github.com/basho/yokozuna/pull/244
[245]: https://github.com/basho/yokozuna/issues/245
[247]: https://github.com/basho/yokozuna/pull/247
[250]: https://github.com/basho/yokozuna/pull/250
[254]: https://github.com/basho/yokozuna/pull/254
[255]: https://github.com/basho/yokozuna/pull/255
[256]: https://github.com/basho/yokozuna/pull/256
[257]: https://github.com/basho/yokozuna/pull/257
[260]: https://github.com/basho/yokozuna/pull/260
[262]: https://github.com/basho/yokozuna/pull/262
[264]: https://github.com/basho/yokozuna/pull/264
[265]: https://github.com/basho/yokozuna/pull/265
[267]: https://github.com/basho/yokozuna/pull/267
[271]: https://github.com/basho/yokozuna/pull/271

0.12.0
------

This release integrates the new bucket type feature and adds the
ability to migrate from Riak Search using AAE. Less disk space will be
used per index and a slight bump in query performance may be
noticed. Also, documentation was added on using Riak's new security
features with Yokozuna. Finally, three breaking changes were
made. There is some API rebranding, a change of the default field
separator, and bucket type creation. More details later in the notes.

### Features ###

* [176][], [230][] - Add support for the new bucket types feature in
  Riak. Bucket types allow an additional level of namespacing as well
  as a more efficient mechanism for storing custom properties. If no
  type is specified then the _default_ type is assumed. Yokozuna works
  with the default type but it is highly recommended you create your
  own types to take advantage of the more efficient property storage.

* [238][] - Ability to migrate from Riak Search via AAE.

### Performance ###

* [205][] - Don't store all Yokozuna specific fields. Several of the
  fields are only used for filtering and thus don't need to be
  stored. By not storing them the disk usage of the index was reduced
  by 25% in one micro-benchmark.

* [221][], [236][] - Use HTTP POST to send queries to Solr. This was
  done to prevent large filter queries from creating overly long URLs
  but also gave a slight bump in performance on one micro benchmark.

### Bugs/Misc ###

* [189][], [230][] - Make sure `_yz_id` is unique.

* [186][] - Remove defunct `bucket` field from HTTP index properties.

* [210][] - Use common URL prefix.

* [216][] - Add anti-entropy directory to configuration.

* [217][] - Require the `_yz_err` field.

* [220][] - Set `solr.solr.home` correctly.

* [228][] - Use a relative path for the schema file.

* [231][] - Change default field separator to `.`.

* [235][], [240][] - Rename some public facing parts of Yokozuna to "search".

* [237][] - Increase Solr startup timeout.

### Documentation ###

* [229][] - Add documentation on using the new Riak 2.0 security
  features with Yokozuna.

### Breaking Changes ###

#### Default Nested Field Separators ([231][]) ####

The default field separator for nested objects has been changed from
`_` to `.`. This means the following nested JSON object
`{"info":{"name":"ryan"}}` now creates a field with the name
`info.name`.

#### Public Interface Renaming ([235][]) ####

Some of the public facing bits of Yokozuna have been renamed to
"Search". Internally, the Yokozuna code-name will still be used but
some public facing parts replaced "yokozuna" or "yz" with "search".

The "yokozuna" prefix was renamed to "search" in the `riak.conf`
configuration file. It is still "yokozuna" in the `advanced.config`.

```
## To enable Search, set this 'on'.
search = on

## The port number which Solr binds to.
search.solr_port = 10014

## The port number which Solr JMX binds to.
search.solr_jmx_port = 10013

# and so on...
```

The type/bucket property to associate an index is now `search_index`.

All HTTP resources renamed `yz` to `search`. To manage an index or
schema, use, respectively:

* http://localhost:10018/search/index/INDEX_NAME
* http://localhost:10018/search/schema/SCHEMA_NAME

#### Associating Indexes, Bucket Type Support ([176][] & [230][]) ####

During Riak 2.0 development the new bucket type feature was added. It
provides an additional level of namespacing as well as a more
efficient and robust property mechanism. However, it also requires
creating a bucket type which can only be done at the console with
`raik-admin`.

All buckets with no explicit type will be considered "legacy" and will
live under the `default` type. Yokozuna indexes may be associated with
buckets under the default type but it is highly recommended that a
custom type be created to take advantage of the more efficient
property mechanism.

An index can be associated by either setting the `search_index`
property on the type, which will apply it to all buckets underneath
that type, or set it per bucket.

See [ADMIN][] for more details.

[ADMIN]: https://github.com/basho/yokozuna/blob/develop/docs/ADMIN.md

[176]: https://github.com/basho/yokozuna/issues/176
[186]: https://github.com/basho/yokozuna/issues/186
[189]: https://github.com/basho/yokozuna/issues/189
[205]: https://github.com/basho/yokozuna/pull/205
[210]: https://github.com/basho/yokozuna/pull/210
[216]: https://github.com/basho/yokozuna/pull/216
[217]: https://github.com/basho/yokozuna/pull/217
[220]: https://github.com/basho/yokozuna/pull/220
[221]: https://github.com/basho/yokozuna/issues/221
[228]: https://github.com/basho/yokozuna/pull/228
[229]: https://github.com/basho/yokozuna/pull/229
[230]: https://github.com/basho/yokozuna/pull/230
[231]: https://github.com/basho/yokozuna/pull/231
[235]: https://github.com/basho/yokozuna/pull/235
[236]: https://github.com/basho/yokozuna/pull/236
[237]: https://github.com/basho/yokozuna/pull/237
[238]: https://github.com/basho/yokozuna/pull/238
[240]: https://github.com/basho/yokozuna/pull/240

0.11.0
------

This release brings Riak Java Client support as well as authentication
and security for the HTTP and protocol buffer transports. An access
control list (ACL) may be created to control administration and access
to indexes. All official Riak clients should now have full support for
Yokozuna's administration and search API. Stored boolean fields and
tagging support were fixed for the protocol buffer transport. And
finally, documentation was added. The new CONCEPTS document goes over
various important concepts in Yokozuna and the RESOURCES document has
links to other resources for learning.

### Features ###

* [106][] - Add administration and search support to the Java client.

* [157][] - Add index and schema administration support to all
  official Riak clients. This was completed with the closing of [106][].

* [145][] - Allow authentication and secure connections for both the
  HTTP and protocol buffer protocols. When enabled, the security
  system allows an ACL to be setup for administration and search
  operations. A user may have all or no administration rights as well
  as search rights on none, some, or all of the available
  indexes. Security showed a 3-6% decrease in indexing/querying
  throughput for one benchmark.

### Bugs/Misc ###

* [132][], [203][], [204][]  - Add integration tests to Basho's internal CI tool.

* [177][], [206][] - Don't wait for Riak KV service. This prevents
  Yokozuna from blocking start of other applications in Riak.

* [202][] - Don't convert index name to a list; it is always binary.

* [209][], [214][] - Fix handling of stored boolean fields in the
  protocol buffer transport.

* [212][] - Don't allow siblings in the schema bucket. Yokozuna
  expects only value for each schema name.

* [222][] - Remove bucket field from index info. It is a hold-over
  from the days of one-to-one indexing.

* [223][] - Fix tagging over the protocol buffer transport.

* [224][] - Keep support for Erlang R15B.

* [226][] - Add support for secure protocol buffer transport to
  Yokozuna's basho bench driver.

### Documentation ###

* [208][] - Small clarification to installation doc.

* [213][] - Add doc with links to resources on Yokozuna and Search.

* [225][] - Add doc about various important concepts in Yokozuna.

[106]: https://github.com/basho/yokozuna/issues/106
[132]: https://github.com/basho/yokozuna/issues/132
[145]: https://github.com/basho/yokozuna/pull/145
[157]: https://github.com/basho/yokozuna/issues/157
[177]: https://github.com/basho/yokozuna/issues/177
[203]: https://github.com/basho/yokozuna/pull/203
[204]: https://github.com/basho/yokozuna/pull/204
[202]: https://github.com/basho/yokozuna/pull/202
[206]: https://github.com/basho/yokozuna/pull/206
[208]: https://github.com/basho/yokozuna/pull/208
[209]: https://github.com/basho/yokozuna/issues/209
[212]: https://github.com/basho/yokozuna/pull/212
[213]: https://github.com/basho/yokozuna/pull/213
[214]: https://github.com/basho/yokozuna/pull/214
[222]: https://github.com/basho/yokozuna/pull/222
[223]: https://github.com/basho/yokozuna/pull/223
[224]: https://github.com/basho/yokozuna/pull/224
[225]: https://github.com/basho/yokozuna/pull/225
[226]: https://github.com/basho/yokozuna/pull/226

0.10.0
------

This release brings a few features such as an upgrade in Solr version
along with some basic indexing and query stats.  The default index has
been removed returning write performance closer to baseline for
non-indexed buckets.  Disk usage was decreased by removing the default
index and the unused timestamp from the entropy data.  Among the list
of other fixes a notable one is the improvement of Solr start-up and
crash semantics.  If Solr crashes too frequently then Yokozuna will
stop the local Riak node.

### Features ###

* [150][], [197][] - Upgrade to Solr 4.4.0.

* [179][] - Add support for stats.  Add basic index and query stats.

* [107][] - Verify support for Riak Ruby client over protocol
  buffers.

* [108][] - Verify support for Riak Python client over protocol
  buffers.

### Performance ###

* [190][] - Remove the entropy data timestamp.  It hasn't been used
  for over a year.  The throughput and latency improvements were small
  but it save 7.4% in index size for one benchmark.

* [171][] - Remove the default index.  Since the 0.1.0 release of
  Yokozuna a "default index" was needed to avoid constant repair by
  AAE.  It came at a cost of performance degredation of all writes,
  more disk space, and more CPU/memory usage.  Simply enabling
  Yokozuna, write performance would drop to 57% of baseline.  With the
  default index removed that figure rises to 93% of baseline.
  Furthermore, less disk space will be used.

### Bugs/Misc ###

* [188][] - More robust Solr start-up and crash semantics.  If the Solr
  JVM instance cannot stay up then bring down the Riak node with it.
  This fail-fast behavior is preferred to silently ignoring the issue.
  It brings the problem to the attention of the user more quickly,
  forcing the problem to be addressed.

* [192][], [194][] - Handle not-found case when fetching a schema via
  protobuffs.

* [175][] - Fix the `associated_buckets` function.

* [165][], [173][] - Create data directory and `solr.xml` at run-time,
  not build-time.  This makes it easier to wipe data by removing
  physical files.

* [129][] - Fix persistence of schema attribute in `solr.xml`.

* [198][] - Fix Solaris build.

* [196][] - Allowing javac to be skipped during Yokozuna build.  This
  is for developers of Riak who don't have a JDK and don't want to
  test Yokozuna.

* [195][] - Cache the Solr package for future builds.  For those
  developing against Riak/Yokozuna this saves network bandwidth and
  time.

* [193][] - Various cleanup of integration tests.

* [184][] - Refactor the `index_name()` type to be `binary()`.

* [182][] - Check for presence of certain tools at build time.

* [180][] - Disable certificate check when pulling Solr package.

* [172][] - Small fixes based on Dialyzer errors.

* [200][], [201][] - Update to new hashtree API.

[107]: https://github.com/basho/yokozuna/issues/107
[108]: https://github.com/basho/yokozuna/issues/108
[129]: https://github.com/basho/yokozuna/issues/129
[150]: https://github.com/basho/yokozuna/issues/150
[165]: https://github.com/basho/yokozuna/issues/165
[171]: https://github.com/basho/yokozuna/pull/171
[172]: https://github.com/basho/yokozuna/pull/172
[173]: https://github.com/basho/yokozuna/pull/173
[175]: https://github.com/basho/yokozuna/pull/175
[179]: https://github.com/basho/yokozuna/pull/179
[180]: https://github.com/basho/yokozuna/pull/180
[182]: https://github.com/basho/yokozuna/pull/182
[184]: https://github.com/basho/yokozuna/pull/184
[188]: https://github.com/basho/yokozuna/pull/188
[190]: https://github.com/basho/yokozuna/pull/190
[192]: https://github.com/basho/yokozuna/issues/192
[193]: https://github.com/basho/yokozuna/pull/193
[194]: https://github.com/basho/yokozuna/pull/194
[195]: https://github.com/basho/yokozuna/pull/195
[196]: https://github.com/basho/yokozuna/pull/196
[197]: https://github.com/basho/yokozuna/pull/197
[198]: https://github.com/basho/yokozuna/pull/198
[200]: https://github.com/basho/yokozuna/issues/200
[201]: https://github.com/basho/yokozuna/pull/201

0.9.0
-----

The ninth release of Yokozuna.  Now integrated with the latest
development branch of Riak.  No special branches required.  Schema and
index administration supported over protocol buffers transport.  A
major performance regression is fixed as well as a deadlock in AAE.
Work on support for migrating from Riak Search has started.  Along
with various other bug fixes.

### Features ###

* [60][] - Various patches integrating Yokozuna and Riak.

  * [154][], - Allow Yokozuna to report AAE status such as tree build
    times and exchange stats. ([kv-596][], [kv-654][])

  * [155][], [156][], [166][] - Integration with Riak development
    branch. ([riak-375][])

* [163][] - Add a function to perform a live switch of query handling
  from Riak Search to Yokozuna.  This is a building block to allow
  migration without restarting the node.

* [163][] - Add a test to verify using AAE as a means to migrate from
  Riak Search to Yokozuna.

* [144][], [161][] - Add index and schema administration support to
  the protocol buffers interface. ([pb-51][])

* [riakc-112][] - Add support for index and schema administration to
  the Riak Erlang Client.

### Performance ###

* [164][] - There is a major performance regression in the 0.8.0
  release.  It affects KV writes and indexing.  Even if Yokozuna is
  disabled.  This patch fixes the regression.

### Bugs/Misc ###

* [153][] - Fix field name creation in XML extractor.

* [159][] - More reliable tear down of JVM process.  Move JAR files
  into `priv` dir which makes upgrades easier.  Move
  `log4j.properties` into `etc` dir.

* [162][] - Simplify the script that downloads Solr.

* [163][] - Fix AAE deadlock.

* [163][] - Fix mixed cluster issues when upgrading from previous Riak
  versions.

* [163][] - Don't send empty parameters to Solr when querying Yokozuna
  via protocol buffers.

* [163][] - Simplify AAE repair function to repairing the local index
  only, thus avoiding RPC.  Fix tree mapping to avoid indefinite index
  repair.

* [167][] - Perform AAE hashtree updates in asynchronous fashion with
  periodic synchronous (blocking) calls.  Use an infinity timeout for
  synchronous call to avoid crashing the KV vnode.  This should also
  help write throughput but no benchmarks were performed.

* [169][] - Add `*_coordinate` field for spatial indexing.

### Documentation ###

* [158][] - Fix example doc.

[60]: https://github.com/basho/yokozuna/issues/60
[144]: https://github.com/basho/yokozuna/pull/144
[153]: https://github.com/basho/yokozuna/pull/153
[154]: https://github.com/basho/yokozuna/issues/154
[155]: https://github.com/basho/yokozuna/issues/155
[156]: https://github.com/basho/yokozuna/issues/156
[158]: https://github.com/basho/yokozuna/pull/158
[159]: https://github.com/basho/yokozuna/pull/159
[161]: https://github.com/basho/yokozuna/pull/161
[162]: https://github.com/basho/yokozuna/pull/162
[163]: https://github.com/basho/yokozuna/pull/163
[164]: https://github.com/basho/yokozuna/pull/164
[166]: https://github.com/basho/yokozuna/pull/166
[167]: https://github.com/basho/yokozuna/pull/167
[169]: https://github.com/basho/yokozuna/pull/169

[kv-596]: https://github.com/basho/riak_kv/pull/596
[kv-654]: https://github.com/basho/riak_kv/pull/654

[pb-51]: https://github.com/basho/riak_pb/pull/51

[riak-375]: https://github.com/basho/riak/pull/375

[riakc-112]: https://github.com/basho/riak-erlang-client/pull/112

0.8.0
-----

The eighth release of Yokozuna.  It is now considered alpha and will
soon become part of Riak proper.  There could still be breaking
changes leading up to the 1.0.0 release which is currently scheduled
for early October.

The main things of interest this release are the re-target to Riak
1.4.0 and removal of a race condition around index creation.

### Features ###

* [139][] - Re-target Yokozuna against Riak 1.4.0.

### Bugs/Misc ###

* [40][], [140][] - Remove custom EC2 support.  Yokozuna will soon be
  part of Riak proper which has it's own EC2 support.

* [126][] - Yokozuna will not support a Riak Search compatible HTTP
  interface.  If you will be migrating from Riak Search to Yokozuna
  then you will need to either a) switch to the Protocol Buffers
  transport or b) use a Solr client to talk to query Yokozuna.

* [134][], [136][] - Improve Solr start-up process in Yokozuna.

* [137][], [138][] - Improve UTF8 handling in XML extractor.  Add more
  comprehensive tests.  Thank you to `sogabe` for the initial patch.

* [147][] - Ability to enable/disable components.  Mostly a renaming
  and documenting of existing functionality.

* [148][] - Set Java AWT headless mode to prevent annoying pop-up.

* [149][] - Fix call that could cause a bucket's index association to
  be lost.

* [151][] - Fix race condition in index creation.  Previously index
  entries could be lost from the index set if created fast enough.

### Documentation ###

* [152][] - Suggest use of Oracle JVM, not OpenJDK.

[40]: https://github.com/basho/yokozuna/issues/40
[126]: https://github.com/basho/yokozuna/issues/126
[134]: https://github.com/basho/yokozuna/issues/134
[136]: https://github.com/basho/yokozuna/pull/136
[137]: https://github.com/basho/yokozuna/pull/137
[138]: https://github.com/basho/yokozuna/pull/138
[139]: https://github.com/basho/yokozuna/issues/139
[140]: https://github.com/basho/yokozuna/pull/140
[147]: https://github.com/basho/yokozuna/pull/147
[148]: https://github.com/basho/yokozuna/pull/148
[149]: https://github.com/basho/yokozuna/pull/149
[151]: https://github.com/basho/yokozuna/pull/151
[152]: https://github.com/basho/yokozuna/pull/152

0.7.0
----------

The seventh pre-release of Yokozuna.  Solr has been upgraded to 4.3.0.
The same index may be used by many buckets.  Indexing errors are
isolated from object writes.  Queries may be used as input to
map-reduce jobs.  Improved throughput for indexing.  Proper cleanup of
JVM processes.  Along with some other small fixes.

### Features ###

* [57][], [112][] - Isolate indexing failure from object writes.  On
  indexing failure write an "error document" in Solr that prevents AAE
  from constantly repairing but also allows user to query for keys
  which failed to index.

* [121][], [122][] - Upgrade to [Solr 4.3.0][s430].  Remove the JSON
  query response writer so `wt=json` actually uses `application/json`
  content-type header.

* [90][] - Allow one-to-many index-to-buckets relationship.  Now the
  same index can be used by many buckets.  A bucket can still only
  have one index associated with it but it doesn't have to have the
  same name and more than one bucket can use the same index.

* [19][], [133][] - Add map-reduce support.  Now Yokozuna queries can
  be used as input to a map-reduce job.

### Performance ###

* [48][] - Send delete and update in the same request.  In order to
  handle siblings Yokozuna currently needs to perform both a delete
  and update for every object.  Performing both operations with one
  request rather than two showed a 15% write-throughput improvement in
  a benchmark on my MacBook Pro.

### Bugs/Misc ###

* [91][], [111][] - Remove the auto-suffix JSON extractor.  It caused
  duplicate code, the suffix generation was too simple, and now there
  is a catch-all field in the default schema.

* [117][] - Some code cleanup.

* [119][] - Fix regression in the extractor test resource.

* [118][] - Close hashtrees on application stop.  Any time a hashtree
  server is stopped make sure to properly close the hashtree first.

* [120][] - Add some scripts for calculating benchmark results.

* [72][] - Work has finished on filter query compression.

* [128][] - Serialize index creation through the claimant node.  This
  prevents index creation events on disjoint nodes from racing with
  each other.

* [124][], [127][] - JVM self destruct.  There are cases where Riak
  might go down in such a way that it doesn't have a chance to stop
  the corresponding JVM process.  Add a periodic task, running inside
  the JVM, that checks for this condition and exits on discovery.
  Without this Riak could not restart because it would try to start a
  new JVM/Solr process but the port would already be bound causing the
  restart to fail.

### Documentation ###

* [116][] - Add INSTALL doc.  Covers installing from source package
  and GitHub.

### Breaking Changes ###

* The one-to-many ([90][]) patch changes the way a bucket is marked for
  indexing.  Previously, when creating an index it implicitly added a
  flag to the bucket with the same name.  This means you could create
  an index and mark a bucket for indexing in one step.  Now it will
  take two.  The first step is to create the index as before.  The
  second is to add a `yz_index` bucket property with the name of the
  index.  See the README for an example.

[19]: https://github.com/basho/yokozuna/issues/19
[48]: https://github.com/basho/yokozuna/pull/48
[57]: https://github.com/basho/yokozuna/pull/57
[72]: https://github.com/basho/yokozuna/issues/72
[90]: https://github.com/basho/yokozuna/pull/90
[91]: https://github.com/basho/yokozuna/issues/91
[111]: https://github.com/basho/yokozuna/pull/111
[112]: https://github.com/basho/yokozuna/pull/112
[116]: https://github.com/basho/yokozuna/pull/116
[117]: https://github.com/basho/yokozuna/pull/117
[118]: https://github.com/basho/yokozuna/pull/118
[119]: https://github.com/basho/yokozuna/pull/119
[120]: https://github.com/basho/yokozuna/pull/120
[121]: https://github.com/basho/yokozuna/issues/121
[122]: https://github.com/basho/yokozuna/pull/122
[124]: https://github.com/basho/yokozuna/issues/124
[127]: https://github.com/basho/yokozuna/pull/127
[128]: https://github.com/basho/yokozuna/pull/128
[133]: https://github.com/basho/yokozuna/pull/133
[s430]: http://lucene.apache.org/solr/4_3_0/changes/Changes.html

0.6.0
----------

The sixth pre-release of Yokozuna.  Now with support for Riak clients
which have a Search API using the Protobuff transport.  A 30-40%
improvement in query throughput.  And much more.

### Features ###

* [95][] - Add catch-all field to default schema.  This will prevent
  unknown fields from causing runtime errors.

* [104][], [114][] - Add `enabled` flag to config, to control whether
  or not Yokozuna is started at cluster start.

* [37][], [38][] - Add protocol buffer client support at parity with
  Riak Search.  This means any Riak clients with Search-PB support can
  now be used to query Yokozuna at parity with the Riak Search query
  interface.

### Performance ###

* [103][], [109][], [115][] - Cache coverage plans in mochiglobal.  Two
  benchmarks showed 30-40% improvement in throughput.  Slow CPU and
  queries cached by Solr will benefit most from this patch.

### Bugs/Misc ###


* [88][], [94][] - Update to newer rebar to fix erlang_js build.

* [58][], [89][], [93][] - Add resilience around bad schemas.  Make
  index creation a best effort with periodic retry.

* [86][] - Fix delete query.  Don't blow up on keys that have
  characters considered special by Solr's default query parser.

* [79][], [96][] - Don't index fallback data.  Fallback data is not
  considered during querying, thus wasting CPU and IO on pointless
  indexing.

* [56][] - Strip down default config, rename to `solrconfig.xml`.

* [64][] - More AAE ports.  Two potential Active Anti-Entropy ports
  from KV were determined unnecessary.

* [92][], [97][] - Pass empty list of JVM arguments by default in the
  code.  The assumption is that if a user doesn't specify a
  `solr_vm_args` then the code should default to passing nothing.

* [59][] - Fix decoding of `yz-extractor` header.

* [99][] - Make sure to remove index data after index delete.

* [100][] - Add `yz-fprof` header to allow analyzing performance of
  search requests.

* [66][], [101][] - Use n-val for put-quorum values rather than constants.
  This will allow Yokozuna to work for n-val < 3.

* [102][] - Fix the Riak Tests, use the new `rt_config` module.

* [113][] - Add Q&A document.

[37]: https://github.com/basho/yokozuna/pull/37
[38]: https://github.com/basho/yokozuna/pull/38
[56]: https://github.com/basho/yokozuna/pull/56
[58]: https://github.com/basho/yokozuna/pull/58
[59]: https://github.com/basho/yokozuna/pull/59
[64]: https://github.com/basho/yokozuna/pull/64
[66]: https://github.com/basho/yokozuna/pull/66
[79]: https://github.com/basho/yokozuna/pull/79
[86]: https://github.com/basho/yokozuna/pull/86
[88]: https://github.com/basho/yokozuna/pull/88
[89]: https://github.com/basho/yokozuna/pull/89
[92]: https://github.com/basho/yokozuna/pull/92
[93]: https://github.com/basho/yokozuna/pull/93
[94]: https://github.com/basho/yokozuna/pull/94
[95]: https://github.com/basho/yokozuna/pull/95
[96]: https://github.com/basho/yokozuna/pull/96
[97]: https://github.com/basho/yokozuna/pull/97
[99]: https://github.com/basho/yokozuna/pull/99
[100]: https://github.com/basho/yokozuna/pull/100
[101]: https://github.com/basho/yokozuna/pull/101
[102]: https://github.com/basho/yokozuna/pull/102
[103]: https://github.com/basho/yokozuna/pull/103
[104]: https://github.com/basho/yokozuna/pull/104
[109]: https://github.com/basho/yokozuna/pull/109
[113]: https://github.com/basho/yokozuna/pull/113
[114]: https://github.com/basho/yokozuna/pull/114
[115]: https://github.com/basho/yokozuna/pull/115


0.5.0
----------

The fifth pre-release of Yokozuna.  It includes new features, bug
fixes, a performance improvment for search, and an upgrade of Solr.

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
