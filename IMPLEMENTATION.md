Implementation Notes
==========

Notes on the implementation _before_ it is implemented.  Think of it
something like [readme driven development] [rdd].


Index Mapping & Cores
----------

* Set `persistent` to `true` in `solr.xml` so that changes during
  runtime will persist on restart.

* Must have an `adminPath` property for `cores` element or else
  dynamic manipulation will not work.

* Potentially use `adminHandler` and create custom admin handler for
  Riak integration.

* The core name is the unique index name used in yokozuna.
  I.e. yokozuna calls an index what Solr calls a core.  However, there
  is a many-to-one mapping of external names, or aliases, to index
  names.

* According to the Solr wiki it overwrites the `solr.xml` when core
  data is changed.  In order to protect against corruption yokozuna
  might want to copy this off somewhere before each core modification.

* The core `CREATE` command only works if the instance dir and config
  is already there.  This means that yokozuna will have to store a
  default setup and copy it over before calling `CREATE`.

* An HTTP endpoint `yz/index/create` will allow the creation of an
  index.  Underneath it will call `yz_index:create`.

* There is an implicit mapping from the index name to itself.

### Integrating with Riak KV

* Ideally, KV should know nothing about yokozuna.  Rather yokozuna
  should register a hook with KV and deal with the rest.  Yokozuna
  will have knowledge of Riak Object for now.  This should probably be
  isolated to a module like `yz_riak_kv` or something.

* Yokozuna is "enabled" on a bucket by first mapping a bucket name to
  an index.  Second, the `yz_riak_kv:postcommit` hook must be
  installed on the bucket.

* Using a postcommit should suffice for prototyping but tighter
  integration will be needed so that updates may be sent to yokozuna
  during events such as read-repair.  I would still do this in the
  form of registering a callback versus coupling the two directly.

* Yokozuna uses a postcommit because the data should exist before the
  index that references it.  This could potentially cause overload
  issues since no indexing back pressure will be provided.  There may
  be ways to deal with this in yokozuna rather than KV such as lagged
  indexing with an append-only log.

### Module Breakdown

* `yz_riak_kv` - All knowledge specific to Riak KV should reside in
  here to keep it isolated.

* `yz_index` - All functionality re indexes such as mapping and
  administrative.

* `yz_solr` - All functionality related to making a request to solr.
  In regards to indexes this module should provide functions to
  administrate cores.

### API Breakdown

* `PUT yz/index?name=<name>&initial_schema=<schema_name>` - create a
  new index with `name` (required) based on the `initial_schema` name
  or the default schema if none is provided.

* `PUT /yz/mapping?alias=<alias>&name=<name>` - create a mapping from
  the `alias` to the index `name`.

* `PUT /yz/kv_hook?bucket=<bucket>&index=<index>&schema=<schema>` -
  install a hook into KV on the `bucket` which maps to `index` and
  uses `schema`.  This subsumes the above two so maybe they aren't
  needed for now.

* `yz_riak_kv:install_kv_hook(Bucket, Index, Schema)` - Same as
  previous but via Erlang.  The HTTP API is so the user can install
  the hook.

### Use Case Rundown

1. User registers yz hook for bucket `B` via `PUT /yz/kv_hook?bucket=B&index=B&schema=default`.

2. User writes value `V` under bucket `B` and key `K`.

3. The `put_fsm` is spun-up, object `O` is created.

4. The quorum is met, the yz hook is called with object `O`.

5. The index is determined by pulling `B` and retrieving registered
   index `I`.

6. The object `O` is converted to the doc `Doc` for storage in Solr.

7. `N` copies of `Doc` are written across `N` shards.


[rdd]: http://tom.preston-werner.com/2010/08/23/readme-driven-development.html
