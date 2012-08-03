Implementation Notes
==========

Notes on the implementation _before_ it is implemented.  Think of it
something like [readme driven development] [rdd].


Indexing
----------

### Avoid Post-Commit Hook

* The object must be sent `2 * N` times.  It needs to be `N` times for
  the KV write and another `N` times for indexing.  In the worst case
  all of those messages have to traverse the network and in the best
  case `2N - 2` messages have to.  If the object size is 5MB--the
  block size in RiakCS--then 30MB of data must traverse the network,
  15MB of which is redundant.

* Post-commit is executed on the coordinator after the object is
  written and a client reply is sent.  This provides no back pressure
  to the client.

* Post-commit error handling is wrong.  It hides errors and just
  increments a counter kept by stats.  You must alter the logging
  levels at runtime to discover the cause of the errors.

* Post-commit is only invoked during user put requests.  Indexing
  changes also need to occur during read-repair and handoff events.
  Any time the KV object changes the index needs to change as well (at
  minimum the object hash must be updated).

### Add Event Hooks to VNodes

* Gives Yokozuna access to all object modifications.

* Exploits locality, avoids redundant transmission of the object
  across the network.

* Provides back-pressure during client writes.

* Could set the stage for atomic commits between KV and other
  services if that's something we wanted to pursue.

* A downside is that now more is happening on the KV vnode which is a
  high contention point as it is.  Measuring and careful thought is
  needed here.

### Ideas for Implementation

* I'm not sure if this is a generic vnode thing or specific to the KV
  vnode.  Right now I'm leaning towards the latter.

* The events Yokozuna needs to react to: put, read-repair (which is
  ultimately a put), and handoff (which once again is just a put).
  Maybe all I need is a low-level hook into the KV backend put.  Might
  help to think of Yokozuna as a backend to KV that compliments the
  primary backend.  Using a low-level put hook covers all cases since
  it is invoked any time the object is modified.  It also provides
  some future proofing as it should always be the least common
  denominator for any future object mutation (e.g. if some new type of
  event was added to KV that causes the object to be modified).

* Invoke the hook IFF the object has changed and the write to the
  backend was successful.  Have to look at `PrepPutRes` from
  `prepare_put` and `Reply` from `perform_put`.

* The deletion of an object from a backend is a separate code path.
  Need to hook into that as well.

* The handoff object put is a different code path, see
  `do_diffobj_put`.  Need to hook into this.

* Yokozuna handoff piggy-backs KV handoff (by re-indexing on put
  versus sending across Solr index data) and therefore Yokozuna vnode
  handoff is simple matter of dropping the index.  Actually, this is a
  lie.  If the KV vnode doesn't handoff first then an entire partition
  of replicas is lost temporarily.  The Yokozuna vnode needs a way to
  tell the handoff system that it cannot start handoff until the KV
  service for the same partition performs handoff.  This could be done
  by returning `{waiting_for, [riak_kv]}`.  The vnode manager will
  probably have to be modified.

Searching
----------

Solr already provides distributed search.  However, it is up to the
client, in this case Yokozuna, which _shards_ to run the query
against.  The caller specifies the shards and Solr handles the
collating.

The shards should be mutually exclusive if you want the results to be
correct.  If the same doc id appears in the rows returned then Solr
will remove all but one instance.  Which instances Solr removes is
non-deterministic.  In the case where the duplicates aren't in the
rows returned and the total rows matching is greater than those
returned then `numCount` may be incorrect.

This poses a problem for Yokozuna since it replicates documents.  A
document spans multiple shards thus neighboring shards will have
overlapping document sets.  Depending on the number of partitions
(also referred to as _ring size_) and number of nodes it may be
possible to pick a set of shards which contain the entire set of
documents with no overlap.  In most cases, however, overlap cannot be
avoided.

The presence of overlap means that Yokozuna can't simply query a set
of shards.  The overlapping could cause `numCount` to be wildly off.
Yokozuna could use a Solr Core per index/partition combination but
this could cause an explosion in the number of Core instances.  Also,
more core instances means more file descriptor usage and less chance
for Solr to optimize Core usage.  A better approach is to filter the
query.

Riak Core contains code to plan and execute _coverage_ queries.  The
idea is to calculate a set of partitions which when combined covers
the entire set of data.  The list of unique nodes, or shards, and the
list of partitions can be obtained from coverage.  The question is how
to filter the data in Solr using the partitions generated by the
coverage plan?

At write time Yokozuna sends the document to `N` different partitions.
Each partition does a write to it's local Solr Core instance.  A Solr
_document_ is a set of field-value pairs.  Yokozuna can leverage this
fact by adding a partition number field (`_pn`) during the local
write.  A document will be replicated `N` times but each replica will
contain a different `_pn` value based on it's owning partition.  That
takes care of the first half of the problem, getting the partition
data in Solr.  Next it must be filtered on.

The most obvious way to filter on `_pn` is append to the user query.
For example, if the user query is `text:banana` then Yokozuna would
transform it to something like `text:banana AND (_pn:<pn1> OR
_pn:<pn2> ... OR _pn:<pnI>)`.  The new query will only accept
documents that have been stored by the specified partitions.  This
works but a more efficient, and perhaps elegant, method is to use
Solr's _filter query_ mechanism.

Solr's filter query is like a regular query but it does not affect
scoring and it's results are cached.  Since a partition can contain
many documents caching may sound scary.  However the cache value is a
`BitDocSet` which uses a single bit for each document.  That means a
megabyte of memory can cache over 8 million documents.  The resulting
query generated by Yokozuna then looks like the following.

    q=text:banana&fq=_pn:P2 OR _pn:P5 ... OR _pn:P65

It may seem like this is the final solution but there is still one
last problem.  Earlier I said that the covering set of partitions
accounts for all the data.  This is true, but in most cases it
accounts for a little bit more than all the data.  Depending on the
number of partitions (`Q`) and the number of replicas (`N`) there may
be no possible way to select a set of partitions that covers _exactly_
the total set of data.  To be precise, if `N` does not evenly divide
into `Q` then the number of overlapping partitions is `L = N - (Q rem
N)`.  For the defaults of `Q=64` and `N=3` this means `L = 3 - (64 rem
3)` or `L=2`.

To guarantee that only the total set of unique documents is returned
the overlapping partitions must be filtered out.  To do this Yokozuna
takes the original set of partitions and performs a series of
transformations ending with the same list of partitions but with
filtering data attached to each.  Each partition will have either the
value `any` or a list of partitions paired with it.  The value
indicates which of it's replicas to include based on the first
partition that owns it. The value `any` means to include a replica no
matter which partition is the first to own it.  Otherwise the
replica's first owner must be one of the partitions in the include
list.

In order to perform this additional filter the first partition number
must be stored as a field in the document.  This is the purpose of the
`_fpn` field.  Using the final list of partitions, with the filtering
data now added, each `{P, all}` pair can be added as a simple `_pn:P`
to the filter query.  However, a `{P, IFPs}` pair must restrain on the
`_fpn` field as well.  The P and IFPs must be applied together.  If
you don't constrain the IFPs to only apply to P then they will apply
to the entire query and only a subset of the total data will be
returned.  Thus a `{P, [IFP1, IFP2]}` pair will be converted to
`(_pn:P AND (_fpn:IFP1 OR _fpn:IFP2))`.  The final query, achieving
100% accuracy, will look something like the following.

    q=text:banana&fq=_pn:P2 OR _pn:P5 ... OR (_pn:P60 AND (_fpn:60)) OR _pn:63


Index Mapping & Cores
----------

Solr has the notion of a [core] [solr_core] which allows multiple
indexes to live under the same Solr/JVM instance.  This is useful
because it allows isolation of index files as well as schemas and
configuration.  Yokozuna exposes the notion of cores as _indexes_.
Each index has a unique name and maps to **one** core.

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

[solr_core]: http://wiki.apache.org/solr/CoreAdmin
