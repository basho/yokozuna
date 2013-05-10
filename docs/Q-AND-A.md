Question & Answer
=================

For now this will serve as a place to answer questions that aren't
quite enough to form their own documentation.

How does Yokozuna deal with siblings?
-------------------------------------

Yokozuna indexes **all** siblings.  Yokozuna has a low-level hook into
the vnode that detects any change to an object on-disk.  If this
object has siblings then Yokozuna will iterate all of them and create
a Solr document for each one.  Conversely, when siblings have been
resolved Yokozuna will index the reconciled object and delete the
siblings indexes.

### Implementation Details ###

There is no easy way to determine when an object has gone from having
siblings to not.  Yokozuna is dumb in that if there are no siblings it
**always** sends a delete request to Solr to remove any potential
sibling indexes for that key.  This is sent as a separate HTTP request
than the index write.  There is a branch, `combine-index-and-del` to
merge these requests into one but it hasn't been completed yet.

How does Yokozuna deal with Bitcask expiry?
-------------------------------------------

Yokozuna has no notion of index expiry.  Therefore, as data expires in
Bitcask the Yokozuna indexes will remain.  This means you will queries
that return keys which no longer exist.  Now, AAE will eventually
catch this discrepancy but a) it could take a while and b) it relies
on AAE being enabled.  There is a [post on riak-users][1] that goes
over the interactions between AAE and Bitcask expiry.

That said, it would be fairly easy to add a notion of expiry to
Yokozuna.  A TTL or expiration time could be added to expiry docs.
Then an expiry-server could be added to Yokozuna that ticks every few
seconds and runs a delete-by-query.  The query, of course, would
detect any documents passed their expiration time.  A config would be
added to to set expiry on all docs.

The problem is now there is a Bitcask and Yokozuna expiry config.  An
easy error to add to one and forget the other, or make a copy and
paste error and drop a digit.  It would be nice if expiry was a
riak-level idea and could be set per bucket or even object.

[1]: http://lists.basho.com/pipermail/riak-users_lists.basho.com/2013-April/011919.html
