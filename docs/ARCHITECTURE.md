Architecture
============

This document goes over Yokozuna's architecture.

Yokozuna is an Erlang Application
---------------------------------

In Erlang OTP an "application" is a group of modules and Erlang
processes which together perform a specific task.  The word
application is confusing because most people think of an application
as an entire program such as Emacs or Photoshop.  But Yokozuna is just
a sub-system in Riak itself.  Erlang applications are often
stand-alone but Yokozuna is more like an appendage of Riak.  It
requires other subsystems like Riak Core and KV, but also extends
their functionality by providing search capabilities for KV data.

The point of Yokozuna is to bring more sophisticated and robust query
and search support to Riak.  Many people consider Lucene, and programs
built on top of it such as Solr, as _the standard_ for open source
search.  There are many successful stories built atop Lucene/Solr and
it sets the bar for the feature set developers and users expect.
Meanwhile, Riak has a great story as a highly-available, distributed,
key-value store.  Yokozuna takes advantage of the fact that Riak
already knows how to do the distributed bits, extending its feature
set with that of Solr's.  It takes the strength of each, and combines
them.

Yokozuna is a mediator between Riak and Solr.  There is nothing
stopping a user from deploying these two programs separately but then
they would be responsible for the glue.  That glue can be tricky to
write.  It needs to deal with monitoring, querying, indexing, and
dissemination of information.

Yokozuna knows how to listen for changes in KV data and make the
appropriate changes to indexes that live in Solr.

Yokozuna knows how to take a user query on any node and convert it to
a Solr Distributed Search which will correctly cover the entire index
without overlap in replicas.

Yokozuna knows how to take index creation commands and disseminate
that information across the cluster.

Yokozuna knows how to communicate and monitor the Solr OS process.

Etc.

Solr/JVM OS Process
----------------

Every node in the Riak cluster has a corresponding operating system
(OS) process running a JVM which is hosting Solr on the Jetty
application server.  This OS process is a child of the Erlang OS
process running Riak.

Yokozuna has a `gen_server` process which monitors the JVM OS process.
The code for this server is in `yz_solr_proc`.  When the JVM process
crashes this server crashes, causing it's supervisor to restart it.
If there is more than 1 restart in 45 seconds then the entire Riak
node will be shutdown.  If Yokozuna is enabled and Solr cannot
function for some reason then the Riak node needs to go down so that
the user will notice and take corrective action.

Conversely, the JVM process monitors the Riak process.  If for any
reason Riak goes down hard, e.g. a seg-fault, then the JVM process
will also exit.

This double monitoring along with the crash semantics means that no
process is may exist without the other.  They are either both up or
both down.
