Tagging
==========

Data stored in Riak is opaque to Riak.  It doesn't know anything about
the structure of the data stored in it.  Whether the data be JSON or
JPEG, it's all the same to Riak.

On the contrary, the application storing the data often has intimate
knowledge of the data. The application may want to tag it with
attributes that give additional context. For example, tagging a
picture with information such as who uploaded it and when it was
taken.

This is called _tagging_ in Yokozuna.  It provides the ability to
create additional index entries based on the object's metadata.

HTTP
----------

**NOTE: This is subject to change in the 0.2 release.  The current
        implementation of metadata in Riak requires all tags to be
        prefixed with `x-riak-meta`.**

Tags can be added via custom HTTP headers.  The `x-riak-meta-yz-tags`
header tells Yokozuna which headers to use as tags.  It's a CSV.

    x-riak-meta-yz-tags: x-riak-meta-user_s, x-riak-meta-description_t

    x-riak-meta-user_s: rzezeski
    x-riak-meta-description_t: Federal Hill at dusk.

Yokozuna strips the `x-riak-meta` prefix and lower cases tag names
before indexing.  In this case the tags will be: `{<<"user_s">>,
<<"rzezeski">>}`, and `{<<"description_t">>, <<"Federal Hill at
dusk">>}`.

A query against the description tag would look like so.

    q=description_t:dusk

### Multi-Valued Fields

The tag values are passed verbatim to Solr.  If you want a tag to be
treated as a multi-valued field then you'll have to configure Solr to
do so.  This should be possible via Solr update processing but
probably requires a custom processor.

TODO: Create a custom processor and show example of creating a
      multi-valued field via tagging.

    x-riak-meta-keywords_ss: baltimore, dusk, landscape

