Install
=======

There are three methods for obtaining Yokozuna. Which
method you choose depends on how close to the bleeding edge you want to be.
The easiest and most convenient method is to use official packages. Binary
packages are provided for many different operating systems and
represent the official way to install Riak in production. The other
two methods are the source package and cloning from GitHub. The
source package provides an easy way to build the latest official
Yokozuna tag which should pass all integration tests. This package is
not an official Riak package but instead is a convenient way to test
features or bug fixes which have not made it into an official Riak
release yet. For those that need the latest code then building from
GitHub is the only way. Instructions for all three methods are
provided below.

Official Packages
----------

**WARNING**: The 2.0.0pre11 Riak release is a month older than the
  0.14.0 Yokozuna source release. The latest documentation may be
  incompatible with 2.0.0pre11.

The current Riak 2.0 preview (riak-2.0.0pre11) comes bundled with
Yokozuna 0.13.0 along with some additional commits.

http://docs.basho.com/riak/2.0.0pre11/downloads/

One you have the package you can follow the [install instructions][ii]
on the official Riak documentation site.

If you are a developer and want to build a local dev cluster then you
can follow the instructions for the [five minute install][fmi].

[ii]: http://docs.basho.com/riak/2.0.0pre11/ops/building/installing/
[fmi]: http://docs.basho.com/riak/2.0.0pre11/quickstart/

Source Package
--------------

Approximately every month a release is cut. Thus, Yokozuna development
can outpace official Riak releases. The source package provides an
easy method for building the latest release and thus testing the
newest features and bug fixes. The source package should always pass
all integration tests and be stable as possible. It should not be
deployed in production, however. These releases are more like previews
and compatibility could break between them. Use official packages for
production.

### Requirements ###

* Erlang R15B03 or later, 16B02 is recommended

* Java 1.6 or later, Oracle 7u25 is recommended

* GNU make

* GCC (both C/C++)

### Instructions ###

Download the source package and corresponding md5 from one of the
following locations.

* https://s3.amazonaws.com/yzami/pkgs/src/riak-yokozuna-0.14.0-src.tar.gz

* https://s3.amazonaws.com/yzami/pkgs/src/riak-yokozuna-0.14.0-src.tar.gz.sha1

Download using wget.

	wget http://data.riakcs.net:8080/yokozuna/riak-yokozuna-0.14.0-src.tar.gz
	wget http://data.riakcs.net:8080/yokozuna/riak-yokozuna-0.14.0-src.tar.gz.sha1

Verify the sha1 (might need to use `sha1sum`).

    shasum -a1 -c riak-yokozuna-0.14.0-src.tar.gz.sha1 riak-yokozuna-0.14.0-src.tar.gz

Unpack the archive.

    tar zxvf riak-yokozuna-0.14.0-src.tar.gz

Compile.

	cd riak-yokozuna-0.14.0-src
	make

To deploy Riak-Yokozuna in a production configuration then you'll want
to build a normal release.

	make stage
	sed -e 's/search = off/search = on/' -i.back rel/riak/etc/riak.conf

If you want to develop against multiple nodes on one machine then you
can build a local development cluster.

	make stagedevrel
    for d in dev/dev*; do sed -e 's/search = off/search = on/' -i.back $d/etc/riak.conf; done

At this point creating a cluster is the same as vanilla Riak.  See
[Basic Cluster Setup][bcs] for more details.  The Riak docs are
currently wrong about the port numbers.  If you are using a
`stagedevrel` then use these port numbers.

    Riak HTTP: 10018, 10028, 10038, 10048, 10058
	Riak PB: 10017, 10027, 10037, 10047, 10057

Here is an example of creating a cluster from the `stagedevrel`
release.

    for d in dev/dev*; do $d/bin/riak start; done
	for d in dev/dev*; do $d/bin/riak ping; done
    for d in dev/dev{2,3,4,5}; do $d/bin/riak-admin cluster join dev1@127.0.0.1; done
	./dev/dev1/bin/riak-admin cluster plan
	./dev/dev1/bin/riak-admin cluster commit


[bcs]: http://docs.basho.com/riak/latest/cookbooks/Basic-Cluster-Setup/

Install From GitHub
-----------------

Unless you plan to submit a patch to Yokozuna or require the bleeding
edge it is recommended to use the official packages described above.

Clone Riak.

    git clone git://github.com/basho/riak.git
    cd riak

Compile.

	make

Make `stage` or `stagedevrel`.

	make stagedevrel

Enable Yokozuna.0.

	for d in dev/dev*; do sed -e 's/search = off/search = on/' -i.back $d/etc/riak.conf; done
