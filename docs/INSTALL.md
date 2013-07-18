Install
=======

Currently there are no platform specific packages.  Yokozuna must be
built from source.

Source Package
--------------

### Requirements ###

* Erlang R15B02, or higher

* JRE 1.6 or later

* GNU make

* GCC (both C/C++)

### Instructions ###

Download the source package and corresponding md5 from one of the
following locations.

* http://data.riakcs.net:8080/yokozuna/riak-yokozuna-0.7.0-src.tar.gz

* http://data.riakcs.net:8080/yokozuna/riak-yokozuna-0.7.0-src.tar.gz.md5

* https://s3.amazonaws.com/yzami/pkgs/src/riak-yokozuna-0.7.0-src.tar.gz

* https://s3.amazonaws.com/yzami/pkgs/src/riak-yokozuna-0.7.0-src.tar.gz.md5

E.g. download from [riakcs.net][rcs].

	wget http://data.riakcs.net:8080/yokozuna/riak-yokozuna-0.7.0-src.tar.gz
	wget http://data.riakcs.net:8080/yokozuna/riak-yokozuna-0.7.0-src.tar.gz.md5

Verify the md5 (note: `md5` might `md5sum`).

	md5 riak-yokozuna-0.7.0-src.tar.gz
	cat riak-yokozuna-0.7.0-src.tar.gz.md5

Unpack the archive.

    tar zxvf riak-yokozuna-0.7.0-src.tar.gz

Compile.

	cd riak-yokozuna-0.7.0-src
	make

To deploy Riak-Yokozuna in a production configuration then you'll want
to build a normal release.

	make stage
	sed -e '/{yokozuna,/,/]}/{s/{enabled, false}/{enabled, true}/;}' -i.back rel/riak/etc/app.config

If you want to develop against multiple nodes on one machine then you
can build a local development cluster.

	make stagedevrel
	for d in dev/dev*; do sed -e '/{yokozuna,/,/]}/{s/{enabled, false}/{enabled, true}/;}' -i.back $d/etc/app.config; done

At this point creating a cluster is the same as vanilla Riak.  See
[Basic Cluster Setup][bcs] for more details.  The Riak docs are
currently wrong about the port numbers.  If you are using a
`stagedevrel` then use these port numbers.

	Riak HTTP: 10018, 10028, 10038, 10048
	Riak PB: 10017, 10027, 10037, 10047

Here is an example of creating a cluster from the `stagedevrel`
release.

    for d in dev/dev*; do $d/bin/riak start; done
	for d in dev/dev*; do $d/bin/riak ping; done
    for d in dev/dev{2,3,4}; do $d/bin/riak-admin cluster join dev1@127.0.0.1; done
	./dev/dev1/bin/riak-admin cluster plan
	./dev/dev1/bin/riak-admin cluster commit


[bcs]: http://docs.basho.com/riak/latest/cookbooks/Basic-Cluster-Setup/

[rcs]: https://www.riakcs.net/

Install From GitHub
-----------------

Unless you plan to submit a patch to Yokozuna or require the
latest-n-greatest it is recommended to use the source package as
described above.

Clone the Yokozuna branch of Riak.

    git clone -b rz-yz-merge-1.4.0 git://github.com/basho/riak.git
    cd riak

Compile.

	make

Make `stage` or `stagedevrel`.

	make stagedevrel

Enable Yokozuna.

	for d in dev/dev*; do sed -e '/{yokozuna,/,/]}/{s/{enabled, false}/{enabled, true}/;}' -i.back $d/etc/app.config; done
