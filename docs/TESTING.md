Testing
==========

## Running Riak Test

Riak Test is a tool for running integration tests against a Riak
cluster.  See the [Riak Test README][rt_readme] for more details.

Clone the riak_test repo with the following command:

    git clone https://github.com/basho/riak_test

Follow the instructions in the [Riak Test README][rt_readme] to build
a `devrel` release and set it up for testing with riak_test.

To successfully run all of the riak_test tests for yokozuna the
basho_bench benchmarking tool is also required. Clone the basho_bench
repo with the following command before running the tests:

    git clone https://github.com/basho/basho_bench

### Add Yokozuna Config

Open `~/.riak_test.config` and add the following to the configuration
stanza to be used for yokozuna testing:

    {basho_bench, "<path-to-basho_bench-repo>"},
    {yz_dir, "<path-to-yokozuna-repo>"},

This will result in a configuration stanza similar to the following:

    {rtdev, [
             {basho_bench, "<path-to-basho_bench-repo>"},
             {yz_dir, "<path-to-yokozuna-repo>"},
             {rt_project, "riak"},
             {rt_harness, rtdev},
             {rtdev_path, [{root,     "/home/you/rt/riak"},
                           {current,  "/home/you/rt/riak/current"},
                           {previous, "/home/you/rt/riak/riak-1.3.2"},
                           {legacy,   "/home/you/rt/riak/riak-1.2.1"}
                          ]}

            ]}.

### Compile Yokozuna Riak Test Files

    cd <path-to-yokozuna>
    make compile-riak-test

At this point you should see `.beam` files in `riak_test/ebin`.

### Run the Test

Finally, run the test.

    cd <path-to-riak_test-repo>
    ./riak_test -c rtdev -d <path-to-yokozuna-repo>/riak_test/ebin/ | tee rt.out

[rt_readme]: https://github.com/basho/riak_test/blob/master/README.md
