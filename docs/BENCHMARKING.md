Running Fruit Benchmark
-----------------------

This goes over high-level steps for running the "fruit" benchmark
which is a syntehtic benchmark for Yokozuna. It's a useful
microbenchmark to quickly verify that changes to Yokozuna have not
caused any obvious regression in performance. There are different ways
you could run this test but I'm going to go over the steps I usually
take.

### Create a benchmark dir with before & after Riak/Yokozuna ###

First you need to create a directory which will contain a devrel build
of Riak/Yokozuna of both before and after the change.

```
mkdir ~/yz-bench
cd ~/yz-bench

git clone git@github.com:basho/riak.git riak-current
git clone git@github.com:basho/riak.git riak-my-change

cd riak-current
make
make stagedevrel
for d in dev/dev*; do sed -e 's/search = off/search = on/' -i.back $d/etc/riak.conf; done
cd ..

cd riak-my-change
make deps
cd deps
rm -rf yokozuna
ln -s ~/path/to/working/copy/of/yokozuna/with/my/change (or you could clone branch)
cd ..
make
make stagedevrel
for d in dev/dev*; do sed -e 's/search = off/search = on/' -i.back $d/etc/riak.conf; done
cd ..
```

### Setup basho bench ###

You'll need a local copy of basho bench. Even if you already have
basho bench cloned you may want to refresh your local copy a rebuild
to get the latest.  Remember, if you have issues nuke deps and start
over.

```
cd ~/work

git clone git@github.com:basho/basho_bench.git
cd basho_bench
make deps
cd deps/riakc
git checkout master
cd ../riak_pb
git checkout develop
make
cd ../..
make
```

### Compile Yokozuna BB driver ###

You shouldn't have to do this every time but you always want to
recompile if updates have been made to the driver.

```
cd ~/work/yokozuna/misc/bench
../../rebar get-deps
../../rebar compile
```

### Setup your fruit benchmark configs ###

After you've done this once you probably won't have to touch these
configs much but if the driver ever changes you may have to tweak the
config.

I've pasted the load and query configs inline below. You'll want to
create two files in your benchmark dir.

```
touch ~/yz-bench/load-fruit.config
touch ~/yz-bench/query-lima.config
```

Here are the contents of the `load-fruit.config`:

```
%% This config will load the cluster with fruit data. It is expected
%% that the bucket has an associated index so that the objects are
%% indexed. This needs to be run before running the query benchmarks.
{mode, max}.
{concurrent, 32}.
{driver, yz_driver}.
{code_paths, ["/root/work/yokozuna/misc/bench"]}.
{secure, false}.
{bucket, {<<"data">>, <<"fruit">>}}.
{index, <<"fruit">>}.
{search_path, "/search/fruit"}.
{pb_conns, [{"127.0.0.1", 10017},
            {"127.0.0.1", 10027},
            {"127.0.0.1", 10037},
            {"127.0.0.1", 10047},
            {"127.0.0.1", 10057}]}.
{http_conns, []}.
{duration, infinity}.
{key_generator, {function,  yz_driver, fruit_key_val_gen, [1000000]}}.
{operations, [{load_fruit_pb, 1}]}.
```

Here is the `query-lima.config`:

```
{mode, max}.
{concurrent, 32}.
{driver, yz_driver}.
{code_paths, ["/root/work/yokozuna/misc/bench"]}.
{secure, false}.
{bucket, {<<"data">>, <<"fruit">>}}.
{index, <<"fruit">>}.
{pb_conns, [{"127.0.0.1", 10017},
            {"127.0.0.1", 10027},
            {"127.0.0.1", 10037},
            {"127.0.0.1", 10047},
            {"127.0.0.1", 10057}]}.
{http_conns, []}.
%% Lima
%%
%% Absolute best case for Riak Search.  A single term query that
%% matches one document.  If Yokozuna can meet or beat this then Riak
%% Search doesn't have a leg to stand on.
%%
%% cardinalities: 1
{duration, 5}.
{operations, [{{search_pb, "korlan", <<"id">>, 1}, 1}]}.
```

You'll want to exit the `code_paths` for both to make sure it points
to your location of `yokozuna/misc/bench`.


### Form a cluster, run the benchmark ###

You'll do this step for the before and after Riak directories.

```
for d in ~/yz-bench/riak-current/dev/dev{1,2,3,4,5}; do $d/bin/riak start; done

for d in ~/yz-bench/riak-current/dev/dev{2,3,4,5}; do $d/bin/riak-admin cluster join dev1@127.0.0.1; done

~/yz-bench/riak-current/dev/dev1/bin/riak-admin transfers (wait until 0 transfers left)

~/yz-bench/riak-current/dev/dev1/bin/riak-admin bucket-type create data '{"props":{}}'

~/yz-bench/riak-current/dev/dev1/bin/riak-admin bucket-type activate data

curl -XPUT 'http://localhost:10018/search/index/fruit'

curl -i -X PUT -H 'content-type: application/json' 'http://localhost:10018/types/data/buckets/fruit/props' -d '{"props":{"search_index":"fruit"}}'
```

At this point you have a 5-node cluster which is ready to accept data
on the `{<<"data">>, <<"fruit">>}` bucket and index it. Now you need
to use BB to load the data.

```
cd ~/work/basho_bench
./basho_bench -d my-change -n before-load-fruit /root/yz-bench/load-fruit.config
```

That may take 10-30 minutes to run depending on your machine. After
it's finished I typically wait a minute or so to let the CPU% calm
down a bit. Recent versions of Riak seem to be a bit wild with usage
of CPU during idle.

Next use BB to run the query. This will take 5 minutes to run.

```
cd ~/work/basho_bench
./basho_bench -d my-change -n before-query-lima /root/yz-bench/query-lima.config
```

Stop the cluster

```
for d in ~/yz-bench/riak-current/dev/dev{1,2,3,4,5}; do $d/bin/riak stop; done
```

Run all thse steps again but this time for the version of
Riak/Yokozuna that includes your change.

### Calc Results ###

In the `yokozuna/misc/bench/bin` dir there are some scripts to help
aggregate results.

```
cd ~/work/basho_bench
for d in my-change/*; do ~/work/yokozuna/yz/misc/bench/bin/calc-mean-thru.sh $d; done
for d in my-change/*; do ~/work/yokozuna/yz/misc/bench/bin/calc-med-latency.sh $d; done
for d in my-change/*; do ~/work/yokozuna/yz/misc/bench/bin/calc-99-latency.sh $d; done
```

Using the results above create a table like below and copy into the
GitHub pull request. One table for load and another for query. The top
line are results for current and bottom for change. The bottom line
should include a % increase/decrease from current.


| Load     | Throughput (Ops/s) | Median (ms)     | 99th (ms)       |
|----------|--------------------|-----------------|-----------------|
|Current   | 2206               | 14.64           | 27.75           |
|My change | 2182 (% inc/dec)   | 14.80 (+/-)     | 27.90 (+/-)     |

| Query    | Throughput (Ops/s) | Median (ms)     | 99th (ms)       |
|----------|--------------------|-----------------|-----------------|
|Current   | 2206               | 14.64           | 27.75           |
|My change | 2182 (% inc/dec)   | 14.80 (+/-)     | 27.90 (+/-)     |

