# Upgrade process for riak-2.2.6

The change [Fd yz cursors ed](https://github.com/basho/yokozuna/pull/744) requires a complete re-index of yokozuna data. 

Suggested update path : 

1. On each node turn off yokozuna in riak.conf.

```
sed -i '' 's/search[ ]*=.*/search=off/' riak.conf
```


2. Stop yokozuna on every cluster node 

```
riak_core_util:multi_rpc_ann([node()|nodes()], application, stop, [yokozuna]). 
```

3. Move the yokozuna directories somewhere for safe keeping. 

3. Upgrade the nodes one by one and start each node


4. Start Attach to a node (`riak attach`), and execute: 

```
riak_core_util:multi_rpc_ann([node()|nodes()], application, stop, [yokozuna]). 
```

5. Wait (days?) to determine if everything worked correctly, and on each node enable yokozuna in the riak.conf.
```
sed -i '' 's/search[ ]*=.*/search=on/' riak.conf
```



