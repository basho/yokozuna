Security
========

As of version 0.11.0 Yokozuna has support for authentication, securing
HTTP and protocol buffers (PB) communication, and creating ACLs for
schema and index resources. Until sections are added to the official
Riak documentation you can learn more about security in
[Riak pull-request 355][355].

What Security Is and Is Not
---------------------------

Security, in relation to Yokozuna, provides the following:

* Authentication at connection initiation.

* Encrypted channels of communication over HTTP or PB.

* Coarse ACLs for index and schema resources.

Security does NOT provide the following:

* Secure communication between Riak and Solr.

* Authentication at the Jetty container (where Solr is running).

* Fine grained controls over access to individual document fields.

* A panacea for all possible security related issues.

Resources and Permissions
-------------------------

An ACL maps permissions to users for a given resource. Yokozuna
currently has two different resources and two different permissions.

| Resource            | Description                               |
|---------------------|-------------------------------------------|
| index               | A Yokozuna index.                         |
| schema              | A Yokozuna schema.                        |

| Permission          | Description                               |
|---------------------|-------------------------------------------|
| search.admin        | Ability to admin (CRUD) a resource.       |
| search.query        | Ability to query an index.                |

The resources are the objects to control access to and the permissions
are the actions which may be taken on them. Notice that the
permissions are name-spaced with `search.`. This is required because
other Riak sub-systems may one day have notions of `admin` or `query`
and this prevents collision.

Currently the security system will allow any combination of resource
and permission but not all combinations make sense. For example,
giving the query permission on a schema. That makes no sense. Here is
a table covering the legal combinations.

| Permission        | Resource(s)     | Sub-Resource            |
|-------------------|-----------------|-------------------------|
| search.admin      | index           | No                      |
| search.admin      | schema          | No                      |
| search.query      | index           | Yes                     |

The sub-resource section denotes whether or not a sub-resource
identifier can be applies to that combination. Sub-resources are
explained more below.

Usage
-----

This is the general form for granting a user a permission on a resource.

```
riak-admin security grant <permission> ON <resource> [<sub-resource>] TO <user>
```

Three pieces of information are always required: 1) the permission
given, 2) the resource the permission applies to, and 3) the user
receiving the grant. There is an optional piece of information, the
`<sub-resource>` which allows the grant to apply only to a specific
instance of a `<resource>`. For example, granting query on a specific
index rather than all indexes.

Examples
--------

In this example there are 3 users. Below is a list of their names
along with the permissions each should have.

* `ryan` - Is a developer. He needs to write apps that can query any
  index.

* `eric` - Is an administrator. He needs to administrate schemas and
  indexes as well as be able to query one index that he calls his
  own.

* `god` - This user can do anything, because why not.

### Create Users, Add Sources ###

These first steps are not actually Yokozuna specific but are here for
completeness. Once again, more information on security can be found
[here][355].

The first thing to do is create the users.

```
% riak-admin security add-user ryan password=ryan
% riak-admin security add-user eric password=eric
% riak-admin security add-user god password=iruleudrool

% riak-admin security print-users
+--------------------+--------------------+----------------------------------------+------------------------------+
|      username      |       roles        |                password                |           options            |
+--------------------+--------------------+----------------------------------------+------------------------------+
|        ryan        |                    |b26568f01c3d4c004d7e37a845d5e499bfae086c|              []              |
|        eric        |                    |ce490c5443473f143b5167132ef3c70cd0add148|              []              |
|        god         |                    |fac0a884b3d0ad93d013399463b79076784632f4|              []              |
+--------------------+--------------------+----------------------------------------+------------------------------+
```

Next the allowed sources (IP range) for a given user must be specified
along with the authentication method. In this case our users decide to
go with password authentication and allow only local
connections. Mainly because it makes this example easier to write :).

```
% riak-admin security add-source all 127.0.0.1/32 password

% riak-admin security print-sources
+--------------------+------------+----------+----------+
|       users        |    cidr    |  source  | options  |
+--------------------+------------+----------+----------+
|        all         |127.0.0.1/32| password |    []    |
+--------------------+------------+----------+----------+
```

Note that `all` is syntactic sugar that allows me to apply this source
rule to all users rather than typing them out.

### Granting Search Permissions ###

With the users created and sources configured Yokozuna specific grants
can now be applied.

First is `ryan` who can query any index, but that's it.

```
% riak-admin security grant search.query ON index TO ryan

% riak-admin security print-user ryan

Inherited permissions

+--------------------+----------+----------+----------------------------------------+
|        role        |   type   |  bucket  |                 grants                 |
+--------------------+----------+----------+----------------------------------------+

Applied permissions

+----------+----------+----------------------------------------+
|   type   |  bucket  |                 grants                 |
+----------+----------+----------------------------------------+
|  index   |    *     |            search.query             |
+----------+----------+----------------------------------------+
```

Next is `eric` who can admin everything but only query his personal
index.

```
% riak-admin security grant search.admin ON schema TO eric
% riak-admin security grant search.admin ON index TO eric
% riak-admin security grant search.query ON index erics_index TO eric
```

Finally, there is `god` who does whatever she pleases.

```
% riak-admin security grant search.admin ON schema TO god
% riak-admin security grant search.admin,search.query ON index TO god
```

Note that there must NOT be any white-space between permissions when
granting many at once. A comma, and only a comma, must be used for
delineation.

### Creating Indexes ###

The user Ryan should be prevented from creating or listing indexes
based on the permissions given.

```
% curl -i -k --user ryan:ryan -X PUT 'https://127.0.0.1:10012/search/index/ryans_index'
HTTP/1.1 403 Forbidden
Server: MochiWeb/1.1 WebMachine/1.10.5 (jokes are better explained)
Date: Thu, 07 Nov 2013 21:22:16 GMT
Content-Length: 75

Permission denied: User 'ryan' does not have'search.admin' on <<"index">>
```

As expected Ryan cannot create a new index. You'll have to excuse the
funny looking string. There is still some polish to be made to the new
security system. What if Eric tries to create the index?

```
% curl -i -k --user eric:eric -X PUT 'https://127.0.0.1:10012/search/index/ryans_index'
HTTP/1.1 204 No Content
Server: MochiWeb/1.1 WebMachine/1.10.5 (jokes are better explained)
Date: Thu, 07 Nov 2013 21:27:21 GMT
Content-Type: application/json
Content-Length: 0
```

This time a 204 response was received with no content. This should
mean success but to verify the indexes can be listed. First, Ryan will
try listing the indexes.

```
% curl -k --user ryan:ryan 'https://127.0.0.1:10012/search/index'
Permission denied: User 'ryan' does not have'search.admin' on <<"index">>
```

Once again Ryan does not have admin permission for indexes. Only Eric
or god can list indexes.

```
% curl -i -k --user god:iruleudrool 'https://127.0.0.1:10012/search/index'
[{"name":"ryans_index","schema":"_yz_default"}]
```

Finally, Eric will create his own index which he can search.

```
% curl -i -k --user eric:eric -X PUT 'https://127.0.0.1:10012/search/index/erics_index'
HTTP/1.1 204 No Content
Server: MochiWeb/1.1 WebMachine/1.10.5 (jokes are better explained)
Date: Thu, 07 Nov 2013 22:06:55 GMT
Content-Type: application/json
Content-Length: 0
```

### Index Data ###

Before Ryan can start indexing a bucket type must be created to hold
the various buckets.

```
% riak-admin bucket-type create data '{"props":{}}'

% riak-admin bucket-type activate data
```

Now god will be given permissions to change bucket properties on the
`data` bucket type, and Ryan will also need write permissions on his
bucket in order to write data.

```
% riak-admin security grant riak_core.set_bucket,riak_core.get_bucket ON data TO god

% riak-admin security grant riak_kv.put,riak_kv.get ON data ryan TO ryan
```

The first line gives permission to set bucket properties under the
`data` bucket-type. In this case the `data` bucket-type is the
_resource_ and it applies to any _sub-resource_ since none was
specified. The second line grants permission to write data to the the
`ryan` bucket under the `data` type. Now the index may be associated
and data written.

```
% curl -k --user god:iruleudrool -H 'content-type: application/json' -X PUT 'https://127.0.0.1:10012/types/data/buckets/ryan/props' -d '{"props":{"yz_index":"ryans_index"}}'

% curl -s -k --user god:god 'https://127.0.0.1:10012/buckets/ryan/props' | grep -o '"yz_index.*"'
"yz_index":"ryans_index"

% curl -s -k --user ryan:ryan -H 'content-type: text/plain' -X PUT 'https://127.0.0.1:10012/types/data/buckets/ryan/keys/1' -d @- <<EOF
One man's constant is another man's variable.
EOF

% curl -s -k --user ryan:ryan -H 'content-type: text/plain' -X PUT 'https://127.0.0.1:10012/types/data/buckets/ryan/keys/2' -d @- <<EOF
Functions delay binding; data structures induce binding. Moral: Structure data late in the programming process.
EOF

% curl -s -k --user ryan:ryan -H 'content-type: text/plain' -X PUT 'https://127.0.0.1:10012/types/data/buckets/ryan/keys/3' -d @- <<EOF
Syntactic sugar causes cancer of the semicolon.
EOF
```

It appears Ryan has knack for Perlisms. Eric is more of a Dijkstra fan.

```
riak-admin security grant riak_kv.put,riak_kv.get ON data eric TO eric

% curl -k --user god:iruleudrool -H 'content-type: application/json' -X PUT 'https://127.0.0.1:10012/types/data/buckets/eric/props' -d '{"props":{"yz_index":"erics_index"}}'

% curl -s -k --user eric:eric -H 'content-type: text/plain' -X PUT 'https://127.0.0.1:10012/types/data/buckets/eric/keys/1' -d @- <<EOF
Program testing can be used to show the presence of bugs, but never to show their absence!
EOF

% curl -s -k --user eric:eric -H 'content-type: text/plain' -X PUT 'https://127.0.0.1:10012/types/data/buckets/eric/keys/2' -d @- <<EOF
Please don't fall into the trap of believing that I am terribly dogmatic about [the go to statement]. I have the uncomfortable feeling that others are making a religion out of it, as if the conceptual problems of programming could be solved by a simple trick, by a simple form of coding discipline!
EOF

% curl -s -k --user eric:eric -H 'content-type: text/plain' -X PUT 'https://127.0.0.1:10012/types/data/buckets/eric/keys/3' -d @- <<EOF
Several people have told me that my inability to suffer fools gladly is one of my main weaknesses.
EOF
```

### Searching ###

Now Ryan wants to search to words in both both indexes. They can't
both be searched at the same time but Ryan does have permission to
search both in separate requests.

```
% curl -s -k --user ryan:ryan 'https://127.0.0.1:10012/search/query/ryans_index?q=text:structure&wt=json&omitHeader=true' | jsonpp
{
  "response": {
    "numFound": 1,
    "start": 0,
    "maxScore": 0.45273256,
    "docs": [
      {
        "_yz_id": "data_ryan_2_40",
        "_yz_rk": "2",
        "_yz_rt": "data",
        "_yz_rb": "ryan"
      }
    ]
  }
}

% curl -s -k --user ryan:ryan 'https://127.0.0.1:10012/search/query/erics_index?q=text:structure&wt=json&omitHeader=true' | jsonpp
{
  "response": {
    "numFound": 0,
    "start": 0,
    "maxScore": 0.0,
    "docs": []
  }
}
```

Ryan found the work structure once in his corpus of Perlis quotes but
zero times in Eric's collection of Dijkstra. What if Eric wants to
query for a word in both indexes?

```
% curl -s -k --user eric:eric 'https://127.0.0.1:10012/search/query/erics_index?q=text:program&wt=json&omitHeader=true' | jsonpp
{
  "response": {
    "numFound": 1,
    "start": 0,
    "maxScore": 0.396141,
    "docs": [
      {
        "_yz_id": "data_eric_1_6",
        "_yz_rk": "1",
        "_yz_rt": "data",
        "_yz_rb": "eric"
      }
    ]
  }
}

% curl -s -k --user eric:eric 'https://127.0.0.1:10012/search/query/ryans_index?q=text:program&wt=json&omitHeader=true'
Permission denied: User 'eric' does not have'search.query' on {<<"index">>,
                                                               <<"ryans_index">>}

% curl -s -k --user god:iruleudrool 'https://127.0.0.1:10012/search/query/ryans_index?q=text:program&wt=json&omitHeader=true' | jsonpp
{
  "response": {
    "numFound": 0,
    "start": 0,
    "maxScore": 0.0,
    "docs": []
  }
}
```

Eric found one quote with the word "program" in his index but was
denied access to Ryan's index as expected. God, however, can do
anything and was able to query Ryan's index.

This is the gist of how security may be applied in Yokozuna. Once
again, for more information refer to [Riak 355][355].

[355]: HTTPS://github.com/basho/riak/issues/355
