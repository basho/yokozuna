package com.basho.yokozuna.handler.component;

import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.request.SolrQueryRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Translate custom Yokozuna filter query shard params to normal
 * filter query in the case of a non-distributed request.
 *
 * Allow setting filter queries per-node. This allows Yokozuna to
 * apply a filter query to a specific node without requiring a special
 * node field. That, in turn, leads to a smaller index, less memory
 * usage, and less CPU time spent on node filtering.
 *
 * It works by translating custom per-node filter queries into a
 * traditional filter query for the destination node. For example, for
 * a distributed query across 2 shards the `shards` parameter might
 * look like so.
 *
 *   ?shards=10.0.1.100:10014/solr/index,10.0.1.101:10014/solr/index
 *
 * This SearchComponent allows applying a filter query exclusively to
 * each shard by passing a query param with the name `$host:$port` and
 * a value of the filter query to run. For example:
 *
 *   ?10.0.1.100:10014=_yz_pn:1 OR _yz_pn:7 OR ...\
 *   &10.0.1.101:10014=_yz_pn:4 OR _yz_pn:10 OR ...
 *
 */
public class FQShardTranslator extends SearchComponent {
    protected static final Logger log = LoggerFactory.getLogger(FQShardTranslator.class);
    public static final String COMPONENT_NAME = "fq_shard_translator";

    @Override
    public void prepare(ResponseBuilder rb) throws IOException {
        SolrQueryRequest req = rb.req;
        SolrParams params = req.getParams();

        if (!isDistrib(params)) {
            String shardUrl = params.get(ShardParams.SHARD_URL);
            if (shardUrl != null) {
                String hostPort = shardUrl.substring(0, shardUrl.indexOf('/'));
                ModifiableSolrParams mp = new ModifiableSolrParams(params);
                mp.add("fq", params.get(hostPort));
                req.setParams(mp);
            }
        }
    }

    @Override
    public void process(ResponseBuilder rb) throws IOException {
        return;
    }

    @Override
    public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
        return;
    }

    @Override
    public String getDescription() {
        return "Yokozuna's FQ Shard Translator";
    }

    @Override
    public String getSource() {
        return "https://github.com/basho/yokozuna";
    }

    private boolean isDistrib(SolrParams params) {
        // Based on HttpShardHandler because rb.isDistrib is not public.
        boolean distrib = params.getBool("distrib", false);
        String shards = params.get(ShardParams.SHARDS);
        boolean hasShardURL = shards != null;

        return hasShardURL || distrib;
    }

}
