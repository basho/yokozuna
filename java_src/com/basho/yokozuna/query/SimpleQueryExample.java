/*
 * This is a simple query example to show that querying Yokozuna with
 * a standard Solr client works.
 *
 * Usage:
 *
 * java -cp priv/solr-jars/WEB-INF/lib/* com.basho.yokozuna.query.SimpleQueryExample BASE_URL INDEX FIELD TERM
 *
 * Example:
 *
 * java -cp priv/solr-jars/WEB-INF/lib/* com.basho.yokozuna.query.SimpleQueryExample http://localhost:8098/search fruit text apple
 */

package com.basho.yokozuna.query;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
//import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.impl.HttpSolrClient.Builder;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;

//@SuppressWarnings("UseOfSystemOutOrSystemErr")
public class SimpleQueryExample {




    public static void main(final String[] args)
            throws SolrServerException, IOException {
        final String baseURL = args[0];
        final String index = args[1];
        final String field = args[2];
        final String term = args[3];

        String prefix = baseURL + "/" + index;
        SolrClient solr = new Builder(prefix).build();
        

        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("qt", "/");
        params.set("q", field + ":" + term);
        //SolrRequest req = new QueryRequest(params);

        QueryResponse resp = solr.query(params);

        System.out.println("resp: " + resp);
    }
}
