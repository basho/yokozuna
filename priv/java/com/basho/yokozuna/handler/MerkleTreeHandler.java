package com.basho.yokozuna.handler;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Base64;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.RTimer;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrIndexReader;;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides handler logic to iterate over the vector clock
 * data stored in the index.  This is used for anti-entropy.
 */
public class MerkleTreeHandler
    extends RequestHandlerBase
    implements PluginInfoInitialized {

    protected static Logger log = LoggerFactory.getLogger(MerkleTreeHandler.class);
    static final String DEFAULT_CONT = "";
    static final int DEFAULT_N = 1000;

    // Pass info from solrconfig.xml
    public void init(PluginInfo info) {
        init(info.initArgs);
    }

    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
        throws Exception, InstantiationException, IllegalAccessException {

        String contParam = req.getParams().get("continue", DEFAULT_CONT);
        String cont = isContinue(contParam) ?
            decodeCont(contParam) : DEFAULT_CONT;

        // TODO: Make before required
        String before = req.getParams().get("before");
        if (before == null) {
            throw new Exception("Parameter 'before' is required");
        }
        int n = req.getParams().getInt("n", DEFAULT_N);
        Term term = new Term("_vc", cont);
        SolrDocumentList docs = new SolrDocumentList();

        // Add docs here and modify object inline in code
        rsp.add("response", docs);
        TermEnum te = null;

        try {
            SolrIndexSearcher searcher = req.getSearcher();
            SolrIndexReader rdr = searcher.getReader();
            te = rdr.terms(term);

            Term tmp = te.term();

            if (tmp == null) {
                rsp.add("more", false);
                return;
            }

            if (isContinue(cont)) {
                log.debug("continue from " + cont);
                // If a continuation was passed in then need to skip
                // first one since it was already seen.
                te.next();
                tmp = te.term();

                if (tmp == null || ! isVClock(tmp)) {
                    rsp.add("more", false);
                    return;
                }
                log.debug("next " + tmp.field() + " : " + tmp.text());
            }

            String field = null;
            String text = null;
            String[] vals = null;
            String ts = null;
            String docId = null;
            String vectorClock = null;
            int count = 0;
            boolean more = true;

            while(tmp != null && isVClock(tmp) && count < n) {
                field = tmp.field();
                text = tmp.text();
                vals = text.split(" ");
                ts = vals[0];

                // TODO: what if null?
                if (! (ts.compareTo(before) < 0)) {
                    rsp.add("more", false);
                    docs.setNumFound(count);
                    more = false;
                    return;
                }

                docId = vals[1];
                vectorClock = vals[2];
                log.debug(field + " : " + text);
                SolrDocument tmpDoc = new SolrDocument();
                tmpDoc.addField("doc_id", docId);
                tmpDoc.addField("base64_vclock", Base64.encodeBase64String(sha(vectorClock)));
                docs.add(tmpDoc);
                count++;
                te.next();
                tmp = te.term();
            }

            if (count < n) {
                rsp.add("more", false);
            } else {
                // Indicates whether more terms match the query.
                rsp.add("more", more);
                String newCont = Base64.encodeBase64URLSafeString(text.getBytes());
                // The continue context for next req to start where
                // this one finished.
                rsp.add("continuation", newCont);
            }

            docs.setNumFound(count);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (te != null) {
                te.close();
            }
        }
    }


    static String decodeCont(String cont) {
        byte[] bytes = Base64.decodeBase64(cont);
        return new String(bytes);
    }

    static byte[] sha(String s) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA");
        md.update(s.getBytes());
        return md.digest();
    }

    static boolean isContinue(String cont) {
        return ! DEFAULT_CONT.equals(cont);
    }

    static boolean isVClock(Term t) {
        return "_vc".equals(t.field());
    }

    @Override
    public String getDescription() {
        return "vector clock data iterator";
    }

    @Override
    public String getVersion() {
        return "0.0.1";
    }

    @Override
    public String getSourceId() {
        return "TODO: implement getSourceId";
    }

    @Override
    public String getSource() {
        return "TODO: implement getSource";
    }
}