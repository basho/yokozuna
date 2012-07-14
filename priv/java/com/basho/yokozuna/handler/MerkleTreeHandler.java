/*
 * Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
 *
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.basho.yokozuna.handler;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Base64;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrIndexSearcher;
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
    static final BytesRef DEFAULT_CONT = null;
    static final int DEFAULT_N = 1000;
    static final String ENTROPY_DATA_FIELD = "_vc";

    // Pass info from solrconfig.xml
    public void init(PluginInfo info) {
        init(info.initArgs);
    }

    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
        throws Exception, InstantiationException, IllegalAccessException {

        String contParam = req.getParams().get("continue");
        BytesRef cont = contParam != null ?
            decodeCont(contParam) : DEFAULT_CONT;

        // TODO: Make before required in handler config
        String before = req.getParams().get("before");
        if (before == null) {
            throw new Exception("Parameter 'before' is required");
        }
        int n = req.getParams().getInt("n", DEFAULT_N);
        SolrDocumentList docs = new SolrDocumentList();

        // Add docs here and modify object inline in code
        rsp.add("response", docs);

        try {
            SolrIndexSearcher searcher = req.getSearcher();
            AtomicReader rdr = searcher.getAtomicReader();
            BytesRef tmp = null;
            Terms terms = rdr.terms(ENTROPY_DATA_FIELD);
            TermsEnum te = terms.iterator(null);

            if (isContinue(cont)) {
                log.debug("continue from " + cont);

                TermsEnum.SeekStatus status = te.seekCeil(cont, true);

                if (status == TermsEnum.SeekStatus.END) {
                    rsp.add("more", false);
                    return;
                }

                // If this term has already been seen then skip it.
                if (status == TermsEnum.SeekStatus.FOUND) {
                    tmp = te.next();

                    if (endOfItr(tmp)) {
                        rsp.add("more", false);
                        return;
                    }
                }
            }

            String text = null;
            String[] vals = null;
            String ts = null;
            String docId = null;
            String vectorClock = null;
            int count = 0;

            while(!endOfItr(tmp) && count < n) {
                text = tmp.utf8ToString();
                vals = text.split(" ");
                ts = vals[0];

                // TODO: what if null?
                if (! (ts.compareTo(before) < 0)) {
                    rsp.add("more", false);
                    docs.setNumFound(count);
                    return;
                }

                docId = vals[1];
                vectorClock = vals[2];
                log.debug("text: " + text);
                SolrDocument tmpDoc = new SolrDocument();
                tmpDoc.addField("doc_id", docId);
                tmpDoc.addField("base64_vclock", Base64.encodeBase64String(sha(vectorClock)));
                docs.add(tmpDoc);
                count++;
                tmp = te.next();
            }

            if (count < n) {
                rsp.add("more", false);
            } else {
                // Indicates whether more terms match the query.
                rsp.add("more", true);
                String newCont = Base64.encodeBase64URLSafeString(tmp.bytes);
                // The continue context for next req to start where
                // this one finished.
                rsp.add("continuation", newCont);
            }

            docs.setNumFound(count);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    static BytesRef decodeCont(String cont) {
        byte[] bytes = Base64.decodeBase64(cont);
        return new BytesRef(bytes);
    }

    static boolean endOfItr(BytesRef returnValue) {
        return returnValue == null;
    }

    static byte[] sha(String s) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA");
        md.update(s.getBytes());
        return md.digest();
    }

    static boolean isContinue(BytesRef cont) {
        return DEFAULT_CONT != cont;
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
    public String getSource() {
        return "TODO: implement getSource";
    }
}
