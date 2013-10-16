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

import java.io.IOException;

import org.apache.commons.codec.binary.Base64;

import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;

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
 * This class provides handler logic to iterate over the entropy data
 * stored in the index.  This data can be used to build a hash tree to
 * detect entropy.
 */
public class EntropyData
    extends RequestHandlerBase
    implements PluginInfoInitialized {

    protected static Logger log = LoggerFactory.getLogger(EntropyData.class);
    static final BytesRef DEFAULT_CONT = null;
    static final int DEFAULT_N = 1000;
    static final String ENTROPY_DATA_FIELD = "_yz_ed";

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

        int n = req.getParams().getInt("n", DEFAULT_N);

        String partition = req.getParams().get("partition");
        if (partition == null) {
            throw new Exception("Parameter 'partition' is required");
        }

        SolrDocumentList docs = new SolrDocumentList();

        // Add docs here and modify object inline in code
        rsp.add("response", docs);

        try {
            SolrIndexSearcher searcher = req.getSearcher();
            AtomicReader rdr = searcher.getAtomicReader();
            BytesRef tmp = null;
            Terms terms = rdr.terms(ENTROPY_DATA_FIELD);

            if (terms == null) {
                rsp.add("more", false);
                return;
            }

            TermsEnum te = terms.iterator(null);

            if (isContinue(cont)) {
                log.debug("continue from " + cont);

                TermsEnum.SeekStatus status = te.seekCeil(cont, true);

                if (status == TermsEnum.SeekStatus.END) {
                    rsp.add("more", false);
                    return;
                } else if (status == TermsEnum.SeekStatus.FOUND) {
                    // If this term has already been seen then skip it.
                    tmp = te.next();

                    if (endOfItr(tmp)) {
                        rsp.add("more", false);
                        return;
                    }
                } else if (status == TermsEnum.SeekStatus.NOT_FOUND) {
                    tmp = te.next();
                }
            } else {
                tmp = te.next();
            }

            String text = null;
            String[] vals = null;
            String docPartition = null;
            String vsn = null;
            String riakBType = null;
            String riakBName = null;
            String riakKey = null;
            String hash = null;
            int count = 0;
            BytesRef current = null;
            DocsEnum de = null;
            Bits liveDocs = rdr.getLiveDocs();

            while(!endOfItr(tmp) && count < n) {
                if (isLive(liveDocs, te)) {
                    current = BytesRef.deepCopyOf(tmp);
                    text = tmp.utf8ToString();
                    log.debug("text: " + text);
                    vals = text.split(" ");

                    vsn = vals[0];
                    docPartition = vals[1];
                    riakBType = vals[2];
                    riakBName = vals[3];
                    riakKey = vals[4];
                    hash = vals[5];

                    if (partition.equals(docPartition)) {
                        SolrDocument tmpDoc = new SolrDocument();
                        tmpDoc.addField("vsn", vsn);
                        tmpDoc.addField("riak_bucket_type", riakBType);
                        tmpDoc.addField("riak_bucket_name", riakBName);
                        tmpDoc.addField("riak_key", riakKey);
                        tmpDoc.addField("base64_hash", hash);
                        docs.add(tmpDoc);
                        count++;
                    }
                }
                tmp = te.next();
            }

            if (count < n) {
                rsp.add("more", false);
            } else {
                rsp.add("more", true);
                String newCont = Base64.encodeBase64URLSafeString(current.bytes);
                // The continue context for next req to start where
                // this one finished.
                rsp.add("continuation", newCont);
            }

            docs.setNumFound(count);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static boolean isLive(Bits liveDocs, TermsEnum te) throws IOException {
        DocsEnum de = te.docs(liveDocs, null);
        return de.nextDoc() != DocIdSetIterator.NO_MORE_DOCS;
    }

    static BytesRef decodeCont(String cont) {
        byte[] bytes = Base64.decodeBase64(cont);
        return new BytesRef(bytes);
    }

    static boolean endOfItr(BytesRef returnValue) {
        return returnValue == null;
    }

    static boolean isContinue(BytesRef cont) {
        return DEFAULT_CONT != cont;
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
