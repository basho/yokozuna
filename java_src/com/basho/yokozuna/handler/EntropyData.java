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
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
//import org.apache.lucene.search.IndexSearcher;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
//import org.apache.solr.search.BitDocSet;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.*;


//import org.apache.lucene.index.AtomicReader;
//import org.apache.lucene.index.DocsEnum;

/**
 * This class provides handler logic to iterate over the entropy data
 * stored in the index.  This data can be used to build a hash tree to
 * detect entropy.
 */
public class EntropyData
    extends RequestHandlerBase
    implements PluginInfoInitialized {

    protected static final Logger log = LoggerFactory.getLogger(EntropyData.class);
    static final BytesRef DEFAULT_CONT = null;
    static final int      DEFAULT_N = 1000;
    static final String   ENTROPY_DATA_FIELD = "_yz_ed";

    // Pass info from solrconfig.xml
    public void init(final PluginInfo info) {
        init(info.initArgs);
    }

    @Override
    public void handleRequestBody(final SolrQueryRequest req, final SolrQueryResponse rsp)
        throws Exception, InstantiationException, IllegalAccessException {

        final String contParam = req.getParams().get("continue");
        final BytesRef cont = contParam != null ?
            decodeCont(contParam) : DEFAULT_CONT;

        final int n = req.getParams().getInt("n", DEFAULT_N);

        final String partition = req.getParams().get("partition");
        if (partition == null) {
            throw new Exception("Parameter 'partition' is required");
        }

        final SolrDocumentList docs = new SolrDocumentList();

        // Add docs here and modify object inline in code
        rsp.add("response", docs);

        try {
            final SolrIndexSearcher searcher = req.getSearcher();
            final LeafReader rdr = searcher.getSlowAtomicReader();

            
            //BitDocSet bset = searcher.getLiveDocs();

            //IndexSearcher isearcher = searcher;
            //LeafReaderContext z = isearcher.leafContext;

            
            final Terms terms = rdr.terms(ENTROPY_DATA_FIELD);

            /*
            LeafReaderContext lrc = rdr.getContext();
            LeafReader lr = lrc.reader();
            BitDocSet bds = searcher.getLiveDocs();
*/


            if (terms == null) {
                rsp.add("more", false);
                return;
            }

            final TermsEnum te = terms.iterator();
            
            BytesRef tmp = null;
            //new BitDocSet

            if (isContinue(cont)) {
                if (log.isDebugEnabled()) {
                    log.debug("continue from " + cont);
                }

                final TermsEnum.SeekStatus status = te.seekCeil(cont);
                log.debug("seek {} -> {}", cont, status);
                switch (status) {
                case END:
                    rsp.add("more", false);
                    return;
                
                case FOUND:
                    // If this term has already been seen then skip it.
                    tmp = te.next();

                    if (endOfItr(tmp)) {
                        rsp.add("more", false);
                        return;
                    }
                    break;
                
                case NOT_FOUND:
                    tmp = te.next();
                    break;
                }
            }
            else {
                tmp = te.next();
            }

            int count = 0;
            BytesRef current = null;
            final Bits liveDocs = rdr.getLiveDocs();
            //BitDocSet sss =            rdr.getLiveDocs();
            
            for (BytesRef ref = tmp; ref != null && count < n; ref = te.next()) {
            //while(!endOfItr(tmp) && count < n) {
                // • live
                if (isLive(liveDocs, te)) {
                    current = BytesRef.deepCopyOf(ref);
                    Optional<SolrDocument> x = getDoc(partition, current);
                    if (x.isPresent()) {
                        docs.add(x.get());
                        count++;
                    }       
                } // isLive(te)

                //tmp = te.next();
            }

            if (count < n) {
                rsp.add("more", false);
            } else {
                rsp.add("more", true);
                final String newCont =
                        org.apache.commons.codec.binary.Base64.encodeBase64URLSafeString(current.bytes);
                // The continue context for next req to start where
                // this one finished.
                rsp.add("continuation", newCont);
            }

            docs.setNumFound(count);

        } catch (final Exception e) {
            log.error("bad things, {}", e);
        }
    }

    static boolean isLive(final Bits liveDocs, final TermsEnum te) {
        //log.debug("is-live {}", te);

        if (liveDocs == null)
            return true;
        try {
        PostingsEnum posts = te.postings(null, PostingsEnum.NONE);
        int did = posts.nextDoc();
        assert did != -1;
        assert did != PostingsEnum.NO_MORE_DOCS;
        assert posts.nextDoc() == PostingsEnum.NO_MORE_DOCS;

        boolean r = liveDocs.get(did);
        log.debug("is-live [{}] => {}", did, r);
        return r;
        } catch (IOException e) {
            log.error("cannot get postings for TermsEnum {}", te);
            return false;
        }
        
        //final DocsEnum de = te.    //docs(liveDocs, null);
        //return de.nextDoc() != DocIdSetIterator.NO_MORE_DOCS;
        
        //return true;
        //te.
    }

    private Optional<SolrDocument> getDoc(String partition, BytesRef tmp) {
        final String text = tmp.utf8ToString();
        if (log.isDebugEnabled()) {
            log.debug("text: " + text);
        }
        final String [] vals = text.split(" ");

        final String docPartition = vals[1];

        /*
          If the partition matches the one we are looking for,
          parse the version, bkey, and object hash from the
          entropy data field (term).
        */
        if (partition.equals(docPartition)) {
            final String vsn = vals[0];

            final String [] decoded = decodeForVersion(vsn,
                                                       vals[2],
                                                       vals[3],
                                                       vals[4]);

            final String hash = vals[5];

            final SolrDocument tmpDoc = new SolrDocument();
            tmpDoc.addField("vsn", vsn);
            tmpDoc.addField("riak_bucket_type", decoded[0]);
            tmpDoc.addField("riak_bucket_name", decoded[1]);
            tmpDoc.addField("riak_key", decoded[2]);
            tmpDoc.addField("base64_hash", hash);
            return Optional.of(tmpDoc);
        }
        return Optional.empty();
    }

    static BytesRef decodeCont(final String cont) {
        final byte[] bytes = org.apache.commons.codec.binary.Base64.decodeBase64(cont);
        return new BytesRef(bytes);
    }

    static boolean endOfItr(final BytesRef returnValue) {
        return returnValue == null;
    }

    static boolean isContinue(final BytesRef cont) {
        return DEFAULT_CONT != cont;
    }

    @Override
    public String getDescription() {
        return "vector clock data iterator";
    }

//    @Override
//    public String getVersion() {
//        return "0.0.1";
//    }

    /**
       @param vsn a String vsn number referring to the item's ed handler version
       @param riakBType riak bucket-type
       @param riakBName riak bucket-name
       @param riakKey riak key
       @return a String array consisting of a Bucket Type, Bucket Name, and Riak Key
    */
    private String [] decodeForVersion(String vsn, String riakBType, String riakBName, String riakKey) {
        final String [] bKeyInfo;
        switch(Integer.parseInt(vsn)) {
            case 1:
                bKeyInfo = new String [] {riakBType, riakBName, riakKey};
                break;
            default:
                bKeyInfo = new String []
                    {
                        decodeBase64DocPart(riakBType),
                        decodeBase64DocPart(riakBName),
                        decodeBase64DocPart(riakKey)
                    };
                break;
        }
        return bKeyInfo;
    }

    /**
       @param base64EncodedVal base64 encoded string
       @return a string of decoded base64 bytes
    */

     public String decodeBase64DocPart(String base64EncodedVal) {
        //return new String(DatatypeConverter.parseBase64Binary(
         //                     base64EncodedVal));
        return new String(Base64.decodeBase64(base64EncodedVal));
    }
}

