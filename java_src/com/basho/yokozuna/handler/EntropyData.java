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
import java.util.Optional;
//import java.util.function.*;

import org.apache.commons.codec.binary.Base64;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.plugin.PluginInfoInitialized;
//import org.codehaus.jackson.map.Serializers;
//import org.jetbrains.annotations.Contract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.codec.binary.Base64;
//import org.apache.lucene.search.IndexSearcher;
//import org.apache.solr.search.BitDocSet;
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

    private static final Logger log = LoggerFactory.getLogger(EntropyData.class);
    private static final BytesRef DEFAULT_CONT = null;
    private static final int DEFAULT_N = 1000;
    // @SuppressWarnings("WeakerAccess")
    static final String ENTROPY_DATA_FIELD = "_yz_ed";

    // Pass info from solrconfig.xml
    public void init(final PluginInfo info) {
        this.init(info.initArgs);
    }

    @Override
    public void handleRequestBody(final SolrQueryRequest req, final SolrQueryResponse response)
        throws SolrException {


        final String contParam = req.getParams().get("continue");
        final BytesRef cont = contParam != null ? decodeCont(contParam) : DEFAULT_CONT;

        final int n = req.getParams().getInt("n", DEFAULT_N);

        final String partition = req.getParams().get("partition");
        if (partition == null) {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Parameter 'partition' is required");
        }

        final SolrDocumentList resultDocs = new SolrDocumentList();
        // Add docs here and modify ‘docs’ inline in code
        response.add("response", resultDocs);

        try {
            final SolrIndexSearcher searcher = req.getSearcher();
            final LeafReader rdr = searcher.getSlowAtomicReader();

            final Optional<Terms> terms = Optional.ofNullable(rdr.terms(this.ENTROPY_DATA_FIELD));

            if (!terms.isPresent()) {
                noMore(response);
                return;
            }

            final TermsEnum te = terms.get().iterator();


            if (isContinue(cont)) {
                if (log.isDebugEnabled()) {
                    log.debug("continue from {}", cont);
                }

                final TermsEnum.SeekStatus sought = te.seekCeil(cont);
                if (log.isDebugEnabled())
                    log.debug("seek({}) → {}", cont, sought);
                switch (sought) {
                case END:
                    noMore(response);
                    return;

                case FOUND:
                    // If this term has already been seen then skip it.
                    if (te.next() == null) {
                        noMore(response);
                        return;
                    }
                    break;

                case NOT_FOUND:
                    te.next();
                    break;
                }
            }
            /* no cont parameter */
            else {
                te.next();
            }

            int count = 0;                   // TODO just do docs.size()?

            BytesRef lastSeen = null;

            {
                final Bits liveDocs = rdr.getLiveDocs();

                for (BytesRef ref = te.term(); ref != null; ref = te.next()) {
                    if (count < n && isLive(liveDocs, te)) {
                        lastSeen = BytesRef.deepCopyOf(ref);
                        Optional<SolrDocument> x = getDocIfPn(partition, lastSeen);
                        if (x.isPresent()) {
                            resultDocs.add(x.get());
                            count++;
                        }
                    }
                }
            } //liveDocs

            if (count < n) {
                noMore(response);
            } else {
                response.add("more", true);

                assert lastSeen != null;

                final String newCont = Base64.encodeBase64URLSafeString(lastSeen.bytes);
                // The continue context for next req to start where
                // this one finished.
                response.add("continuation", newCont);
            }

            resultDocs.setNumFound(count);
        }
        catch (final Exception e) {
            log.error("bad things, {}", e);
        }

    }


    private void noMore(SolrQueryResponse rsp) {
        rsp.add("more", false);
    }


    // @Contract("null, _ -> true")
    private static boolean isLive(final Bits liveDocs, final TermsEnum te) {
        if (liveDocs == null)
            return true;

        try {
            PostingsEnum posts = te.postings(null, PostingsEnum.NONE);
            int firstDid = posts.nextDoc();
            assert firstDid != -1;  // as we've called nextDoc()
            assert firstDid != posts.NO_MORE_DOCS; // there should be one
            boolean alive = liveDocs.get(firstDid);

            {
                // FIXME Is this even possible?
                //       Being overly causious.
                int did;
                boolean firstAlive = alive;
                while ((did = posts.nextDoc()) != posts.NO_MORE_DOCS) {
                    // Solr, are you drunk
                    boolean a = liveDocs.get(did);
                    log.info("Same-terms docs: first was {} [alive={}] and now {} [alive={}]", firstDid, firstAlive, did, a);
                    alive = alive || a;
                }
            }

            if (log.isDebugEnabled()) {
                if (!alive)
                    log.debug("Dead! docid={}", firstDid);
            }
            return alive;
        } catch (IOException e) {
            log.error("cannot get postings for TermsEnum {}", te);
            return false;
        }


    }

    /**
     * Get a {@link SolrDocument} encoded in tmp, if it matches {@param partition}
     */
    private Optional<SolrDocument> getDocIfPn(String partition, BytesRef tmp) {
        final String text = tmp.utf8ToString();
        if (log.isTraceEnabled())
            log.trace("getDoc if p={}", partition);

        final String[] vals = text.split(" ");

        final String docPartition = vals[1];

        /*
          If the partition matches the one we are looking for,
          parse the version, bkey, and object hash from the
          entropy data field (term).
        */
        if (partition.equals(docPartition)) {
            if (log.isDebugEnabled())
                log.debug(" getDoc ｢{}｣", text);

            final String vsn = vals[0];
            final String[] decoded = decodeForVersion(vsn,
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

    private static BytesRef decodeCont(final String cont) {
        final byte[] bytes = org.apache.commons.codec.binary.Base64.decodeBase64(cont);
        return new BytesRef(bytes);
    }

    //    private static boolean endOfItr(final BytesRef returnValue) {
    //        return returnValue == null;
    //    }

    private static boolean isContinue(final BytesRef cont) {
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
     * @param vsn       a String vsn number referring to the item's ed handler version
     * @param riakBType riak bucket-type
     * @param riakBName riak bucket-name
     * @param riakKey   riak key
     * @return a String array consisting of a Bucket Type, Bucket Name, and Riak Key
     */
    private String[] decodeForVersion(String vsn, String riakBType, String riakBName, String riakKey) {
        final String[] bKeyInfo;
        switch (Integer.parseInt(vsn)) {
        case 1:
            bKeyInfo = new String[]{riakBType, riakBName, riakKey};
            break;
        default:
            bKeyInfo = new String[]
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
     * @param base64EncodedVal base64 encoded string
     * @return a string of decoded base64 bytes
     */

    public static String decodeBase64DocPart(String base64EncodedVal) {
        //return new String(DatatypeConverter.parseBase64Binary(
        //                     base64EncodedVal));
        return new String(Base64.decodeBase64(base64EncodedVal));
    }
}

