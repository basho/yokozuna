/*
 * Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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

package com.basho.yokozuna.monitor;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kill Solr when stdin closes, as it will when the Erlang VM shuts
 * down or yz_solr_proc exits.
 */
public class Monitor extends Thread {
    protected static final Logger log = LoggerFactory.getLogger(Monitor.class);

    public Monitor() {
        // nothing to init
    }

    public void run() {
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Monitor attempting read on stdin");
                }
                if (System.in.read() < 0) {
                    die();
                }
                if (log.isDebugEnabled()) {
                    log.debug("Monitoring succeeded reading stdin");
                }
            }
            catch (final IOException ioe) {
                die();
            }
    }

    // for dtrace’s sake
    private void die() {
        if (log.isInfoEnabled()) {
            log.info("Yokozuna has exited - shutting down Solr");
        }
        System.exit(0);
    }

    /**
     * Start monitoring stdin in a background thread
     */
    public static Monitor monitor() {
        final Monitor m = new Monitor();
        m.setName("riak superviser thread");
        m.start();
        return m;
    }

    /**
     * Main for testing
     */
    public static void main(String[] args) {
        monitor();

        try {
            while(true) {
                // hang out until thread sees stdin close
                Thread.sleep(1000);
            }
        }
        catch (final InterruptedException ie) {
            // nothing to do but shutdown
        }
    }
}
