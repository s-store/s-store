/***************************************************************************
 *  Copyright (C) 2017 by S-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Portland State University                                              *
 *                                                                         *
 *  Author:  The S-Store Team (sstore.cs.brown.edu)                        *
 *                                                                         *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/

package edu.brown.hstore;

import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.util.AbstractProcessingRunnable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * Special thread that will process ClientResponses and send them back to clients 
 * @author pavlo
 */
public final class TransactionPostProcessor extends AbstractProcessingRunnable<Object[]> {
    private static final Logger LOG = Logger.getLogger(TransactionPostProcessor.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }

    /**
     * 
     * @param hstore_site
     */
    public TransactionPostProcessor(HStoreSite hstore_site,
                                    BlockingQueue<Object[]> queue) {
        super(hstore_site,
              HStoreConstants.THREAD_NAME_POSTPROCESSOR,
              queue,
              hstore_site.getHStoreConf().site.status_exec_info);
    }
    
    @Override
    protected void processingCallback(Object data[]) {
        ClientResponseImpl cresponse = (ClientResponseImpl)data[0];
        @SuppressWarnings("unchecked")
        RpcCallback<ClientResponseImpl> clientCallback = (RpcCallback<ClientResponseImpl>)data[1];
        long initiateTime = (Long)data[2];
        int restartCounter = (Integer)data[3];
        long batchId = (Long)data[4];
        
        assert(cresponse != null);
        assert(clientCallback != null);
        
        if (debug.val)
            LOG.debug(String.format("Processing ClientResponse for txn #%d at partition %d [status=%s]",
                      cresponse.getTransactionId(), cresponse.getBasePartition(), cresponse.getStatus()));
        try {
            this.hstore_site.responseSend(cresponse, clientCallback, batchId, initiateTime, restartCounter);
        } catch (Throwable ex) {
            if (this.isShuttingDown() == false) throw new RuntimeException(ex);
            this.shutdown();
        }
    }
}
