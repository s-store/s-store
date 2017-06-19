/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original By: VoltDB Inc.											   *
 *  Ported By:  Justin A. DeBrabant (http://www.cs.brown.edu/~debrabant/)  *								   
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
package edu.brown.benchmark.tpcdi.orig;

import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.tpcdi.orig.procedures.SP1SplitTuple;
import edu.brown.benchmark.tpcdi.orig.procedures.SP2GetTypes;
import edu.brown.benchmark.tpcdi.orig.procedures.SP3GetSecurityID;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Logger;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

public class TPCDIClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(TPCDIClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static long lastTime;
    private static int timestamp;
    private static long batchId;
    private static long aId;
    private static int batchCounter;
    private static long[] aIds;
    private static long[] startTimes;
    private static String[] curTuples;

    // Flags to tell the worker threads to stop or go
    AtomicBoolean warmupComplete = new AtomicBoolean(false);
    AtomicBoolean benchmarkComplete = new AtomicBoolean(false);

    // voterdemosstore benchmark state
    AtomicLong acceptedVotes = new AtomicLong(0);
    AtomicLong badContestantVotes = new AtomicLong(0);
    AtomicLong badVoteCountVotes = new AtomicLong(0);
    AtomicLong failedVotes = new AtomicLong(0);

    final Callback callback = new Callback();

//    public static void main(String args[]) {
//        BenchmarkComponent.main(DistributedMoveClient.class, args, false);
//    }

    public TPCDIClient(String args[]) {
        super(args);
        lastTime = System.nanoTime();
        timestamp = 0;
        batchId = 0;
        batchCounter = 0;
        aId = 0;
        aIds = new long[TPCDIConstants.NUM_PER_BATCH];
        startTimes = new long[TPCDIConstants.NUM_PER_BATCH];
        curTuples = new String[15];
    }

    @Override
    public void runLoop() {
        Socket clientSocket = null;

        try {

            clientSocket = new Socket(TPCDIConstants.STREAMINGESTOR_HOST, TPCDIConstants.STREAMINGESTOR_PORT);
            clientSocket.setSoTimeout(5000);

            BufferedInputStream in = new BufferedInputStream(clientSocket.getInputStream());

            while (true) {
                int length = in.read();
                if (length == -1 || length == 0) {
                    break;
                }
                byte[] messageByte = new byte[length];
                in.read(messageByte);
                String tuple = new String(messageByte);
                String[] curTuple = new String[1];
                curTuple[0] = tuple;
                Client client = this.getClientHandle();
                boolean response = client.callProcedure(callback,
                    "SP1SplitTuple",
                    aId,
                    curTuple,
                    0);
                aId++;
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected boolean runOnce() throws IOException {
        return false;
    	/*String tuples[] = TPCDIConstants.SAMPLETUPLES;
    	if(aId >= tuples.length)
    		return false;
    	String[] curTuple = new String[1];
    	curTuple[0] = tuples[(int)aId];
    	Client client = this.getClientHandle();
        boolean response = client.callProcedure(callback,
        										 "SP1SplitTuple",
        										 aId,
                                                 curTuple,
                                                 0);
        aId++;
        return response;
    	*/
    	/**
    	curTuples[(int)aId] = tuples[(int)aId];
    	aId++;
    	
    	if(aId == tuples.length){
    		Client client = this.getClientHandle();
            boolean response = client.callProcedure(callback,
            										 "SP1SplitTuple",
            										 0,
                                                     curTuples,
                                                     0);
            return response;
    	}
        
        return true;*/
    	
    }

    @Override
    public String[] getTransactionDisplayNames() {
        // Return an array of transaction names
        String procNames[] = new String[]{
            SP1SplitTuple.class.getSimpleName(),
            SP2GetTypes.class.getSimpleName(),
            SP3GetSecurityID.class.getSimpleName()
        };
        return (procNames);
    }

    private class Callback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            // Increment the BenchmarkComponent's internal counter on the
            // number of transactions that have been completed
            incrementTransactionCounter(clientResponse, 0);
            /**
            // Keep track of state (optional)
            if (clientResponse.getStatus() == Status.OK) {
                VoltTable results[] = clientResponse.getResults();
                assert(results.length == 1);
                long status = results[0].asScalarLong();
                if (status == TPCDIConstants.VOTE_SUCCESSFUL) {
                    acceptedVotes.incrementAndGet();
                    //incrementTransactionCounter(clientResponse, 1);
                }
                else if (status == TPCDIConstants.ERR_INVALID_CONTESTANT) {
                    badContestantVotes.incrementAndGet();
                }
                else if (status == TPCDIConstants.ERR_VOTER_OVER_VOTE_LIMIT) {
                    badVoteCountVotes.incrementAndGet();
                }
            }
            else if (clientResponse.getStatus() == Status.ABORT_UNEXPECTED) {
                if (clientResponse.getException() != null) {
                    clientResponse.getException().printStackTrace();
                }
                if (debug.val && clientResponse.getStatusString() != null) {
                    LOG.warn(clientResponse.getStatusString());
                }
            }*/
        }
    } // END CLASS
}
