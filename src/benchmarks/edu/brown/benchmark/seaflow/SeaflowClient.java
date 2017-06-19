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
package edu.brown.benchmark.seaflow;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import edu.brown.benchmark.seaflow.procedures.Steering;
import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.seaflow.procedures.IngestSensorData;
//import edu.brown.benchmark.seaflow.procedures.IngestBAC;
//import edu.brown.benchmark.seaflow.procedures.IngestSFL;
//import edu.brown.benchmark.seaflow.procedures.IngestSFLBAC;
import edu.brown.benchmark.tpcdi.orig.TPCDIConstants;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

public class SeaflowClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(SeaflowClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean();

    // Flags to tell the worker threads to stop or go
    AtomicBoolean warmupComplete = new AtomicBoolean(false);
    AtomicBoolean benchmarkComplete = new AtomicBoolean(false);

    // Seaflow benchmark state
    AtomicLong acceptedVotes = new AtomicLong(0);
    AtomicLong badContestantVotes = new AtomicLong(0);
    AtomicLong badVoteCountVotes = new AtomicLong(0);
    AtomicLong failedVotes = new AtomicLong(0);
    
    final Callback callback = new Callback();
    static AtomicLong batchid = new AtomicLong(0); //monotonically increasing batchId. BatchId is used to track stream workflows.

    public static void main(String args[]) {
        BenchmarkComponent.main(SeaflowClient.class, args, false);
    }

    public SeaflowClient(String args[]) {
        super(args);
    }

    private static long start = System.currentTimeMillis();
    
    @Override
    //"runOnce" is the method that runs by default. This will run the number of times client.txnrate param is set to (1000 per second by default)
    protected boolean runOnce() throws IOException {
    	

    	Client client = this.getClientHandle();
    	
    	String sfl[] = {"SCOPE_7,8/5/2015 6:48:57 AM,-0.05,-0.05,,1.2,1.3"}; //TEST SFL tuple
    	String bac[] = {"SCOPE_7,8/5/2015 6:48:57 AM,1.1,1.2,1.3,1.4,1.5,1.6,1.7,1.8"}; //TEST BAC tuple
    	//String full[] = {"SCOPE_7,8/5/2015 6:48:57 AM,-0.05,-0.05,,1.2,1.3,1.1,1.2,1.3,1.4,1.5,1.6,1.7,1.8"}; //TEST FULL tuple
    	String full[] = {"SCOPE_7,07/25/2015 00:19:46.000000,21.33255,-158.203965,35.1198888889,27.2298888889," +
    			"0.0727550055556,203.160873717,28.5424864781,18.5129572688,0.00138924280766,1.1069461566,1.90855118765," +
    			"2.87082384013"};
    	long bid = batchid.getAndIncrement();
        
        //asynchronous call for stream procedures
        //boolean response = client.callStreamProcedure(  callback,"IngestSFLBAC",bid,bid,sfl,bac);
        boolean response = client.callStreamProcedure(  callback,"IngestSensorData",bid,bid,full);

        // FIXME call steering and write JSON every 5 sec
        if (System.currentTimeMillis() - start > 5000) {
            start = System.currentTimeMillis();

            boolean steeringResp = client.callProcedure( callback, "Steering");
            boolean jsonResp = client.callProcedure ( callback, "GetJSONDataFull");
        }
		
        return response;

    }

    @Override
    //to use "runLoop" instead of "runOnce," set the client.txnrate param to -1 at runtime
    public void runLoop() {

        long start = System.currentTimeMillis();

    	Socket clientSocket = null;
        try {
        	clientSocket = new Socket(SeaflowConstants.STREAMINGESTOR_HOST, SeaflowConstants.STREAMINGESTOR_PORT);
            clientSocket.setSoTimeout(5000);

            BufferedInputStream in = new BufferedInputStream(clientSocket.getInputStream());
            while (true) {
                try {
                	int length = in.read();
                    if (length == -1 || length == 0) {
                        break;
                    }
                    byte[] messageByte = new byte[length];
                    in.read(messageByte);
                    String tuple = new String(messageByte);
                    String[] curTuple = new String[1];
                    curTuple[0] = "SCOPE_7,"+tuple;
                    Client client = this.getClientHandle();
                    long bid = batchid.getAndIncrement();
                    
                    boolean response = client.callStreamProcedure(  callback,"IngestSensorData",bid,bid,curTuple);
                    /**
                    // FIXME call steering and write JSON every 5 sec
                    if (System.currentTimeMillis() - start > 5000) {
                        start = System.currentTimeMillis();

                        boolean steeringResp = client.callProcedure( callback, "Steering");
                        boolean jsonResp = client.callProcedure ( callback, "GetJSONDataFull");
                    }*/
                	
                } catch (Exception e) {
                    failedVotes.incrementAndGet();
                }
            } // WHILE
        } catch (Exception e) {
            // Client has no clean mechanism for terminating with the DB.
            e.printStackTrace();
        }
    }

    @Override
    public String[] getTransactionDisplayNames() {
        // Return an array of transaction names
        String procNames[] = new String[]{
            IngestSensorData.class.getSimpleName(),
            //Steering.class.getSimpleName(),
            //IngestBAC.class.getSimpleName(),
            //IngestSFLBAC.class.getSimpleName(),
        };
        return (procNames);
    }

    private class Callback implements ProcedureCallback {
    	
    	public Callback()
    	{
    		super();
    	}
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            // Increment the BenchmarkComponent's internal counter on the
            // number of transactions that have been completed
            // NOTE: all callbacks will be for the "Vote" procedure. To see how many times other procedures are called, you will need to check the @Statistics system procedure
//            if(clientResponse.getResults()[0].getColumnCount() > 0 && clientResponse.getResults()[0].getColumnName(0).equals("st_rotation")) {
//                incrementTransactionCounter(clientResponse, 1);
//            } else {
                incrementTransactionCounter(clientResponse, 0);
//            if (clientResponse.getResults()[0].getRowCount() > 0) {
//                if(clientResponse.getResults()[0].getColumnName(0).contains("rotation")) {
//
//                }
//            }
//            }
            // Keep track of state (optional)
        	/**
            if (clientResponse.getStatus() == Status.OK) {
                VoltTable results[] = clientResponse.getResults();
                assert(results.length == 1);
                long status = results[0].asScalarLong();

                if (status == SeaflowConstants.VOTE_SUCCESSFUL) {
                    acceptedVotes.incrementAndGet();
                }
                else if (status == SeaflowConstants.ERR_INVALID_CONTESTANT) {
                    badContestantVotes.incrementAndGet();
                }
                else if (status == SeaflowConstants.ERR_VOTER_OVER_VOTE_LIMIT) {
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
