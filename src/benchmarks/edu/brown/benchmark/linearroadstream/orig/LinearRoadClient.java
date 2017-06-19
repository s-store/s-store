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
package edu.brown.benchmark.linearroadstream.orig;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import edu.brown.api.BenchmarkComponent;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

public class LinearRoadClient extends BenchmarkComponent {
	
	private static final Logger LOG = Logger.getLogger(LinearRoadClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
	
	static AtomicLong batchId = new AtomicLong(0);
	static AtomicLong openTxns = new AtomicLong(0);

    public static void main(String args[]) {
        BenchmarkComponent.main(LinearRoadClient.class, args, false);
    }

    public LinearRoadClient(String[] args) {
        super(args);
        for (String key : m_extraParams.keySet()) {
            // TODO: Retrieve extra configuration parameters
        } // FOR
    }
   
    
    private void parseInput(String tuple, PrintWriter out)
	{
		try {
			long time,vid,qid;
			int spd, xway, lane, dir, seg, pos, part_id, segbegin, segend, day, tod;
			Client client = this.getClientHandle();
    		assert(tuple.length() > 0);
    		String t[] = tuple.split(",");
    		//LOG.info(tuple);
    		int first = new Integer(t[0]);
    		boolean result;
    		Callback cb = new Callback(first, out);
    		long starttime = System.nanoTime();
    		switch(first) {
    		case 0:
    			time = new Long(t[1]);
    			vid = new Long(t[2]);
    			spd = new Integer(t[3]);
    			xway = new Integer(t[4]);
    			lane = new Integer(t[5]);
    			dir = new Integer(t[6]);
    			seg = new Integer(t[7]);
    			pos = new Integer(t[8]);
    			part_id = LinearRoadConstants.findPartId(xway);
    			//LOG.info(tuple + ": xway = " + xway);
    			//0   , 1   , 2    , 3    , 4   , 5   , 6  , 7  , 8  , 9     , 10,    , 11   , 12 , 13 , 14
    			//type, time, carid, speed, xway, lane, dir, seg, pos, queryid, m_init, m_end, dow, tod, day
    			result = client.callStreamProcedure(cb, "InsertPosition", batchId.getAndIncrement(), part_id, time, vid, spd, xway, lane, dir, seg, pos, starttime);
    			//result = client.callStreamProcedure(cb, "InsertPositionOneProc", batchId.getAndIncrement(), part_id, time, vid, spd, xway, lane, dir, seg, pos, starttime);
    			break;
    		/**
    		case 2:	
    			time = new Long(t[1]);
    			vid = new Long(t[2]);
    			qid = new Long(t[9]);
    			//result = client.callProcedure(cb, "GetAccountBalance", time, vid, qid, starttime);
    			break;
    		case 3:	
    			time = new Long(t[1]);
    			vid = new Long(t[2]);
    			qid = new Long(t[9]);
    			xway = new Integer(t[4]);
    			day = new Integer(t[14]);
    			part_id = LinearRoadConstants.findPartId(xway);
    			//result = client.callProcedure(cb, "GetDailyExpenditure", part_id, time, vid, qid, xway, day, starttime);
    			break;
    		case 4:	
    			time = new Long(t[1]);
    			vid = new Long(t[2]);
    			qid = new Long(t[9]);
    			xway = new Integer(t[4]);
    			segbegin = new Integer(t[10]);
    			segend = new Integer(t[11]);
    			day = new Integer(t[12]);
    			tod = new Integer(t[13]);
    			part_id = LinearRoadConstants.findPartId(xway);
    			//LOG.info("GetTravelEstimate: "+part_id+","+time+","+vid+","+qid+","+xway+","+segbegin+","+segend+","+day+","+tod+","+starttime);
    			//result = client.callProcedure(cb, "GetTravelEstimate", part_id, time, vid, qid, xway, segbegin, segend, day, tod, starttime);
    		    break;
    		default:
    			LOG.info("Unable to read incoming tuple: not a recognized tuple type");
    			break;*/
    		}
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			System.err.println("Problem calling procedure");
			e.printStackTrace();
		}
	}
    
    @Override
    public void runLoop() {
    	LOG.info("RUN LOOP");
    	try {
	    	ServerSocket serverSocket = new ServerSocket(LinearRoadConstants.IN_PORT_NUM);
	    	Socket outputSocket = null;
	    	if(LinearRoadConstants.ENABLE_OUT_SOCKET)
	    		outputSocket = new Socket(LinearRoadConstants.OUT_HOST, LinearRoadConstants.OUT_PORT_NUM);
	    		    	
	    	LOG.info("running socketserver");

			Socket clientSocket = serverSocket.accept();
			BufferedReader inputReader;
			if(LinearRoadConstants.READ_FROM_FILE) {
				inputReader = new BufferedReader(new FileReader(LinearRoadConstants.FILE_DIR + "cardatapoints.out0"));
			}
			else {
				inputReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
			}
			String tuple = inputReader.readLine();
			PrintWriter out = null;
			if(outputSocket != null)
				out = new PrintWriter(outputSocket.getOutputStream(), true);
			else {
				out = new PrintWriter(new FileWriter(LinearRoadConstants.OUTPUT_FILE, true));
			}
			out.println(">>>>> " + LinearRoadConstants.NUM_PARTITIONS + " sites : " + LinearRoadConstants.NUMBER_OF_XWAYS + " xways <<<<<<<<<<<<");
			while(!tuple.startsWith("END")) {
				parseInput(tuple, out);
				tuple = inputReader.readLine();
				if(LinearRoadConstants.READ_FROM_FILE)
					Thread.sleep(1);
			}
			while(openTxns.get() > 0) {
				Thread.sleep(1000);
			}
			inputReader.close();
			if(outputSocket != null)
				outputSocket.close();
			clientSocket.close();
			serverSocket.close();
    		
		} catch (IOException e) {
			LOG.info("IOEXCEPTION");
			System.err.println("Unable to process client request");
            e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    @Override
    protected boolean runOnce() throws IOException {
    	return false;
    }

    private class Callback implements ProcedureCallback {
        private final int idx;
        private PrintWriter out;

        public Callback(int idx, PrintWriter o) {
        	this.idx = idx;
    		this.out = o;
        	if(idx >= 0 && idx <= 4 && idx != 1)
        	{
        		openTxns.getAndIncrement();
        	}
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            // Increment the BenchmarkComponent's internal counter on the
            // number of transactions that have been completed
            incrementTransactionCounter(clientResponse, this.idx);
            try {
            	VoltTable[] output = clientResponse.getResults();
            	 if (output != null && output.length > 0) {
 	            	for(int i = 0; i < output[0].getRowCount(); i++) {
 	            		long starttime = output[0].fetchRow(i).getLong(0);
 	            		long endTime = System.nanoTime();
 	            		long emitTime = (endTime - starttime) / 1000000;
 	            		String emitTimeStr = "" + emitTime;
 		            	String tuple = output[0].fetchRow(i).getString(1);
 		            	tuple = tuple.replaceAll(LinearRoadConstants.EMIT_TIME_STR, emitTimeStr);
 		            	if(out != null) 
 		            	{
 		            		if(emitTime >= LinearRoadConstants.LATENCY_THRESHOLD) {
 		            			  out.println(LinearRoadConstants.NUM_PARTITIONS + " sites - " + LinearRoadConstants.NUM_PARTITIONS + ": " + emitTime + " ms |" + tuple);
 		            			  out.flush();
 		            		}
 		            		
 		            	}
 	            	}
            	 }
            	
            	/**
	            if (output != null && output.length > 0) {
	            	for(int i = 0; i < output[0].getRowCount(); i++) {
	            		long starttime = output[0].fetchRow(i).getLong(0);
	            		long endTime = System.nanoTime();
	            		String emitTime = new Long((endTime - starttime) / 1000000).toString();
		            	String tuple = output[0].fetchRow(i).getString(1);
		            	tuple = tuple.replaceAll(LinearRoadConstants.EMIT_TIME_STR, emitTime);
		            	if(out != null) 
		            		out.println(tuple);
	            	}
	            }*/
            }
            finally {
            	openTxns.getAndDecrement();
            }
        }
    } // END CLASS

    @Override
    public String[] getTransactionDisplayNames() {
        // Return an array of transaction names
        String procNames[] = new String[LinearRoadProjectBuilder.PROCEDURES.length];
        for (int i = 0; i < procNames.length; i++) {
            procNames[i] = LinearRoadProjectBuilder.PROCEDURES[i].getSimpleName();
        }
        return (procNames);
    }
    
}
