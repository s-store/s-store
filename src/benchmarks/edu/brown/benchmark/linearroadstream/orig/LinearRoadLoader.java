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
import java.io.DataInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

import edu.brown.api.BenchmarkComponent;
import edu.brown.api.Loader;
import edu.brown.catalog.CatalogUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ThreadUtil;

public class LinearRoadLoader extends Loader {

	 	private static final Logger LOG = Logger.getLogger(LinearRoadLoader.class);
	 	private static final LoggerBoolean debug = new LoggerBoolean();
	    static {
	        LoggerUtil.attachObserver(LOG, debug);
	    }
	    
	    private int loadthreads = Math.min(ThreadUtil.availableProcessors(), LinearRoadConstants.MAX_LOAD_THREADS);
	    final AtomicLong fileStatsTotal = new AtomicLong(0);
	    

	    public static void main(String args[]) throws Exception {
	       if (debug.val) LOG.debug("MAIN: " + LinearRoadLoader.class.getName());
	        Loader.main(LinearRoadLoader.class, args, true);
	    }

	    public LinearRoadLoader(String[] args) {
	        super(args);
	        if (debug.val)
	            LOG.debug("CONSTRUCTOR: " + LinearRoadLoader.class.getName());
	        
	        final CatalogContext catalogContext = this.getCatalogContext();
	        
	    }
	    	    
	    public void loadMultiThread(final String tablename, String filename) throws Exception {
	    	final CatalogContext catalogContext = this.getCatalogContext(); 
	        final Table catalog_tbl = catalogContext.getTableByName(tablename);
	        final AtomicLong total = new AtomicLong(0);
	        
	        //try {
		        
	        	String file = LinearRoadConstants.FILE_DIR + filename;
		        BufferedReader br = new BufferedReader(new FileReader(file));
		        String nextline = br.readLine();
		        while(nextline != null) {
		        	String[] tuples = new String[LinearRoadConstants.MAX_ARRAY_SIZE];
		        	for(int curIndex = 0; curIndex < LinearRoadConstants.MAX_ARRAY_SIZE; curIndex++) {
			    		if(nextline == null)
			    		{
			    			if(curIndex == 0)
					    		return;
			    			tuples = Arrays.copyOfRange(tuples,0,curIndex);
			    			break;
			    		}
		        		tuples[curIndex] = nextline;
			        	nextline = br.readLine();
			    	}
				    		        	
			    	final String[] allTuples = tuples;
			    	
			        
			        // Multi-threaded loader
			        final int rows_per_thread = (int)Math.ceil(allTuples.length / (double)this.loadthreads);
			        final List<Runnable> runnables = new ArrayList<Runnable>();
			        for (int i = 0; i < this.loadthreads; i++) {
			            final int thread_id = i;
			            final int start = rows_per_thread * i;
			            final int stop = start + rows_per_thread;
			            runnables.add(new Runnable() {
			                @Override
			                public void run() {
			                    // Create an empty VoltTable handle and then populate it in batches
			                    // to be sent to the DBMS
			                    VoltTable table = CatalogUtil.getVoltTable(catalog_tbl);
			                    Object row[] = new Object[table.getColumnCount()];
		
			                    for (int i = start; i < stop; i++) {
			                    	if(i >= allTuples.length)
			                    		break;
			                    	
			                    	String[] tuple = allTuples[i].split(",");
		
			                    	row[0] = LinearRoadConstants.findPartId(new Integer(tuple[LinearRoadConstants.XWAY_COL]));
			                        // randomly generate strings for each column
			                        for (int col = 0; col < tuple.length; col++) {
			                            row[col+1] = new Long(tuple[col]);
			                        } // FOR
			                        if(tablename.equals(LinearRoadConstants.SEG_STATS_TABLE_NAME))
			                        {
			                        	row[LinearRoadConstants.SEG_STATS_DOW_COL_TABLE] = LinearRoadConstants.getDayOfWeek(new Integer(tuple[LinearRoadConstants.SEG_STATS_ABSDAY_COL]));
			                        }
			                        
			                        table.addRow(row);
		
			                        // insert this batch of tuples
			                        if (table.getRowCount() >= LinearRoadConstants.BATCH_SIZE) {
			                        	//LOG.info("inserting batch " + (i-LinearRoadConstants.BATCH_SIZE) + " - " + i);
			                            loadVoltTable(tablename, table);
			                            total.addAndGet(table.getRowCount());
			                            fileStatsTotal.addAndGet(table.getRowCount());
			                            table.clearRowData();
			                            if (debug.val)
			                                LOG.debug(String.format("[%d] Records Loaded: %6d / %d",
			                                          thread_id, total.get(), allTuples.length));
			                        }
			                    } // FOR
		
			                    // load remaining records
			                    if (table.getRowCount() > 0) {
			                        loadVoltTable(tablename, table);
			                        total.addAndGet(table.getRowCount());
			                        fileStatsTotal.addAndGet(table.getRowCount());
			                        table.clearRowData();
			                        if (debug.val)
			                            LOG.debug(String.format("[%d] Records Loaded: %6d / %d",
			                                      thread_id, total.get(), allTuples.length));
			                    }
			                }
			            });
			        } // FOR
			        ThreadUtil.runGlobalPool(runnables);
		        }
		        br.close();
	        //}
	       // catch (IOException e) {
	       // 	System.err.println("THREAD LOADING FAILED: " + e.getMessage());
	       // }
	    }
	    
	    @Override
	    public void load() {
	    	try {
	    		int numXWays = LinearRoadConstants.NUMBER_OF_XWAYS;
		    	int part_id;
		    	LOG.info("Number of threads: " + this.loadthreads);
		    	/**	REMOVING ALL LARGE TABLE LOADING FOR NOW
		        loadMultiThread(LinearRoadConstants.VEHICLE_TABLE_NAME, LinearRoadConstants.HISTORIC_TOLLS_FILE);
		        //loadMultiThread(LinearRoadConstants.SEG_STATS_TABLE_NAME, "fullxway.historical-stats");
		        fileStatsTotal.getAndSet(0);
		        for(int i = 0; i < numXWays; i++) {
		        	final String filename = "xway" + i + ".historical-stats";
		        	LOG.info("loading " + filename);
		        	loadMultiThread(LinearRoadConstants.SEG_STATS_TABLE_NAME, filename);
		        }
		        */
		        LOG.info("InsertTimestamp");
		        for(int i = 0; i < numXWays; i++) {	  
	        		part_id = LinearRoadConstants.findPartId(i);
	        		this.getClientHandle().callProcedure("InsertTimestamp", part_id, i);
	        	}	 
		        LOG.info("finished loading!");
	    	}
	    	catch (Exception e) {
	    		LOG.info(fileStatsTotal.get() + " tuples loaded");
	    		System.err.println("LOADING FAILED: " + e.getMessage());
	    		System.err.println("CLASS: " + e.getClass().getName());
	    		System.err.println("LOCALIZED MESSAGE" + e.getLocalizedMessage());
	    		e.printStackTrace();
	    		System.err.println(fileStatsTotal.get() + " tuples loaded");
	    	}
	    }
	    
}
