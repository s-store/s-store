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
package edu.brown.benchmark.tpcdi.varconfig;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;

import edu.brown.api.Loader;
import edu.brown.catalog.CatalogUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ThreadUtil;

public class TPCDILoader extends Loader {

    private static final Logger LOG = Logger.getLogger(TPCDILoader.class);
    private static final boolean d = LOG.isDebugEnabled();
    
 	private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }
    
    private int loadthreads = Math.min(ThreadUtil.availableProcessors(), TPCDIConstants.MAX_LOAD_THREADS);
    private static HashMap<String, AtomicLong> uniqueIDs;
    private static HashMap<Long, Long> brokerIDs;
    private static HashMap<String, Long> secIDs;
    private static HashMap<Long, Long> compIDs;
    private static HashMap<String, Long> compNames;
    private static HashMap<Long, Long> custIDs;
    private static HashMap<Long, Long> acctIDs;
    private static AtomicInteger objectType;
    final AtomicLong fileStatsTotal = new AtomicLong(0);
    

//    public static void main(String args[]) throws Exception {
//        if (d) LOG.debug("MAIN: " + DistributedMoveLoader.class.getName());
//        Loader.main(DistributedMoveLoader.class, args, true);
//    }

    public TPCDILoader(String[] args) {
        super(args);
        if (d) LOG.debug("CONSTRUCTOR: " + TPCDILoader.class.getName());
        uniqueIDs = new HashMap<String, AtomicLong>();
        brokerIDs = new HashMap<Long, Long>();
        secIDs = new HashMap<String, Long>();
        compIDs = new HashMap<Long, Long>();
        compNames = new HashMap<String, Long>();
        custIDs = new HashMap<Long, Long>();
        acctIDs = new HashMap<Long, Long>();
        objectType = new AtomicInteger();
    }
    
    public Object[] parseRow(Table table, String tuple, Object row[], int partId) {
    	String[] parseTuple;
    	String tablename = table.getName();
    	long sk_id;
    	long onesp_id;
    	String symbol;
    	String type;
    	switch(tablename){
    	case TPCDIConstants.DIMTRADE_TABLE:
    		break;
    	case TPCDIConstants.STATUSTYPE_TABLE:
    		parseTuple = tuple.split("\\|");
     		row[0] = parseTuple[0];
    		row[1] = parseTuple[1];
				row[2] = partId;
    		break;
    	case TPCDIConstants.TRADETYPE_TABLE:
    		parseTuple = tuple.split("\\|");
    		row[0] = parseTuple[0];
				row[1] = parseTuple[1];
				row[2] = new Integer(parseTuple[2]);
				row[3] = new Integer(parseTuple[3]);
				row[4] = partId;
 			break;
    	case TPCDIConstants.TRADETXT_TABLE:
    	case TPCDIConstants.DIMDATE_TABLE:
    		parseTuple = tuple.split("\\|");
    		row[0] = new Long(parseTuple[0]);
    		row[1] = parseTuple[1];
    		row[2] = new Integer(parseTuple[3]);
    		row[3] = new Integer(parseTuple[7]);
    		row[4] = new Integer(parseTuple[9]);
    		row[5] = new Integer(parseTuple[11]);
    		row[6] = new Integer(parseTuple[13]);
    		row[7] = new Integer(parseTuple[15]);
				row[8] = partId;
    		break;    		
    	case TPCDIConstants.DIMTIME_TABLE:
    		parseTuple = tuple.split("\\|");
    		row[0] = new Long(parseTuple[0]);
    		row[1] = new Integer(parseTuple[2]);
    		row[2] = new Integer(parseTuple[4]);
    		row[3] = new Integer(parseTuple[6]);
				row[4] = partId;
    		break;    		
    	case TPCDIConstants.DIMBROKER_TABLE://csv HR.csv
    		parseTuple = tuple.split(",");
    		if(!parseTuple[5].equals("314"))
    			return null;
    		sk_id = uniqueIDs.get(tablename).getAndIncrement();
    		onesp_id = new Long(parseTuple[0]);
    		row[0] = sk_id;
    		row[1] = onesp_id;
    		brokerIDs.put(onesp_id, sk_id);    		
    		break;
    	case TPCDIConstants.DIMSECURITY_TABLE:
    		type = tuple.substring(15, 18);
    		symbol = tuple.substring(18, 33);
    		String coNameOrCIK = tuple.substring(160);
    		if(!type.equals("SEC") || secIDs.containsKey(symbol))
        		return null;
    		sk_id = uniqueIDs.get(tablename).getAndIncrement();
    		row[0] = sk_id;
    		row[1] = symbol;
			row[3] = TPCDIUtil.hashCode(tablename, symbol);
    		if(StringUtils.isNumeric(coNameOrCIK)){
    			if(!compIDs.containsKey(new Long(coNameOrCIK))) {
    				System.out.println(coNameOrCIK + " not found in DimCompany!");
    				return null;
    			}
    			row[2] = compIDs.get(new Long(coNameOrCIK));
    		}
    		else {
    			if(!compNames.containsKey(coNameOrCIK)) {
    				System.out.println(coNameOrCIK + " not found in DimCompany!");
    				return null;
    			}
    			row[2] = compNames.get(coNameOrCIK);
    		}
    		secIDs.put(symbol, sk_id);
    		break;
    	case TPCDIConstants.DIMCOMPANY_TABLE:
    		type = tuple.substring(15, 18);
    		
    		if(!type.equals("CMP"))
        		return null;
    		onesp_id = new Long(tuple.substring(78, 88));
    		String name = tuple.substring(18, 78);
    		if(compIDs.containsKey(onesp_id))
    			return null;
    		sk_id = uniqueIDs.get(tablename).getAndIncrement();
    		row[0] = sk_id;
    		row[1] = onesp_id;
    		compIDs.put(onesp_id, sk_id);
    		compNames.put(name, sk_id);
    		break;
    		
    	case TPCDIConstants.DIMCUSTOMER_TABLE://xml CustomerMgmt.xml
    		parseTuple = tuple.split("\\|");
    		type = parseTuple[0];
    		if(!type.equals("NEW"))
    			return null;

    		onesp_id = new Long(parseTuple[2]);
    		sk_id = uniqueIDs.get(tablename).getAndIncrement();
    		//System.out.println(parseTuple[2] + ": " + sk_id + " , " + onesp_id);
    		row[0] = sk_id;
    		row[1] = onesp_id;
    		custIDs.put(onesp_id, sk_id);
    		break;
    		
    	case TPCDIConstants.DIMACCOUNT_TABLE:
    		parseTuple = tuple.split("\\|");
    		type = parseTuple[0];
    		long onesp_customer_id;
			  long onesp_broker_id;
    		
    		if(type.equals("NEW")) {
    			onesp_customer_id = new Long(parseTuple[2]);
    			onesp_broker_id = new Long(parseTuple[34]);
    			onesp_id = new Long(parseTuple[32]);
    		} else if(type.equals("ADDACCT")) {
    			onesp_customer_id = new Long(parseTuple[2]);
    			onesp_broker_id = new Long(parseTuple[5]);
    			onesp_id = new Long(parseTuple[3]);
    		} else {
    			//System.out.println("ACCOUNT NOT NEW OR ADDACCT");
    			return null;
    		}
    		if(!custIDs.containsKey(onesp_customer_id)){
    			System.out.println("WARNING: CUSTOMER ID " + onesp_customer_id +" NOT FOUND");
    			return null;
    		}
    		if(!brokerIDs.containsKey(onesp_broker_id)){
    			System.out.println("WARNING: BROKER ID " + onesp_broker_id +" NOT FOUND");
    			return null;
    		}
    		long new_customer_id = custIDs.get(onesp_customer_id);
    		long new_broker_id = brokerIDs.get(onesp_broker_id);
    		sk_id = uniqueIDs.get(tablename).getAndIncrement();
    		row[0] = sk_id;
    		row[1] = onesp_id;
    		row[2] = new_broker_id;
    		row[3] = new_customer_id;
			row[4] = TPCDIUtil.hashCode(tablename, String.valueOf(onesp_id));
    		acctIDs.put(onesp_id, sk_id);
    		break;
    		
    	}
    	//int part_id = TPCDIUtil.getPartitionID(table, row);
    	//row[row.length-1] = part_id;
    	
    	return row;
    };
    
    public void loadMultiThread(final String tablename, String filename, int partId) throws Exception {
    	final CatalogContext catalogContext = this.getCatalogContext(); 
        final Table catalog_tbl = catalogContext.getTableByName(tablename);
        final AtomicLong total = new AtomicLong(0);
				final int _partId = partId;
        //try {
	        
        	String file = TPCDIConstants.FILE_DIR + filename;
	        BufferedReader br = new BufferedReader(new FileReader(file));
	        String nextline = br.readLine();
	        while(nextline != null) {
	        	String[] tuples = new String[TPCDIConstants.MAX_ARRAY_SIZE];
	        	for(int curIndex = 0; curIndex < TPCDIConstants.MAX_ARRAY_SIZE; curIndex++) {
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
		                    	
		                    	row = new Object[table.getColumnCount()];
		                    	row = parseRow(catalog_tbl, allTuples[i], row, _partId);
		                    	//String[] parseTuple = allTuples[i].split("\\|");
		                		//row[0] = parseTuple[0];
		                		//row[1] = parseTuple[1];
		                    	if(row != null)
		                    		table.addRow(row);
	
		                        // insert this batch of tuples
		                        if (table.getRowCount() >= TPCDIConstants.BATCH_SIZE) {
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
    	String loadfiles[][] = TPCDIConstants.LOADFILES;
        for(int i = 0; i < loadfiles.length; i++){
        	try {
        		AtomicLong startID = new AtomicLong(objectType.getAndIncrement() * 100000000L);
        		uniqueIDs.put(loadfiles[i][0], startID);
        		if(loadfiles[i][1].equals(TPCDIConstants.FINWIRE))
        		{
        			File folder = new File(TPCDIConstants.FILE_DIR);
        			File[] listOfFiles = folder.listFiles();
        			Arrays.sort(listOfFiles);
        			for(File curFile : listOfFiles) {
        				String filename = curFile.getName();
        				if(!filename.contains(TPCDIConstants.FINWIRE) || filename.contains("audit"))
        					continue;


								loadMultiThread(loadfiles[i][0], curFile.getName(), 0);

        			}
        			
        		} else {

							// CMATHIES: Copy Date, Time, StatusType, and TradeType to all partitions.
							if (loadfiles[i][0].equals(TPCDIConstants.DIMDATE_TABLE) || loadfiles[i][0].equals(TPCDIConstants.DIMTIME_TABLE)
									|| loadfiles[i][0].equals(TPCDIConstants.STATUSTYPE_TABLE) || loadfiles[i][0].equals(TPCDIConstants.TRADETYPE_TABLE)) {

								for (int j = 0; j < TPCDIConstants.NUM_PARTITIONS; j++) {
									loadMultiThread(loadfiles[i][0], loadfiles[i][1], j);
								}
							} else {
								loadMultiThread(loadfiles[i][0], loadfiles[i][1], 0);
							}
        		}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        
    }
}
