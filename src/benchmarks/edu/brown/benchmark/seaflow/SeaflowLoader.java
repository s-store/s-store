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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Table;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

import edu.brown.api.Loader;
import edu.brown.catalog.CatalogUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ThreadUtil;

public class SeaflowLoader extends Loader {

    private static final Logger LOG = Logger.getLogger(SeaflowLoader.class);
    private static final boolean d = LOG.isDebugEnabled();
    private int loadthreads = Math.min(ThreadUtil.availableProcessors(), SeaflowConstants.MAX_LOAD_THREADS);
    final AtomicLong fileStatsTotal = new AtomicLong(0);
    private static final LoggerBoolean debug = new LoggerBoolean();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + SeaflowLoader.class.getName());
        Loader.main(SeaflowLoader.class, args, true);
    }

    public SeaflowLoader(String[] args) {
        super(args);
        if (d) LOG.debug("CONSTRUCTOR: " + SeaflowLoader.class.getName());
    }
    
    public Object[] parseRow(Table table, String tuple, Object row[]) {
    	String[] parseTuple;
    	String tablename = table.getName();

    	parseTuple = tuple.split(",");
    	for(int i = 0 ; i < parseTuple.length; i++){
    		parseTuple[i] = parseTuple[i].replace("\n", "");
    	}
    	try{
	    	switch(tablename){
	    	case SeaflowConstants.ARGO_TBL:
	     		row[0] = new Double(parseTuple[0]);
	     		row[1] = new Double(parseTuple[1]);
	     		row[2] = new Integer(parseTuple[2]);
	     		row[3] = new Integer(parseTuple[3]);
	     		if(parseTuple.length > 4 && !parseTuple[4].equals(""))
	     			row[4] = new Double(parseTuple[4]);
	     		if(parseTuple.length > 5 && !parseTuple[5].equals(""))
	     			row[5] = new Double(parseTuple[5]);
	    	}
    	}
    	catch(NumberFormatException e){
    		System.out.println("__________________" + parseTuple[4] + "_______________________");
    	}
    	//int part_id = TPCDIUtil.getPartitionID(table, row);
    	//row[row.length-1] = part_id;
    	
    	return row;
    };
    
    public void loadMultiThread(final String tablename, String filename) throws Exception {
    	final CatalogContext catalogContext = this.getCatalogContext(); 
        final Table catalog_tbl = catalogContext.getTableByName(tablename);
        final AtomicLong total = new AtomicLong(0);
        //try {
	        
        	String file = filename;
	        BufferedReader br = new BufferedReader(new FileReader(file));
	        String nextline = br.readLine();
	        while(nextline != null) {
	        	String[] tuples = new String[SeaflowConstants.MAX_ARRAY_SIZE];
	        	for(int curIndex = 0; curIndex < SeaflowConstants.MAX_ARRAY_SIZE; curIndex++) {
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
		                    	row = parseRow(catalog_tbl, allTuples[i], row);
		                    	//String[] parseTuple = allTuples[i].split("\\|");
		                		//row[0] = parseTuple[0];
		                		//row[1] = parseTuple[1];
		                    	if(row != null)
		                    		table.addRow(row);
	
		                        // insert this batch of tuples
		                        if (table.getRowCount() >= SeaflowConstants.BATCH_SIZE) {
		                        	//LOG.info("inserting batch " + (i-LinearRoadConstants.BATCH_SIZE) + " - " + i);
		                            loadVoltTable(tablename, table);
		                            total.addAndGet(table.getRowCount());
		                            fileStatsTotal.addAndGet(table.getRowCount());
		                            table.clearRowData();
		                            LOG.info(String.format("[%d] Records Loaded: %6d / %d",
		                                          thread_id, total.get(), allTuples.length));
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
    
    public void loadSalinity(String filedir){
    	try {
			BufferedReader br = new BufferedReader(new FileReader(filedir));
			String curline = br.readLine();
	    	int i = 0;
	    	while(curline != null){
	    		String spl[] = curline.split(",");
	    		if(spl.length < 5){
	    			curline = br.readLine();
	    			continue;
	    		}
	    		
	    		double lat = new Double(spl[0]);
	    		double lon = new Double(spl[1]);
	    		int month = new Integer(spl[2]);
	    		int depth = new Integer(spl[3]);
	    		double val = new Double(spl[4]);
	    		this.getClientHandle().callProcedure("InitializeSalRow",
	                    lat,lon,month,depth,val);
	            i++;
	            curline = br.readLine();
	            //break;
			}
	    	br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoConnectionsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ProcCallException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    @Override
    public void load() {

        try {
        	LOG.info("LOAD TEMPERATURE");
        	loadMultiThread(SeaflowConstants.ARGO_TBL, SeaflowConstants.FILE_DIR);
        	
        	LOG.info("LOAD SALINITY");
        	//loadSalinity(SeaflowConstants.FILE_DIR + SeaflowConstants.SAL_FILE);
        	
        	this.getClientHandle().callProcedure("Initialize");
        	//		-75.500,-154.500,6,15,-1.0);
        	
        	
        	//this.getClientHandle().callProcedure("Initialize", "sal",
            //        SeaflowConstants.FILE_DIR + SeaflowConstants.SAL_FILE);
        	LOG.info("FINISHED LOADING");
        	
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
