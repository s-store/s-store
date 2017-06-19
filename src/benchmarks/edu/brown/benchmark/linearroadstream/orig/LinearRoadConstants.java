/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original By: VoltDB Inc.											   *
 *  Ported By:  Justin A. DeBrabant (http://www.cs.brown.edu/~debrabant/)  *                                                                      *
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
package edu.brown.benchmark.linearroadstream.orig;

import java.util.ArrayList;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;


public abstract class LinearRoadConstants {
	
	//file names
	//public static final String FILE_DIR = "/home/john/git/papers/S-Store-VLDB2015/mitsim/output/1xway/";
	//public static final String FILE_DIR = "/home/john/git/papers/S-Store-VLDB2015/mitsim/output/8-small/";
	//public static final String FILE_DIR = "/media/john/Files2/linearroad/xways/8/";
	public static final String FILE_DIR = "/data/jlmeehan/mitsimout/64/";
	public static final String HISTORIC_TOLLS_FILE = "historical-tolls.out";
	public static final String SEGMENT_STATISTICS_FILE = "historical-stats";
	public static final String OUTPUT_FILE = "linearroad-latencies.out";
	
	public static final String VEHICLE_TABLE_NAME = "tolls_per_vehicle";
	public static final String SEG_STATS_TABLE_NAME = "segment_history";
	public static final String EMIT_TIME_STR = "XXXXX";
	
	public static final int  BATCH_SIZE = 1000;
	public static final int  MAX_ARRAY_SIZE = 100000;
	public static final int  XWAY_COL = 2;
	public static final int  SEG_STATS_ABSDAY_COL = 0;
	public static final int  SEG_STATS_DOW_COL_TABLE = 9;
    
	// potential return codes
    public static final long INSERT_POSITION_SUCCESSFUL = 0;
    public static final long DETECT_ACCIDENT_SUCCESSFUL = 1;
    public static final long CALCULATE_TOLLS_SUCCESSFUL = 2;
    public static final long PROCEDURE_SUCCESSFUL = 4;
    public static final long INSERT_POSITION_ERR_BAD_SEG = 5;
    
    public static final int NUMBER_OF_XWAYS = 1;
    public static final int NUM_PARTITIONS = 1;
    
    public static final int REPORT_FREQUENCY = 30;
    public static final int STOPPED_POS_POINTS = 4;
    public static final int MINUTES_PER_DAY = 1440;
    public static final int NUM_MINUTES_HISTORY = 2;
    public static final int ENTRANCE_LANE = 0;
    public static final int EXIT_LANE = 4;
    public static final int MAX_LOAD_THREADS = 8;
    public static final int MAX_SQL_BATCH = 50;
    public static final int LATENCY_THRESHOLD = 1000;
    
    public static final int IN_PORT_NUM = 8800;
    public static final int OUT_PORT_NUM = 8900;
    public static final String OUT_HOST = "localhost";
    public static final boolean AUDIT_QUERIES = false;
    public static final boolean READ_FROM_FILE = false;
    public static final boolean ENABLE_OUT_SOCKET = false;
    
    
    public static int findPartId(int xway)
    {
    	return xway % NUM_PARTITIONS;
    }
    
    public static int getDayOfWeek(int day)
    {
    	return day % 7 + 1;
    }
    
    public static VoltTable[] getOutputVoltTable(long starttime, String out) {
    	ArrayList<String> s = new ArrayList<String>();
    	s.add(out);
        return getOutputVoltTable(starttime, s);
    }
    
    public static int calcLav(int numCars, int totalSpeed, int lav4) {
    	return (numCars==0) ? (4*lav4)/5 : ((totalSpeed/numCars) + 4 * lav4) / 5;
    }
    
    public static int calcToll(int numCars, int lav, boolean accident) {
    	if(numCars > 50 && lav < 40 && !accident)
    		return 2 * (numCars - 50) * (numCars - 50);
    	else
    		return 0;
    }
    
    //hack to return a string back through a VoltTable
    public static VoltTable[] getOutputVoltTable(long starttime, ArrayList<String> out) {
    	ColumnInfo firstColumn = new ColumnInfo("starttime", VoltType.BIGINT);
    	ColumnInfo secondColumn = new ColumnInfo("outstring", VoltType.STRING);
        VoltTable[] output = new VoltTable[1];
        output[0] = new VoltTable(firstColumn,secondColumn);
        for(int i = 0; i < out.size(); i++) {
        	output[0].addRow(starttime, out.get(i));
        }
        return output;
    }
}
