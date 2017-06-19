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
package edu.brown.benchmark.linearroadstream.orig.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;

import edu.brown.benchmark.linearroadstream.orig.LinearRoadConstants;

public class GetTravelEstimate extends VoltProcedure {
	//TODO: response needs to be (type=2, time, emit time, last updated time, QID, balance (t <= time)
	public final SQLStmt GetTravelEstimateStmt = new SQLStmt("SELECT seg, avg(lav), avg(cnt) FROM segment_history WHERE part_id = ? AND xway = ? AND dir = ? AND seg = ? AND dow = ? AND tod = ? GROUP BY seg");

	public final SQLStmt insertQueryAuditStmt = new SQLStmt("INSERT INTO query_audit_tbl (part_id, proc_id, proc_name, tod, query_id, query_time) VALUES (?,?,?,?,?,?)");
    
    public long auditQuery(int part_id, long tod, int query_id, long starttime) {
    	if(LinearRoadConstants.AUDIT_QUERIES) {
	    	long queryTime = System.nanoTime() - starttime;
	    	voltQueueSQL(insertQueryAuditStmt, part_id, 4, "GetTravelEstimate", tod, query_id, queryTime);
	    	voltExecuteSQL();
	    	return System.nanoTime();
    	}
    	else return 0;
    }
	
	
    public VoltTable[] run(int part_id, long time, long vid, long qid, int xway, int segbegin, int segend, int dow, int tod, long starttime) {
    	
    	long auditTime = System.nanoTime();
    	int curtod = tod;
    	if(segbegin > segend){
    		int dir = 1;
    		for(int i = segbegin; i >= segend; i--)
    		{
    			if(i < 0)
    				break;
    			voltQueueSQL(GetTravelEstimateStmt, part_id, xway, dir, i, dow, curtod);
    			curtod++;
    		}
    	}
    	else {
    		int dir = 0;
    		for(int i = segbegin; i <= segend; i++)
    		{
    			if(i > 99)
    				break;
    			voltQueueSQL(GetTravelEstimateStmt, part_id, xway, dir, i, dow, curtod);
    			curtod++;
    		}
    	}
    	VoltTable v[] = voltExecuteSQL();
    	
    	
    	int totalTime = 0;
    	int totalToll = 0;
    	
    	for(int i = 0; i < v.length; i++)
    	{
    		if(v[i].getRowCount() == 0)
    			continue;
    		VoltTableRow row = v[i].fetchRow(0);
    		int lav = (int)row.getDouble(1);
    		int count = (int)row.getDouble(2);
    		
    		totalToll += LinearRoadConstants.calcToll(count, lav, false);
    		
    		if(lav == 0)
    			lav = 1;//if the statistic is zero for some reason, round up to 1
    		totalTime += (int)((1.0 / lav)*60);
    	}
    	auditTime = auditQuery(part_id, tod, 1, auditTime);
    	 
    	//hack to return a string back through a VoltTable
        String o = "4," + qid + "," + totalTime + "," + totalToll;
            
        return LinearRoadConstants.getOutputVoltTable(starttime, o);
    }
}
