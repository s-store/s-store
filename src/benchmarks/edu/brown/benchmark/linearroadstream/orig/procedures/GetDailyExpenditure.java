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
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;

import edu.brown.benchmark.linearroadstream.orig.LinearRoadConstants;

public class GetDailyExpenditure extends VoltProcedure {
	//TODO: response needs to be (type=3, time, emit time, QID, balance (t <= time)
	public final SQLStmt GetDailyExpenditureStmt = new SQLStmt("SELECT tolls FROM tolls_per_vehicle WHERE part_id = ? AND xway = ? AND VID = ? AND tollday = ?");

	public final SQLStmt insertQueryAuditStmt = new SQLStmt("INSERT INTO query_audit_tbl (part_id, proc_id, proc_name, tod, query_id, query_time) VALUES (?,?,?,?,?,?)");
    
    public long auditQuery(int part_id, long tod, int query_id, long starttime) {
    	if(LinearRoadConstants.AUDIT_QUERIES) {
	    	long queryTime = System.nanoTime() - starttime;
	    	voltQueueSQL(insertQueryAuditStmt, part_id, 3, "GetDailyExpenditure", tod, query_id, queryTime);
	    	voltExecuteSQL();
	    	return System.nanoTime();
    	}
    	else return 0;
    }
	
	
    public VoltTable[] run(int part_id, long time, long vid, long qid, int xway, int day, long starttime) {
    	Long auditTime = System.nanoTime();
        voltQueueSQL(GetDailyExpenditureStmt, part_id, xway, vid, day);
        //voltQueueSQL(GetLastUpdatedTime, part_id, xway);
        VoltTable v[] = voltExecuteSQL();
        
        int tolls = 0;
        if(v[0].getRowCount() > 0)
        {
        	tolls = (int)v[0].fetchRow(0).getLong(0);
        }
        auditTime = auditQuery(part_id, time/60, 1, auditTime);
        //int updatedTime = (int)v[1].fetchRow(0).getLong(0);
        
        //hack to return a string back through a VoltTable
        String o = "3,"+time+","+ LinearRoadConstants.EMIT_TIME_STR + "," + qid + "," + tolls;
                
        return LinearRoadConstants.getOutputVoltTable(starttime, o);
    }
}
