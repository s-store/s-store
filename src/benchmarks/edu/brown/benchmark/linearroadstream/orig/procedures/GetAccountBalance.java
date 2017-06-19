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
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.catalog.Table;

import edu.brown.benchmark.linearroadstream.orig.LinearRoadConstants;

public class GetAccountBalance extends VoltProcedure {
	//TODO: response needs to be (type=2, time, emit time, last updated time, QID, balance (t <= time)
	//public final SQLStmt GetAccountBalanceStmt = new SQLStmt("SELECT part_id, SUM(tolls) FROM tolls_per_vehicle WHERE VID = ? GROUP BY part_id");
	//public final SQLStmt GetAccountBalanceStmt = new SQLStmt("SELECT part_id, tolls FROM tolls_per_vehicle WHERE VID = ?");
	public final SQLStmt GetAccountBalanceStmt = new SQLStmt("SELECT part_id, SUM(tolls) FROM tolls_per_vehicle WHERE part_id = ? AND VID = ? GROUP BY part_id");
	
	
	//public final SQLStmt GetLastUpdatedTime = new SQLStmt("SELECT part_id, max(ts) FROM current_ts GROUP BY part_id");
	public final SQLStmt GetLastUpdatedTime = new SQLStmt("SELECT part_id, max(ts) FROM current_ts WHERE part_id = ? GROUP BY part_id");
	
	public final SQLStmt insertQueryAuditStmt = new SQLStmt("INSERT INTO query_audit_tbl (part_id, proc_id, proc_name, tod, query_id, query_time) VALUES (?,?,?,?,?,?)");
    
    public long auditQuery(int part_id, long tod, int query_id, long starttime) {
    	if(LinearRoadConstants.AUDIT_QUERIES) {
	    	long queryTime = System.nanoTime() - starttime;
	    	voltQueueSQL(insertQueryAuditStmt, part_id, 2, "GetAccountBalance", tod, query_id, queryTime);
	    	voltExecuteSQL();
	    	return System.nanoTime();
    	}
    	else return 0;
    }
	/**
    public VoltTable[] run(long time, long vid, long qid, long starttime) {
    	long auditTime = System.nanoTime();
    	
        voltQueueSQL(GetAccountBalanceStmt, vid);
        voltQueueSQL(GetLastUpdatedTime);
        VoltTable v[] = voltExecuteSQL();
        
        int tolls = 0;
//        for(int i = 0; i < v[0].getRowCount(); i++) {
//        	tolls += (int)v[0].fetchRow(i).getLong(1);
//        }
        for(int i = 0; i < v.length; i++) {
        	if(v[i].getRowCount() == 0)
        		continue;
        	tolls += (int)v[i].fetchRow(0).getLong(1);
        }
        
        int updatedTime = 0;
        for(int i = 0; i < v[1].getRowCount(); i++) {
        	if (updatedTime < (int)v[1].fetchRow(0).getLong(0))
        		updatedTime = (int)v[1].fetchRow(0).getLong(0);
        }
        auditTime = auditQuery(0, time/60, 1, auditTime);
        
        //hack to return a string back through a VoltTable
        String o = "2,"+time+","+ LinearRoadConstants.EMIT_TIME_STR + "," + updatedTime + "," + qid + "," + tolls;
                
        return LinearRoadConstants.getOutputVoltTable(starttime, o);
    }*/
    
    public VoltTable[] run(long time, long vid, long qid, long starttime) {
    	long auditTime = System.nanoTime();
    	
    	for(int i = 0; i < LinearRoadConstants.NUM_PARTITIONS; i++) {
    		voltQueueSQL(GetAccountBalanceStmt, i, vid);
    		voltQueueSQL(GetLastUpdatedTime, i);
    	}
        VoltTable v[] = voltExecuteSQL();
        
        int tolls = 0;
        int updatedTime = 0;

        for(int i = 0; i < v.length; i++) {
        	if(v[i].getRowCount() == 0)
        		continue;
        	if(i%2==0) //even queries are account balance stmts
        		tolls += (int)v[i].fetchRow(0).getLong(1);
        	else if (updatedTime < (int)v[i].fetchRow(0).getLong(0)){ //odd queries are updated time stmts
        		updatedTime = (int)v[i].fetchRow(0).getLong(0);
        	}
        }
        
        auditTime = auditQuery(0, time/60, 1, auditTime);
        
        //hack to return a string back through a VoltTable
        String o = "2,"+time+","+ LinearRoadConstants.EMIT_TIME_STR + "," + updatedTime + "," + qid + "," + tolls;
                
        return LinearRoadConstants.getOutputVoltTable(starttime, o);
    }
}
