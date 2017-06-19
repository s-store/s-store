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

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;

import edu.brown.benchmark.linearroadstream.orig.LinearRoadConstants;

@ProcInfo (
		partitionInfo = "start_seg_stats.part_id:0",
		partitionNum = -1,
		singlePartition = true
	)
public class CalculateSegStats extends VoltProcedure {
	
	protected void toSetTriggerTableName()
	{
		addTriggerTable("start_seg_stats");
	}
	
	public final SQLStmt GetInputStream = new SQLStmt("SELECT xway, tod, ts, seg, dir, add_count, add_speed FROM start_seg_stats WHERE part_id = ? ORDER BY ts LIMIT 1");
	
	public final SQLStmt RemoveFromInputStream = new SQLStmt("DELETE FROM start_seg_stats WHERE part_id = ? AND xway = ? AND tod = ?");
	
	public final SQLStmt GetPrevSegStats = new SQLStmt("SELECT seg, dir, num_cars, total_spd, lav4 FROM cur_seg_stats WHERE part_id = ? AND xway = ? AND tod = ?");
	
	public final SQLStmt GetCurrentAccidents = new SQLStmt("SELECT seg, dir, vid FROM potential_accidents WHERE part_id = ? AND xway = ? AND started_tod <= ? AND ended_tod >= ?");
	
	public final SQLStmt UpdateAccident = new SQLStmt("UPDATE potential_accidents SET ended_tod = ? WHERE part_id = ? AND xway = ? AND seg = ? AND dir = ?");
	
	public final SQLStmt GetLastPositionTOD = new SQLStmt("SELECT tod FROM cur_position WHERE part_id = ? AND xway = ? AND vid = ?");
    
    public final SQLStmt InsertFinalStats = new SQLStmt("INSERT INTO segment_history (part_id, absday, tod, xway, dir, seg, lav, cnt, toll, dow) VALUES (?,?,?,?,?,?,?,?,?,?)");
    
    public final SQLStmt getAllAvgSpds = new SQLStmt("SELECT seg, dir, AVG(avg_spd) FROM cur_avg_speeds WHERE part_id = ? AND xway = ? GROUP BY seg, dir");
    
    public final SQLStmt InsertNewCurStats = new SQLStmt("INSERT INTO cur_seg_stats (part_id, xway, tod, seg, dir, num_cars, total_spd, lav4) VALUES (?,?,?,?,?,?,?,?)");
    
    public final SQLStmt UpdateAvgSpeeds = new SQLStmt("UPDATE cur_avg_speeds SET avg_spd = ? WHERE part_id = ? AND xway = ? and seg = ? and dir = ? and row_id = ?");
    
    public final SQLStmt RemoveOldSegStats = new SQLStmt("DELETE FROM cur_seg_stats WHERE part_id = ? AND xway = ? AND tod < ?");
    
    public final SQLStmt RemoveOldAccidents = new SQLStmt("DELETE FROM potential_accidents WHERE part_id = ? AND xway = ? AND ended_tod < ?");

    public final SQLStmt selectQueryAuditStmt = new SQLStmt("SELECT total_query_time, total_query_count FROM query_audit_tbl WHERE part_id = ? AND proc_id = ? AND query_id = ?");
	
    public final SQLStmt insertQueryAuditStmt = new SQLStmt("INSERT INTO query_audit_tbl (part_id, proc_id, proc_name, query_id, total_query_time, total_query_count, avg_query_time) VALUES (?,?,?,?,?,?,?)");
    
    public final SQLStmt updateQueryAuditStmt = new SQLStmt("UPDATE query_audit_tbl SET total_query_time = ?, total_query_count = ?, avg_query_time = ? WHERE part_id = ? AND proc_id = ? AND query_id = ?");
    
    public long auditQuery(int part_id, long tod, int query_id, long starttime) {
    	if(LinearRoadConstants.AUDIT_QUERIES) {
    		int proc_id = 5;
    		String proc_name = "CalculateSegStats";
	    	long queryTime = (System.nanoTime() - starttime) / 1000;
	    	voltQueueSQL(selectQueryAuditStmt, part_id, proc_id, query_id);
	    	VoltTable v[] = voltExecuteSQL();
	    	if(v[0].getRowCount() == 0)
	    		voltQueueSQL(insertQueryAuditStmt, part_id, proc_id, proc_name, query_id, queryTime, proc_id, queryTime);
	    	else {
	    		long totalQueryTime = v[0].fetchRow(0).getLong(0);
	    		long totalQueryCount = v[0].fetchRow(0).getLong(1);
	    		totalQueryTime += queryTime;
	    		totalQueryCount++;
	    		long newAvgQueryTime = totalQueryTime / totalQueryCount;
	    		voltQueueSQL(updateQueryAuditStmt, totalQueryTime, totalQueryCount, newAvgQueryTime, part_id, proc_id, query_id);
	    	}
	    	voltExecuteSQL();
	    	return System.nanoTime();
    	}
    	else return 0;
    }
    
    public long run(int part_id) {
    	
    	long auditTime = System.nanoTime();
    	//auditTime = auditQuery(part_id, -1, -1, auditTime);
    	part_id = part_id % LinearRoadConstants.NUM_PARTITIONS;
    	
    	voltQueueSQL(GetInputStream, part_id);
    	VoltTable v[] = voltExecuteSQL();
    	
    	if(v[0].getRowCount() == 0)
        	return 0;
    	
    	VoltTableRow row = v[0].fetchRow(0);
    	int xway = (int)row.getLong(0);
    	int tod = (int)row.getLong(1);
    	int ts = (int)row.getLong(2);
    	int seg = (int)row.getLong(3);
    	int dir = (int)row.getLong(4);
    	int add_count = (int)row.getLong(5);
    	int add_speed = (int)row.getLong(6);
    	
		//get the previous minute's segment statistics
		voltQueueSQL(GetPrevSegStats, part_id, xway, tod-1);
		voltQueueSQL(GetCurrentAccidents, part_id, xway, tod-1, tod-1);
		v = voltExecuteSQL();
		auditTime = auditQuery(part_id, tod, 1, auditTime); /////////////////audit 1
		
		boolean allAccidents[] = new boolean[200];
		int allAvgSpds[] = new int[200];
		
		VoltTable prevStats = v[0];
		VoltTable accV = v[1];
		
		for(int i = 0; i < accV.getRowCount(); i++) {
			int tmpSeg = (int)accV.fetchRow(i).getLong(0);
			int tmpDir = (int)accV.fetchRow(i).getLong(1);
			int tmpVID = (int)accV.fetchRow(i).getLong(2);
			if(tmpDir == 0) {
				for(int j = tmpSeg; j > tmpSeg - 5; j--) {
					if(j < 0)
						break;
					allAccidents[j] = true;
				}
			}
			else {
				for(int j = tmpSeg; j < tmpSeg + 5; j++) {
					if(j > 99)
						break;
					allAccidents[100 + j] = true;
				}
			}
			
			voltQueueSQL(GetLastPositionTOD, part_id, xway, tmpVID);
			v = voltExecuteSQL();
			if(v[0].getRowCount() == 0)
				continue;
			int lastTOD = (int)v[0].fetchRow(0).getLong(0);
			if(lastTOD < tod-1) {
				voltQueueSQL(UpdateAccident, tod-1, part_id, xway, tmpSeg, tmpDir);
				voltExecuteSQL();
			}
		}
		
		//move rows from the curstats tbl to 
		for(int i = 0; i < prevStats.getRowCount(); i++) {
			row = prevStats.fetchRow(i);
			int tmpSeg = (int)row.getLong(0);
			int tmpDir = (int)row.getLong(1);
			int tmpNumCars = (int)row.getLong(2);
			int tmpTotalSpd = (int)row.getLong(3);
			int tmpLav4 = (int)row.getLong(4);
			int tmpAvgSpd = (tmpNumCars==0) ? 0 : (tmpTotalSpd/tmpNumCars);
			int tmpLav = LinearRoadConstants.calcLav(tmpNumCars, tmpTotalSpd, tmpLav4);
			boolean accident = allAccidents[tmpDir*100 + tmpSeg];
			int tmpToll = LinearRoadConstants.calcToll(tmpNumCars, tmpLav, accident);
			allAvgSpds[tmpSeg + tmpDir * 100] = tmpAvgSpd;
			voltQueueSQL(InsertFinalStats, part_id, 0, tod-1, xway, tmpDir, tmpSeg, tmpLav, tmpNumCars, tmpToll, LinearRoadConstants.getDayOfWeek(0));
			if((i+1)%LinearRoadConstants.MAX_SQL_BATCH == 0)
				voltExecuteSQL();
		}
		voltExecuteSQL();
		auditTime = auditQuery(part_id, tod, 2, auditTime); /////////////////audit 2
		
		//insert new rows into the new current stats tbl and update the LAV tbl
		voltQueueSQL(getAllAvgSpds, part_id, xway);
		v = voltExecuteSQL();
		
		for(int i = 0; i < v[0].getRowCount(); i++) {
			row = v[0].fetchRow(i);
			int tmpSeg = (int)row.getLong(0);
			int tmpDir = (int)row.getLong(1);
			int tmpLav4 = (int)row.getDouble(2);
			
			if(tmpSeg == seg && tmpDir == dir) 
				voltQueueSQL(InsertNewCurStats, part_id, xway, tod, tmpSeg, tmpDir, add_count, add_speed, tmpLav4);
			else
				voltQueueSQL(InsertNewCurStats, part_id, xway, tod, tmpSeg, tmpDir, 0, 0, tmpLav4);
			voltQueueSQL(UpdateAvgSpeeds, allAvgSpds[tmpDir*100 + tmpSeg], part_id, xway, tmpSeg, tmpDir, (tod-1)%4);
			if((i+1)%(LinearRoadConstants.MAX_SQL_BATCH/2) == 0)
				voltExecuteSQL();
		}
		//garbage collection for the cur_seg_stats and accidents
		voltQueueSQL(RemoveOldSegStats, part_id, xway, tod-1);
		voltQueueSQL(RemoveOldAccidents, part_id, xway, tod);
		voltQueueSQL(RemoveFromInputStream, part_id, xway, tod);
		voltExecuteSQL();
		auditTime = auditQuery(part_id, tod, 3, auditTime); /////////////////audit 3
    	
    	return LinearRoadConstants.CALCULATE_TOLLS_SUCCESSFUL;
    }
}
