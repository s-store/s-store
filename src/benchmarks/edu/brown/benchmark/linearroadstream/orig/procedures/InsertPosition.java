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

import java.util.ArrayList;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;

import edu.brown.benchmark.linearroadstream.orig.LinearRoadConstants;

public class InsertPosition extends VoltProcedure {
	final String outStreamName = "start_seg_stats";
	
	public final SQLStmt GetPrevTS = new SQLStmt("SELECT ts, tod FROM current_ts WHERE part_id = ? AND xway = ?");
	
	public final SQLStmt GetPrevPosition = new SQLStmt("SELECT seg, dir, lane, pos, speed, ts, tod, count_at_pos, toll_to_charge FROM cur_position WHERE part_id = ? AND xway = ? AND vid = ?");
	
	//TODO this query is wrong
	public final SQLStmt getLastMinStats = new SQLStmt("SELECT num_cars, total_spd, lav4 FROM cur_seg_stats WHERE part_id = ? AND xway = ? AND tod = ? AND seg = ? AND dir = ?");
	
	public final SQLStmt getLastMinAccidents = new SQLStmt("SELECT seg FROM potential_accidents WHERE part_id = ? AND xway = ? AND dir = ? AND seg >= ? AND seg <= ? AND started_tod <= ? AND ended_tod >= ?");
	
	public final SQLStmt getPreviousVIDTolls = new SQLStmt("SELECT tolls FROM tolls_per_vehicle WHERE part_id = ? AND vid = ? AND xway = ? AND tollday = ?");
	
	public final SQLStmt updateVIDToll = new SQLStmt("UPDATE tolls_per_vehicle SET tolls = ? WHERE part_id = ? AND vid = ? AND xway = ? AND tollday = ?");
	
	public final SQLStmt insertVIDToll = new SQLStmt("INSERT INTO tolls_per_vehicle (part_id, vid, xway, tollday, tolls) VALUES (?,?,?,?,?)");
	
	//ACCIDENTS
	public final SQLStmt insertAccident = new SQLStmt("INSERT INTO potential_accidents (part_id, xway, dir, seg, pos, num_cars, started_tod, ended_tod, vid) VALUES (?,?,?,?,?,?,?,?,?)");
	
	public final SQLStmt startAccident = new SQLStmt("UPDATE potential_accidents SET num_cars = ?, started_tod = ? WHERE part_id = ? AND xway = ? AND dir = ? AND seg = ? AND pos = ?");
	
	public final SQLStmt updateAccident = new SQLStmt("UPDATE potential_accidents SET num_cars = ? WHERE part_id = ? AND xway = ? AND dir = ? AND seg = ? AND pos = ?");
	
	public final SQLStmt endAccident = new SQLStmt("UPDATE potential_accidents SET num_cars = ?, ended_tod = ? WHERE part_id = ? AND xway = ? AND dir = ? AND seg = ? AND pos = ?");
	
	public final SQLStmt getPotentialAccidents = new SQLStmt("SELECT num_cars, started_tod, ended_tod FROM potential_accidents WHERE part_id = ? AND xway = ? AND dir = ? and seg = ? AND pos = ?");
	//END ACCIDENTS
	
	@StmtInfo( upsertable=true )
	public final SQLStmt upsertPosition = new SQLStmt("INSERT INTO cur_position (part_id, xway, vid, seg, dir, lane, pos, speed, ts, tod, count_at_pos, toll_to_charge) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)");
	
	public final SQLStmt updateCurSegStats = new SQLStmt("UPDATE cur_seg_stats SET num_cars = num_cars + ?, total_spd = total_spd + ? WHERE part_id = ? AND xway = ? and tod = ? and seg = ? and dir = ?");
		
	public final SQLStmt insertSegStatsStream = new SQLStmt("INSERT INTO start_seg_stats (part_id, xway, tod, ts, seg, dir, add_count, add_speed) VALUES (?,?,?,?,?,?,?,?)");
	
    public final SQLStmt UpdateCurrentTimestamp = new SQLStmt("UPDATE current_ts SET tod = ?, ts = ? WHERE xway = ? AND part_id = ?");
    
    public final SQLStmt selectQueryAuditStmt = new SQLStmt("SELECT total_query_time, total_query_count FROM query_audit_tbl WHERE part_id = ? AND proc_id = ? AND query_id = ?");
	
    public final SQLStmt insertQueryAuditStmt = new SQLStmt("INSERT INTO query_audit_tbl (part_id, proc_id, proc_name, query_id, total_query_time, total_query_count, avg_query_time) VALUES (?,?,?,?,?,?,?)");
    
    public final SQLStmt updateQueryAuditStmt = new SQLStmt("UPDATE query_audit_tbl SET total_query_time = ?, total_query_count = ?, avg_query_time = ? WHERE part_id = ? AND proc_id = ? AND query_id = ?");
    
    public final SQLStmt updatePreviousInput = new SQLStmt("UPDATE prev_input SET ts = ?, vid = ?, spd = ?, lane = ?, dir = ?, seg = ?, pos = ? WHERE part_id = ? AND xway = ?");
    
    public final SQLStmt getSegStatsStreamCount = new SQLStmt("SELECT count(*) from start_seg_stats");
    
    public long auditQuery(int part_id, long tod, int query_id, long starttime) {
    	if(LinearRoadConstants.AUDIT_QUERIES) {
    		int proc_id = 1;
    		String proc_name = "InsertPosition";
	    	long queryTime = (System.nanoTime() - starttime) / 1000;
	    	voltQueueSQL(selectQueryAuditStmt, part_id, proc_id, query_id);
	    	VoltTable v[] = voltExecuteSQL();
	    	if(v[0].getRowCount() == 0) {
	    		voltQueueSQL(insertQueryAuditStmt, part_id, proc_id, proc_name, query_id, queryTime, proc_id, queryTime);
	    	}
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
    
    public VoltTable[] run(int part_id, long time, long vid, int spd, int xway, int lane, int dir, int seg, int pos, long starttime) {
    	
    	long auditTime = System.nanoTime();
    	long tod = time / 60;

    	int addSpd = spd;
    	int addCount = 1, curCount = 1;
    	boolean todChange = false;
    	int nextToll = 0;
    	int prevSeg = -1, prevDir = -1, prevPos = -1, prevSpeed = -1, prevCarTS = -1, prevCarTOD = -1, prevCount = -1, prevToll = -1;
    	ArrayList<String> outputTuples = new ArrayList<String>();
    	
    	/**
    	voltQueueSQL(getSegStatsStreamCount);
    	VoltTable[] vss = voltExecuteSQL();
    	
    	if(vss[0].fetchRow(0).getLong(0) != 0)
    	{   		
    		String out = "-1," + time + "," + LinearRoadConstants.EMIT_TIME_STR + "," + vid + "," + seg + "," + dir; 
    		outputTuples.add(out);
    		return LinearRoadConstants.getOutputVoltTable(starttime, outputTuples);
    	}
    	voltQueueSQL(updateQueryAuditStmt, (int)time, (int)vid, spd, lane, dir, seg, pos, part_id, xway);
    	voltExecuteSQL();*/
    	
    	//if not a valid segment
    	if(seg < 0 || seg >= 100 || dir < 0 || dir > 1 || xway > LinearRoadConstants.NUMBER_OF_XWAYS)
    	{
    		String out = "-1," + time + "," + LinearRoadConstants.EMIT_TIME_STR + "," + vid + "," + seg + "," + dir; 
    		outputTuples.add(out);
    		return LinearRoadConstants.getOutputVoltTable(starttime, outputTuples);
    	}
    	
    	voltQueueSQL(GetPrevTS, part_id, xway);
    	voltQueueSQL(GetPrevPosition, part_id, xway, vid);
    	VoltTable[] v = voltExecuteSQL();
    	
    	long prevTS = v[0].fetchRow(0).getLong(0);
    	long prevTOD = v[0].fetchRow(0).getLong(1);
    	
    	if(prevTOD < tod)
    		todChange = true;
    	
    	//if the vehicle is NOT just entering the highway
    	if(lane != 0 && v[1].getRowCount() > 0) {
    		prevSeg = (int)v[1].fetchRow(0).getLong("seg");
    		prevDir = (int)v[1].fetchRow(0).getLong("dir");
    		prevPos = (int)v[1].fetchRow(0).getLong("pos");
    		prevSpeed = (int)v[1].fetchRow(0).getLong("speed");
    		prevCarTS = (int)v[1].fetchRow(0).getLong("ts");
    		prevCarTOD = (int)v[1].fetchRow(0).getLong("tod");
    		prevCount = (int)v[1].fetchRow(0).getLong("count_at_pos");
    		prevToll = (int)v[1].fetchRow(0).getLong("toll_to_charge");
    	}
    	auditTime = auditQuery(part_id, tod, 1, auditTime); /////////////audit 1
    	
    	///////////if we're entering a new segment/////////////////////
    	if(seg != prevSeg) {
    		/////////////////////notifications for tolls and accidents//////////////////////////
    		voltQueueSQL(getLastMinStats, part_id, xway, tod - 1, seg, dir);
    		if(dir == 0)
    			voltQueueSQL(getLastMinAccidents, part_id, xway, dir, seg, seg + 4, tod - 1, tod - 1);
    		else
    			voltQueueSQL(getLastMinAccidents, part_id, xway, dir, seg - 4, seg, tod - 1, tod - 1);
    		v = voltExecuteSQL();
    		
    		if(lane != LinearRoadConstants.EXIT_LANE) {
    			boolean upcomingAccident = (v[1].getRowCount() == 0) ? false : true;
	    		if(v[0].getRowCount() != 0) {
	    			int numCars = (int)v[0].fetchRow(0).getLong(0);
	    			int totalSpeed = (int)v[0].fetchRow(0).getLong(1);
	    			int lav4 = (int)v[0].fetchRow(0).getLong(2);
	    			int lav = LinearRoadConstants.calcLav(numCars, totalSpeed, lav4); //recompute the lav each time the recent statistic is needed
	    			nextToll = LinearRoadConstants.calcToll(numCars, lav, upcomingAccident);
	    			if(nextToll > 0) {
	    				String s = "0," + time + "," + LinearRoadConstants.EMIT_TIME_STR + "," + lav + "," + nextToll;
	    				outputTuples.add(s);
	    			}
	    		}
	    		
	    		for(int i = 0; i < v[1].getRowCount(); i++) {
	    			String s = "1," + time + "," + LinearRoadConstants.EMIT_TIME_STR + "," + v[0].fetchRow(i).getLong(0);
	    			outputTuples.add(s);
	    		}
    		}
    		///////////////////end notifications/////////////////////////
    		
    		///////////////////add to account balance////////////////////
    		if(prevToll > 0) {
    			voltQueueSQL(getPreviousVIDTolls, part_id, vid, xway, 0);
    			v = voltExecuteSQL();
	    		//if there is already an account row for this vid on this xway
	    		if(v[0].getRowCount() > 0){
	    			voltQueueSQL(updateVIDToll, prevToll, part_id, vid, xway, 0);
	    		}
	    		else {
	    			voltQueueSQL(insertVIDToll, part_id, vid, xway, 0, prevToll);
	    		}
	    		voltExecuteSQL();
    		}
    		///////////////////end add to account balance/////////////////
    		auditTime = auditQuery(part_id, tod, 2, auditTime); /////////////audit 2
    	}
    	else {
    		////////////////////look for accidents/////////////////////
    		if(prevPos == pos) {
    			curCount = curCount + prevCount;
    		}
    		
    		if(prevCount >= 3) {
    			voltQueueSQL(getPotentialAccidents, part_id, xway, dir, seg, pos);
    			v = voltExecuteSQL();
    			boolean foundAccident = false;
    			int num_cars = 0;
    			int started_tod = 9999999;
    			int ended_tod = 9999999;
    			if(v[0].getRowCount() > 0) {
    				foundAccident = true;
    				num_cars = (int)v[0].fetchRow(0).getLong(0);
    				started_tod = (int)v[0].fetchRow(0).getLong(1);
    				ended_tod = (int)v[0].fetchRow(0).getLong(2);
    			}
    			
    			if(prevPos == pos) {
    				if(prevCount == 3) { //this car is new
    					if (num_cars == 1) //one car has been 
    						voltQueueSQL(startAccident, num_cars + 1, tod, part_id, xway, dir, seg, pos);
    					else if (num_cars > 1) //two cars already exist
    						voltQueueSQL(updateAccident, num_cars + 1, part_id, xway, dir, seg, pos);
    					else if (num_cars == 0 && !foundAccident) //this is the first car
    						voltQueueSQL(insertAccident, part_id, xway, dir, seg, pos, 1, started_tod, ended_tod, vid);
    				}
    			}
    			else {
    				if(num_cars == 2) { //this is one of the two remaining cars in the accident
    					voltQueueSQL(endAccident, num_cars - 1, tod, part_id, xway, prevDir, prevSeg, prevPos);
    				}
    				else if(foundAccident){
    					voltQueueSQL(updateAccident, num_cars - 1, part_id, xway, prevDir, prevSeg, pos);
    				}
    			}
    			voltExecuteSQL();
    		}
    		///////////////////end look for accidents////////////////////
    		
    		if(prevTOD == tod) {
    			addSpd = (spd + prevSpeed)/2 - prevSpeed;
    			addCount = 0;
    		}
    		auditTime = auditQuery(part_id, tod, 3, auditTime); /////////////audit 3
    	}
    	
    	voltQueueSQL(upsertPosition, part_id, xway, vid, seg, dir, lane, pos, spd, time, tod, curCount, nextToll);
    	
    	if(todChange) {
    		voltExecuteSQL();
    		auditTime = auditQuery(part_id, tod, 100, auditTime);
    		voltQueueSQL(insertSegStatsStream, part_id, xway, tod, time, seg, dir, addCount, addSpd);
    		voltQueueSQL(UpdateCurrentTimestamp, tod, time, xway, part_id);
    	}
    	else {
    		voltQueueSQL(updateCurSegStats, addCount, addSpd, part_id, xway, tod, seg, dir);
    	}
    	voltExecuteSQL();
		
        auditTime = auditQuery(part_id, tod, 4, auditTime); /////////////////audit 4
        
        String params =time+","+tod+" minutes ("+part_id + "," + xway + ")";
        outputTuples.add(params);
        
        //hack to return a string back through a VoltTable
        return LinearRoadConstants.getOutputVoltTable(starttime, outputTuples);
    }
}
