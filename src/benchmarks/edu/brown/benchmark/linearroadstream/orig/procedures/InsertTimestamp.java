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

import edu.brown.benchmark.linearroadstream.orig.LinearRoadConstants;

public class InsertTimestamp extends VoltProcedure {
	public final SQLStmt InsertTimestamp = new SQLStmt("INSERT INTO current_ts (XWay, tod, ts, part_id) VALUES (?,?,?,?)");
	
	public final SQLStmt InsertCurAvgSpeed = new SQLStmt("INSERT INTO cur_avg_speeds (part_id, xway, seg, dir, row_id, avg_spd) VALUES (?,?,?,?,?,?)");
	
	public final SQLStmt InsertCurSegStats = new SQLStmt("INSERT INTO cur_seg_stats (part_id, xway, tod, seg, dir, num_cars, total_spd, lav4) VALUES (?,?,?,?,?,?,?,?)");
	
	public final SQLStmt InsertPrevInput = new SQLStmt("INSERT INTO prev_input (part_id, xway, vid, seg, dir, lane, pos, spd, ts) VALUES (?,?,?,?,?,?,?,?,?)");
	
    public VoltTable[] run(int part_id, int xway) {
        voltQueueSQL(InsertTimestamp, xway, 0, 0, part_id);
        VoltTable[] v = voltExecuteSQL();
        
        //load rows into cur_avg_speeds (keeps track of the previous four average speed values)
        int numRows = 0;
        for(int s = 0; s < 100; s++) {
        	for(int d = 0; d < 2; d++) {
        		for(int r = 0; r < 4; r++) {
        			voltQueueSQL(InsertCurAvgSpeed, part_id, xway, s, d, r, 0);
        			numRows++;
        			if(numRows % LinearRoadConstants.MAX_SQL_BATCH == 0)
        				voltExecuteSQL();
        		}
        		voltQueueSQL(InsertCurSegStats, part_id, xway, 0, s, d, 0, 0, 0);
        	}
        }
        voltQueueSQL(InsertPrevInput, part_id, xway, 0,0,0,0,0,0,0);
        voltExecuteSQL();
        
        return null;
    }
}
