/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

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
//
// Accepts a vote, enforcing business logic: make sure the vote is for a valid
// contestant and that the voterdemosstore (phone number of the caller) is not above the
// number of allowed votes.
//

package edu.brown.benchmark.tpcdi.varconfig.procedures;

import edu.brown.benchmark.tpcdi.varconfig.TPCDIConstants;
import edu.brown.benchmark.tpcdi.varconfig.TPCDIUtil;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

@ProcInfo (
	partitionInfo = "SP1out.T_ID:2"
)
public class SP1SplitTuple extends VoltProcedure {
	
    // Put vote into leaderboard
    public final SQLStmt SP1OutStmt = new SQLStmt(
	   "INSERT INTO SP1out VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
    );

//    public final SQLStmt updateStats = new SQLStmt(
//            "UPDATE state_tbl SET ts_delta_client_sp1 = ?, ts_delta_sp1_insert = ?, total_tuples = ?, total_batches = ? WHERE part_id = 0 AND row_id = 0;"
//    );

//    public final SQLStmt getStats = new SQLStmt(
//            "SELECT ts_delta_client_sp1, ts_delta_sp1_insert, total_tuples, total_batches FROM state_tbl WHERE part_id = 0 AND row_id = 0;"
//    );

    private String replaceWithZeroIfEmpty(String str) {
      if (str.isEmpty()) {
        return "0";
      } else {
        return str;
      }
    }
    public long run(long batchid, String[] tuples, long part_id) {
    	long tmpTime = System.currentTimeMillis();

    	/** STATS
        long clientToSP1 = System.currentTimeMillis() - times[0];

        long startInsert = System.currentTimeMillis();
        */

      Long T_ID = null;

    	for(int i = 0; i < tuples.length; i++) {
    		String[] st = tuples[i].split("\\|", -1);
    		T_ID = new Long(st[0]);
    		voltQueueSQL(SP1OutStmt, //st[0],new Long(st[1]),
    								T_ID,st[1],st[2]
    								 ,st[3],new Short(st[4]),st[5],new Integer(st[6])
    								 ,new Double(st[7]),new Integer(st[8]),st[9],new Double(replaceWithZeroIfEmpty(st[10])),
    								 new Double(replaceWithZeroIfEmpty(st[11])),new Double(replaceWithZeroIfEmpty(st[12])),new Double(replaceWithZeroIfEmpty(st[13])),
    								 batchid, part_id
    								);

      }
    	//voltExecuteSQL();
        int destinationPartition = TPCDIUtil.hashCode(TPCDIConstants.DIMTRADE_TABLE,String.valueOf(T_ID));
        voltExecuteSQLDownStream("SP1out", destinationPartition);
    	/** STATS
        long SP1Insert = System.currentTimeMillis() - startInsert;

        voltQueueSQL(getStats);
        VoltTable[] stats = voltExecuteSQL();
        long currentClientToSP1 = stats[0].fetchRow(0).getLong(0);
        long currentSP1Insert = stats[0].fetchRow(0).getLong(1);
        long currentTuples = stats[0].fetchRow(0).getLong(2);
        long currentBatches = stats[0].fetchRow(0).getLong(3);

        voltQueueSQL(updateStats, currentClientToSP1+clientToSP1, currentSP1Insert+SP1Insert, currentTuples+aId.length, currentBatches+1);
        voltExecuteSQL();*/

        // Set the return value to 0: successful vote
        return TPCDIConstants.PROC_SUCCESSFUL;
    }
}