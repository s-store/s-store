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

import edu.brown.benchmark.tpcdi.varconfig.TPCDIUtil;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;

import edu.brown.benchmark.tpcdi.varconfig.TPCDIConstants;

@ProcInfo (
	partitionInfo = "StatusType.part_id:0"
)
public class SP2GetTypes extends VoltProcedure {
	
	protected void toSetTriggerTableName()
	{
		addTriggerTable("SP1out");
	}
	
	public final SQLStmt getStatus = new SQLStmt(
		   "SELECT ST_NAME FROM StatusType WHERE ST_ID = ? AND part_id = ?"
	    );
	
	public final SQLStmt getTradeType = new SQLStmt(
		   "SELECT TT_NAME FROM TradeType WHERE TT_ID = ? AND part_id = ?"
	    );
	
	public final SQLStmt getDateID = new SQLStmt(
		   "SELECT SK_DateID FROM DimDate WHERE DateValue = ? AND part_id = ?"
	    );
	
	public final SQLStmt getTimeID = new SQLStmt(
		   "SELECT SK_TimeID FROM DimTime WHERE HourID = ? AND MinuteID = ? AND SecondID = ? AND part_id = ?"
	    );

    public final SQLStmt insertSP2Out = new SQLStmt(
	   "INSERT INTO SP2out "
	  + "(T_ID,SK_CreateDateID,SK_CreateTimeID,SK_CloseDateID,SK_CloseTimeID,Status,Type,"
	  + "T_IS_CASH,T_S_SYMB,T_QTY,T_BID_PRICE,T_CA_ID,T_EXEC_NAME,T_TRADE_PRICE,T_CHRG,T_COMM,T_TAX,batch_id,part_id)"
	   + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
    );

	

    
    public void compute(long time) {
    	try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
	
    public long run(int partId, VoltTable sp1Data, long[] extraArgs) {
    	

		for (int i=0; i < sp1Data.getRowCount(); i++) {
			VoltTableRow row = sp1Data.fetchRow(i);
			String T_ST_ID = row.getString("T_ST_ID");
			String T_TT_ID = row.getString("T_TT_ID");
			
			//get date and time
			String T_DTS = row.getString("T_DTS");
			String date = T_DTS.split(" ")[0];
			
			
			String[] time = T_DTS.split(" ")[1].split(":");
			int hour = new Integer(time[0]);
			int min = new Integer(time[1]);
			int sec = new Integer(time[2]);
			
			voltQueueSQL(getStatus, T_ST_ID, partId);
			voltQueueSQL(getTradeType, T_TT_ID, partId);
			voltQueueSQL(getDateID, date, partId);
			voltQueueSQL(getTimeID, hour,min,sec, partId);
			VoltTable[] results = voltExecuteSQL();
			
			String status = results[0].fetchRow(0).getString(0);
			String type = results[1].fetchRow(0).getString(0);
			String symbol = row.getString("T_S_SYMB");
			long SK_CreateDateID = -1;
			long SK_CreateTimeID = -1;
			long SK_CloseDateID = -1;
			long SK_CloseTimeID = -1;
			
			if((T_ST_ID.equals("SBMT") && (T_TT_ID.equals("TMB") || T_TT_ID.equals("TMS"))) || T_ST_ID.equals("PNDG")) {
				SK_CreateDateID = results[2].fetchRow(0).getLong(0);
				SK_CreateTimeID = results[3].fetchRow(0).getLong(0);
			}
			else if (T_ST_ID.equals("CMPT") || T_ST_ID.equals("CNCL")) {
				SK_CloseDateID = results[2].fetchRow(0).getLong(0);
				SK_CloseTimeID = results[3].fetchRow(0).getLong(0);
			}
			
			voltQueueSQL(insertSP2Out, row.getLong("T_ID"), SK_CreateDateID, SK_CreateTimeID, SK_CloseDateID, SK_CloseTimeID,
					status, type, (short) row.getLong("T_IS_CASH"), symbol, (int) row.getLong("T_QTY"),
					row.getDouble("T_BID_PRICE"), (int) row.getLong("T_CA_ID"), row.getString("T_EXEC_NAME"),
					row.getDouble("T_TRADE_PRICE"), row.getDouble("T_CHRG"), row.getDouble("T_COMM"), row.getDouble("T_TAX"),
					row.getLong("batch_id"), partId);

			int destinationPartition = TPCDIUtil.hashCode(TPCDIConstants.DIMSECURITY_TABLE,symbol);
			voltExecuteSQLDownStream("SP2out", destinationPartition);
		}
		
        // Set the return value to 0: successful vote
        return TPCDIConstants.PROC_SUCCESSFUL;
    }
}