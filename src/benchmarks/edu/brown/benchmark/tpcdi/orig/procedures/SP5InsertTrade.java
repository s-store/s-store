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

package edu.brown.benchmark.tpcdi.orig.procedures;

import edu.brown.benchmark.tpcdi.orig.TPCDIUtil;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;

import edu.brown.benchmark.tpcdi.orig.TPCDIConstants;

@ProcInfo (
		partitionInfo = "DimTrade.part_id:0",
		singlePartition = true
)
public class SP5InsertTrade extends VoltProcedure {
	
	protected void toSetTriggerTableName()
	{
		addTriggerTable("SP4out");
	}
	
	 public final SQLStmt insertDimTrade = new SQLStmt(
			   "INSERT INTO DimTrade "
			  + "(TradeID,SK_BrokerID,SK_CreateDateID,SK_CreateTimeID,SK_CloseDateID,SK_CloseTimeID,Status,Type,"
			  + "CashFlag,SK_SecurityID,SK_CompanyID,Quantity,BidPrice,SK_CustomerID,SK_AccountID,"
			  + "ExecutedBy,TradePrice,Fee,Commission,Tax,BatchID,part_id)"
			   + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
		    );

	public final SQLStmt insertMessage = new SQLStmt(
			   "INSERT INTO DiMessages (BatchID, MessageSource, MessageText, MessageType, MessageData, part_id) "
			   + "VALUES (?,?,?,?,?,?);"
		    );

    public long run(int partId, VoltTable sp4Data, long[] extraArgs) {
    	
		for (int i=0; i < sp4Data.getRowCount(); i++) {
			VoltTableRow row = sp4Data.fetchRow(i);
			double tradePrice = row.getDouble("T_TRADE_PRICE");
			double quantity = (double)row.getLong("T_QTY");
			Double commission = row.getDouble("T_COMM");
			Double fee = row.getDouble("T_CHRG");
			long tradeID = row.getLong("T_ID");
			long batchID = row.getLong("batch_id");
			String messageText = "";
			String messageData = "";
			
			if(commission != null && commission > tradePrice*quantity){
				messageText = "Invalid trade commission";
				messageData = "T_ID=" + tradeID + ", T_COMM=" + commission;
				voltQueueSQL(insertMessage, batchID,"DimTrade","Alert",messageText,messageData,partId);
			}
			if(fee != null && fee > tradePrice*quantity){
				messageText = "Invalid trade fee";
				messageData = "T_ID=" + tradeID + ", T_CHRG=" + fee;
				voltQueueSQL(insertMessage, batchID, "DimTrade","Alert",messageText,messageData,partId);
			}

			voltQueueSQL(insertDimTrade, row.getLong("T_ID"), row.getLong("SK_BrokerID"), row.getLong("SK_CreateDateID"),
					row.getLong("SK_CreateTimeID"), row.getLong("SK_CloseDateID"), row.getLong("SK_CloseTimeID"),
					row.getString("Status"), row.getString("Type"), (short) row.getLong("T_IS_CASH"),
					row.getLong("SK_SecurityID"), row.getLong("SK_CompanyID"), (int) row.getLong("T_QTY"),
					row.getDouble("T_BID_PRICE"), row.getLong("SK_CustomerID"), row.getLong("SK_AccountID"),
					row.getString("T_EXEC_NAME"), row.getDouble("T_TRADE_PRICE"), row.getDouble("T_CHRG"),
					row.getDouble("T_COMM"), row.getDouble("T_TAX"), row.getLong("batch_id"), partId);
			
			voltExecuteSQL();
		}
        // Set the return value to 0: successful vote
        return TPCDIConstants.PROC_SUCCESSFUL;
    }
}