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
		partitionInfo = "DimAccount.part_id:0"
)
public class SP4GetAccountInfo extends VoltProcedure {
	
	protected void toSetTriggerTableName()
	{
		addTriggerTable("SP3out");
	}
	
	 public final SQLStmt insertSP4Out = new SQLStmt(
			   "INSERT INTO SP4out "
			  + "(T_ID,SK_CreateDateID,SK_CreateTimeID,SK_CloseDateID,SK_CloseTimeID,Status,Type,"
			  + "T_IS_CASH,SK_SecurityID,SK_CompanyID,T_QTY,T_BID_PRICE,SK_AccountID,SK_CustomerID,SK_BrokerID,"
			  + "T_EXEC_NAME,T_TRADE_PRICE,T_CHRG,T_COMM,T_TAX,batch_id,part_id)"
			   + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
		    );
	 
	public final SQLStmt getAccountInfo = new SQLStmt(
			   "SELECT SK_AccountID, SK_CustomerID,SK_BrokerID FROM DimAccount WHERE AccountID = ? AND part_id = ?;"
		    );

    public long run(int partId, VoltTable sp3Data, long[] extraArgs) {

		for (int i=0; i < sp3Data.getRowCount(); i++) {
			VoltTableRow row = sp3Data.fetchRow(i);
			long ca_id = row.getLong("T_CA_ID");
			voltQueueSQL(getAccountInfo, ca_id, partId);
			VoltTable v[] = voltExecuteSQL();
			long SK_AccountID = v[0].fetchRow(0).getLong(0);
			long SK_CustomerID = v[0].fetchRow(0).getLong(1);
			long SK_BrokerID = v[0].fetchRow(0).getLong(2);

			voltQueueSQL(insertSP4Out,row.getLong("T_ID"),row.getLong("SK_CreateDateID"),
					row.getLong("SK_CreateTimeID"),	row.getLong("SK_CloseDateID"),
					row.getLong("SK_CloseTimeID"),row.getString("Status"),
					row.getString("Type"),(short)row.getLong("T_IS_CASH"),
					row.getLong("SK_SecurityID"),row.getLong("SK_CompanyID"),(int)row.getLong("T_QTY"),row.getDouble("T_BID_PRICE"),
					SK_AccountID,SK_CustomerID,SK_BrokerID,
					row.getString("T_EXEC_NAME"),row.getDouble("T_TRADE_PRICE"),
					row.getDouble("T_CHRG"),row.getDouble("T_COMM"),row.getDouble("T_TAX"),row.getLong("batch_id"), partId
					);


			int destinationPartition = TPCDIUtil.hashCode(String.valueOf(row.getLong("T_ID")), TPCDIConstants.NUM_PARTITIONS);
			voltExecuteSQLDownStream("SP4out", destinationPartition);
		}
        // Set the return value to 0: successful vote
        return TPCDIConstants.PROC_SUCCESSFUL;
    }
}