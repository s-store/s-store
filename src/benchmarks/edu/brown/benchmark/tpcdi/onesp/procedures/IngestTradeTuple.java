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

package edu.brown.benchmark.tpcdi.onesp.procedures;

import edu.brown.benchmark.tpcdi.onesp.TPCDIConstants;
import edu.brown.benchmark.tpcdi.onesp.TPCDIUtil;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;

@ProcInfo (
	//partitionInfo = "SP1out.T_ID:2",
    singlePartition = false
)
public class IngestTradeTuple extends VoltProcedure {
	
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
 	 
 	public final SQLStmt getSecurityID = new SQLStmt(
 			   "SELECT SK_SecurityID, SK_CompanyID FROM DimSecurity WHERE Symbol = ? AND part_id = ?;"
 		    );
	 
	public final SQLStmt getAccountInfo = new SQLStmt(
			   "SELECT SK_AccountID, SK_CustomerID,SK_BrokerID FROM DimAccount WHERE AccountID = ? AND part_id = ?;"
		    );
	
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
    		//SP1    		
    		String[] st = tuples[i].split("\\|", -1);
    		T_ID = new Long(st[0]);
    		
    		/**
    		voltQueueSQL(SP1OutStmt, //st[0],new Long(st[1]),
    								T_ID,st[1],st[2]
    								 ,st[3],new Short(st[4]),st[5],new Integer(st[6])
    								 ,new Double(st[7]),new Integer(st[8]),st[9],new Double(replaceWithZeroIfEmpty(st[10])),
    								 new Double(replaceWithZeroIfEmpty(st[11])),new Double(replaceWithZeroIfEmpty(st[12])),new Double(replaceWithZeroIfEmpty(st[13])),
    								 batchid, part_id
    								);
    		*/
    		
    		//SP2
    		String T_ST_ID = st[2];
			String T_TT_ID = st[3];
			
			//get date and time
			String T_DTS = st[1];
			String date = T_DTS.split(" ")[0];
			
			String timeall = T_DTS.split(" ")[1];
			String[] time = T_DTS.split(" ")[1].split(":");
			int hour = new Integer(time[0]);
			int min = new Integer(time[1]);
			int sec = new Integer(time[2]);
			
			int dest_part = TPCDIUtil.hashCode(TPCDIConstants.STATUSTYPE_TABLE,String.valueOf(T_ST_ID));
			voltQueueSQL(getStatus, T_ST_ID, dest_part);
			dest_part = TPCDIUtil.hashCode(TPCDIConstants.TRADETYPE_TABLE,String.valueOf(T_TT_ID));
			voltQueueSQL(getTradeType, T_TT_ID, dest_part);
			dest_part = TPCDIUtil.hashCode(TPCDIConstants.DIMDATE_TABLE,String.valueOf(date));
			voltQueueSQL(getDateID, date, dest_part);
			dest_part = TPCDIUtil.hashCode(TPCDIConstants.DIMTIME_TABLE,String.valueOf(timeall));
			voltQueueSQL(getTimeID, hour,min,sec, dest_part);
			VoltTable[] results = voltExecuteSQL();
			
			String status = results[0].fetchRow(0).getString(0);
			String type = results[1].fetchRow(0).getString(0);
			String symbol = st[5];
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

			//SP3
			dest_part = TPCDIUtil.hashCode(TPCDIConstants.DIMSECURITY_TABLE,String.valueOf(symbol));
			voltQueueSQL(getSecurityID, symbol, dest_part);
			VoltTable v[] = voltExecuteSQL();
			long SK_SecurityID = v[0].fetchRow(0).getLong(0);
			long SK_CompanyID = v[0].fetchRow(0).getLong(1);

			//SP4
			long ca_id = new Integer(st[8]);
			dest_part = TPCDIUtil.hashCode(TPCDIConstants.DIMACCOUNT_TABLE,String.valueOf(ca_id));
			voltQueueSQL(getAccountInfo, ca_id, dest_part);
			v = voltExecuteSQL();
			long SK_AccountID = v[0].fetchRow(0).getLong(0);
			long SK_CustomerID = v[0].fetchRow(0).getLong(1);
			long SK_BrokerID = v[0].fetchRow(0).getLong(2);

			//SP5
			double tradePrice = new Double(replaceWithZeroIfEmpty(st[10]));
			double quantity = new Double(st[6]);
			Double commission = new Double(replaceWithZeroIfEmpty(st[12]));
			Double fee = new Double(replaceWithZeroIfEmpty(st[11]));
			String messageText = "";
			String messageData = "";
			
			if(commission != null && commission > tradePrice*quantity){
				messageText = "Invalid trade commission";
				messageData = "T_ID=" + T_ID + ", T_COMM=" + commission;
				dest_part = TPCDIUtil.hashCode(TPCDIConstants.DIMESSAGES_TABLE,String.valueOf(batchid));
				voltQueueSQL(insertMessage, batchid,"DimTrade","Alert",messageText,messageData,dest_part);
			}
			if(fee != null && fee > tradePrice*quantity){
				messageText = "Invalid trade fee";
				messageData = "T_ID=" + T_ID + ", T_CHRG=" + fee;
				dest_part = TPCDIUtil.hashCode(TPCDIConstants.DIMESSAGES_TABLE,String.valueOf(batchid));
				voltQueueSQL(insertMessage, batchid, "DimTrade","Alert",messageText,messageData,dest_part);
			}

			dest_part = TPCDIUtil.hashCode(TPCDIConstants.DIMTRADE_TABLE,String.valueOf(T_ID));
			voltQueueSQL(insertDimTrade, T_ID, SK_BrokerID, SK_CreateDateID,
					SK_CreateTimeID, SK_CloseDateID, SK_CloseTimeID,
					status, type, new Short(st[4]),
					SK_SecurityID, SK_CompanyID, (int) quantity,
					new Double(st[7]), SK_CustomerID, SK_AccountID,
					st[9], tradePrice, fee,
					commission, new Double(replaceWithZeroIfEmpty(st[13])), batchid, dest_part);
			
			voltExecuteSQL();
      }

        // Set the return value to 0: successful vote
        return TPCDIConstants.PROC_SUCCESSFUL;
    }
}