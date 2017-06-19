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
// Initializes the database, pushing the list of contestants and documenting domain data (Area codes and States).
//

package edu.brown.benchmark.tpcdi.varconfig.procedures;

import edu.brown.benchmark.tpcdi.varconfig.TPCDIConstants;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

@ProcInfo (
singlePartition = false
)
public class CalculateStats extends VoltProcedure
{
	public final SQLStmt getStateTable = new SQLStmt(
		"SELECT * FROM state_tbl ORDER BY row_id ASC;"
	);
	
    public long run() {
		/*
		   row_id       integer NOT NULL,
   batch_id    bigint  NOT NULL,
   part_id      bigint  NOT NULL,
   ts_delta_client_sp1     bigint NOT NULL,
   ts_delta_sp1_insert     bigint NOT NULL,
   ts_delta_sp1_sp2     bigint NOT NULL,
   ts_delta_sp2_insert     bigint NOT NULL,
   ts_delta_total     bigint NOT NULL,
   total_tuples bigint NOT NULL,
   total_batches bigint NOT NULL,
		 */

		voltQueueSQL(getStateTable);
		VoltTable[] data = voltExecuteSQL();
		float totalTuples = (float) data[0].fetchRow(1).getLong(8);
		float totalBatches = (float) data[0].fetchRow(1).getLong(9);

		float tsDeltaClientSP1 = (float) data[0].fetchRow(0).getLong(3) / totalBatches;
		float tsDeltaSP1Insert = (float) data[0].fetchRow(0).getLong(4) / totalBatches;
		float tsDeltaSP1ToSP2 = (float) data[0].fetchRow(1).getLong(5) / totalBatches;
		float tsDeltaSP2Insert = (float) data[0].fetchRow(1).getLong(6) / totalBatches;
		float tsDeltaTotal = (float) data[0].fetchRow(1).getLong(7) / totalBatches;







		PrintWriter writer = null;
		try {
			writer = new PrintWriter("move-stats.txt", "UTF-8");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		writer.println("client_to_sp1,sp1_insert,sp1_to_sp2,sp2_insert,total_time,total_tuples,total_batches");
		writer.printf("%.2f", tsDeltaClientSP1);
		writer.printf(",");
		writer.printf("%.2f", tsDeltaSP1Insert);
		writer.printf(",");
		writer.printf("%.2f", tsDeltaSP1ToSP2);
		writer.printf(",");
		writer.printf("%.2f", tsDeltaSP2Insert);
		writer.printf(",");
		writer.printf("%.2f", tsDeltaTotal);
		writer.printf(",");
		writer.printf("%.2f", totalTuples);
		writer.printf(",");
		writer.printf("%.2f", totalBatches);
		writer.printf(",");
		writer.println();
		//writer.println(tsDeltaClientSP1 + "," + tsDeltaSP1Insert + "," + tsDeltaSP1ToSP2 + "," + tsDeltaSP2Insert + "," + tsDeltaTotal + "," + totalTuples + "," + totalBatches);
		writer.close();

        return TPCDIConstants.PROC_SUCCESSFUL;
    }
}