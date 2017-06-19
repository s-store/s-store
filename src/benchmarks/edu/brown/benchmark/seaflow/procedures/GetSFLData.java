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
package edu.brown.benchmark.seaflow.procedures;

import edu.brown.benchmark.seaflow.SeaflowConstants;
import org.json.JSONArray;
import org.json.JSONException;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;

import static edu.brown.benchmark.seaflow.SeaflowUtil.parseResultsToJSONArray;


//@ProcInfo (
//		singlePartition = true,
//		partitionInfo = "sfl_tbl.a_cruise_id:2"
//)
public class GetSFLData extends VoltProcedure
{
    
	public final SQLStmt getSFLJSON = 
    		new SQLStmt("SELECT lat, lon, salinity, temp, par, epoch_ms " +
    				"FROM SFLToJSON_tbl ORDER BY epoch_ms;"
    			);
    
    public final SQLStmt deleteSFL =
    		new SQLStmt("DELETE from SFLToJSON_tbl");
    
	
    public VoltTable[] run() {
	    voltQueueSQL(getSFLJSON);
	    VoltTable v[] = voltExecuteSQL();

	    voltQueueSQL(deleteSFL);
	    voltExecuteSQL();
        return v;
    }
}