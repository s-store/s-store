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
public class GetJSONDataFull extends VoltProcedure
{
	private static boolean printToFile = false;
    
	public final SQLStmt getSFLJSON = 
    		new SQLStmt("SELECT datatype, lat, lon, salinity, temp, par, epoch_ms " +
    				"FROM SFLToJSON_tbl ORDER BY epoch_ms;"
    			);
	
	public final SQLStmt getBACJSON = 
    		new SQLStmt("SELECT datatype, fsc_small, abundance, pop, epoch_ms " +
    				"FROM BACToJSON_tbl ORDER BY epoch_ms;"
    			);
    
    public final SQLStmt deleteSFL =
    		new SQLStmt("DELETE from SFLToJSON_tbl");
    
    public final SQLStmt deleteBAC =
    		new SQLStmt("DELETE from BACToJSON_tbl");

    public final SQLStmt getSteering = new SQLStmt(
            "SELECT st_rotation FROM steering_tbl WHERE st_id = 1;"
    );
	
    public VoltTable[] run() {
	    voltQueueSQL(getSFLJSON);
	    voltQueueSQL(getBACJSON);
        voltQueueSQL(getSteering);
	    VoltTable v[] = voltExecuteSQL();
	    
	    if(printToFile){
			JSONArray json0 = parseResultsToJSONArray(v[0]);
            JSONArray json1 = parseResultsToJSONArray(v[1]);
            JSONArray json2 = parseResultsToJSONArray(v[2]);

            try (FileWriter file = new FileWriter(SeaflowConstants.JSON_OUTPUT_DIR + "json" + System.currentTimeMillis() + ".txt")) {
                file.write("Current data:\n");
                for(int i = 0; i < json0.length(); i++) {
                    file.write(json0.getJSONObject(i).toString());
                }

                file.write("Bacteria population:\n");
                for(int i = 0; i < json1.length(); i++) {
                    file.write(json1.getJSONObject(i).toString());
                }

                file.write("Steering recommendation:\n");
                for(int i = 0; i < json2.length(); i++) {
                    file.write(json2.getJSONObject(i).toString());
                }
                file.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

	    voltQueueSQL(deleteSFL);
	    voltQueueSQL(deleteBAC);
	    voltExecuteSQL();
        return v;
    }
}