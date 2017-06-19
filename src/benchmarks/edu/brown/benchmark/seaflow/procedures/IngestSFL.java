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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

import edu.brown.benchmark.seaflow.SeaflowConstants;
import edu.brown.benchmark.seaflow.SeaflowUtil;

//@ProcInfo (
//		singlePartition = true,
//		partitionInfo = "sfl_tbl.a_cruise_id:2"
//)
public class IngestSFL extends VoltProcedure
{
    public final SQLStmt addSFL = //new SQLStmt("UPDATE argo_tbl SET a_lat = ? WHERE a_lat = ? AND a_lon = ? and a_month = ? and a_depth = ?;");
    		new SQLStmt("INSERT INTO SFL_tbl (s_id, s_cruise, s_date ,s_lat, s_lon, s_salinity, s_ocean_tmp, s_par, s_epoch_ms) "
    				+ "VALUES (?,?,?,?,?,?,?,?,?);"
    			);
	
    public long run(long batch_id, String[] tuples) {
    	int i = 0;
    	for(String tuple : tuples) {
    		try {
    			//voltQueueSQL(addSFL,batch_id,"ShipA","8/5/2015 6:48:57 AM",-0.05,-0.05,1.1,1.2,1.3);
    		
	    		String splTuple[] = tuple.split(",");
	    		String s_cruise = splTuple[0];
	    		String s_date = splTuple[1];
	    		Double s_lat = splTuple[2].equals("") ? null : new Double(splTuple[2]);
	    		Double s_lon = splTuple[3].equals("") ? null : new Double(splTuple[3]);
	    		Double s_salinity = splTuple[4].equals("") ? null : new Double(splTuple[4]);
	    		Double s_ocean_tmp = splTuple[5].equals("") ? null : new Double(splTuple[5]);
	    		Double s_par = splTuple[6].equals("") ? null : new Double(splTuple[6]);
	    
	    	    long s_epoch_ms = SeaflowUtil.convertToEpoch(s_date);
	    	    voltQueueSQL(addSFL,batch_id,s_cruise,s_date,s_lat,s_lon,
	    	    		s_salinity,s_ocean_tmp,s_par,s_epoch_ms);
	    	    if(i > SeaflowConstants.MAX_SQL_BATCH){
	    	    	voltExecuteSQL();
	    	    	i = 0;
	    	    	continue;
	    	    }
	    	    i++;
    		} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
	    voltExecuteSQL();
	   
        return 0;
    }
}