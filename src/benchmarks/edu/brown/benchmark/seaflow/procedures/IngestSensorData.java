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
import org.voltdb.VoltTable;

import edu.brown.benchmark.seaflow.SeaflowConstants;
import edu.brown.benchmark.seaflow.SeaflowUtil;

//@ProcInfo (
//		singlePartition = true,
//		partitionInfo = "sfl_tbl.a_cruise_id:2"
//)
public class IngestSensorData extends VoltProcedure
{
    public final SQLStmt addSFLFrontend = //new SQLStmt("UPDATE argo_tbl SET a_lat = ? WHERE a_lat = ? AND a_lon = ? and a_month = ? and a_depth = ?;");
    		new SQLStmt("INSERT INTO SFLFull_tbl (s_id, s_cruise, s_date ,s_lat, s_lon, s_salinity, s_ocean_tmp, s_par, " +
    				"s_prochloro_conc, s_synecho_conc, s_picoeuk_conc, s_beads_conc, " +
    				"s_prochloro_size, s_synecho_size, s_picoeuk_size, s_beads_size, s_epoch_ms) " +
    				"VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"//17
    			);
    
    public final SQLStmt addSFLtoJSON = //new SQLStmt("UPDATE argo_tbl SET a_lat = ? WHERE a_lat = ? AND a_lon = ? and a_month = ? and a_depth = ?;");
    		new SQLStmt("INSERT INTO SFLtojson_tbl (datatype, lat, lon, salinity, temp, par, epoch_ms) " +
    				"VALUES (?,?,?,?,?,?,?);"//7
    			);
    
    public final SQLStmt addBACtoJSON = //new SQLStmt("UPDATE argo_tbl SET a_lat = ? WHERE a_lat = ? AND a_lon = ? and a_month = ? and a_depth = ?;");
    		new SQLStmt("INSERT INTO BACtojson_tbl (datatype, fsc_small, abundance, pop, epoch_ms) " +
    				"VALUES (?,?,?,?,?);"//5
    			);
    
    public final SQLStmt addSFLWin = //new SQLStmt("UPDATE argo_tbl SET a_lat = ? WHERE a_lat = ? AND a_lon = ? and a_month = ? and a_depth = ?;");
    		new SQLStmt("INSERT INTO SFLFull_win (s_id, s_cruise, s_date ,s_lat, s_lon, s_salinity, s_ocean_tmp, s_par, " +
    				"s_prochloro_conc, s_synecho_conc, s_picoeuk_conc, s_beads_conc, " +
    				"s_prochloro_size, s_synecho_size, s_picoeuk_size, s_beads_size, s_epoch_ms,ts) " +
    				"VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"//17
    			);
    
    public final SQLStmt addSFLHourWin = //new SQLStmt("UPDATE argo_tbl SET a_lat = ? WHERE a_lat = ? AND a_lon = ? and a_month = ? and a_depth = ?;");
    		new SQLStmt("INSERT INTO SFLFullHour_win (s_id, s_cruise, s_date ,s_lat, s_lon, s_salinity, s_ocean_tmp, s_par, " +
    				"s_prochloro_conc, s_synecho_conc, s_picoeuk_conc, s_beads_conc, " +
    				"s_prochloro_size, s_synecho_size, s_picoeuk_size, s_beads_size, s_epoch_ms,ts) " +
    				"VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"//17
    			);
    
    public final SQLStmt avgSFL = 
    		new SQLStmt("SELECT s_id, s_cruise, MIN(s_date), AVG(s_lat), AVG(s_lon), AVG(s_salinity), AVG(s_ocean_tmp), AVG(s_par), " +
    				"AVG(s_prochloro_conc), AVG(s_synecho_conc), AVG(s_picoeuk_conc), AVG(s_beads_conc), " +
    				"AVG(s_prochloro_size), AVG(s_synecho_size), AVG(s_picoeuk_size), AVG(s_beads_size), " +
    				"MIN(s_epoch_ms) " +
    				"FROM SFLFull_win GROUP BY s_id, s_cruise;"
    			);
    
    public final SQLStmt insertCurPos =
    		new SQLStmt("UPDATE cur_location_tbl SET c_lat = ?,c_lon = ?,c_month = ? WHERE c_id = 1");
   
    
	
    public long run(long batch_id, String[] sfl) {
    	int i = 0;
    	double cur_lat = 0.0;
    	double cur_lon = 0.0;
    	int cur_mon = 6;
    	for(String tuple : sfl) {
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
	    		Double s_prochloro_conc = splTuple[7].equals("") ? null : new Double(splTuple[7]);
	    		Double s_synecho_conc = splTuple[8].equals("") ? null : new Double(splTuple[8]);
	    		Double s_picoeuk_conc = splTuple[9].equals("") ? null : new Double(splTuple[9]);
	    		Double s_beads_conc = splTuple[10].equals("") ? null : new Double(splTuple[10]);
	    		Double s_prochloro_size = splTuple[11].equals("") ? null : new Double(splTuple[11]);
	    		Double s_synecho_size = splTuple[12].equals("") ? null : new Double(splTuple[12]);
	    		Double s_picoeuk_size = (splTuple.length < 14 || splTuple[13].equals("")) ? null : new Double(splTuple[13]);
	    		Double s_beads_size = (splTuple.length < 15 || splTuple[14].equals("")) ? null : new Double(splTuple[14]);
			
	    	    long s_epoch_ms = SeaflowUtil.convertToEpoch(s_date);
//	    	    voltQueueSQL(addSFLFrontend,batch_id,s_cruise,s_date,s_lat,s_lon,s_salinity,s_ocean_tmp,s_par,
//	    	    		s_prochloro_conc,s_synecho_conc,s_picoeuk_conc,s_beads_conc,
//	    	    		s_prochloro_size,s_synecho_size,s_picoeuk_size,s_beads_size,
//	    	    		s_epoch_ms);
	    	    voltQueueSQL(addSFLtoJSON, "sfl", s_lat, s_lon, s_salinity, s_ocean_tmp, s_par, s_epoch_ms);
	    	    voltQueueSQL(addBACtoJSON, "stat", s_prochloro_conc, s_prochloro_size, "prochloro", s_epoch_ms);
	    	    voltQueueSQL(addBACtoJSON,"stat", s_synecho_conc, s_synecho_size, "synecho", s_epoch_ms);
	    	    voltQueueSQL(addBACtoJSON, "stat", s_picoeuk_conc, s_picoeuk_size, "picoeuk", s_epoch_ms);
	    	    voltQueueSQL(addBACtoJSON, "stat", s_beads_conc, s_beads_size, "beads", s_epoch_ms);
	    	    voltQueueSQL(addSFLWin,batch_id,s_cruise,s_date,s_lat,s_lon,s_salinity,s_ocean_tmp,s_par,
	    	    		s_prochloro_conc,s_synecho_conc,s_picoeuk_conc,s_beads_conc,
	    	    		s_prochloro_size,s_synecho_size,s_picoeuk_size,s_beads_size,
	    	    		s_epoch_ms, batch_id);
	    	    //voltQueueSQL(addSFLHourWin,batch_id,s_cruise,s_date,s_lat,s_lon,s_salinity,s_ocean_tmp,s_par,
	    	    //		s_prochloro_conc,s_synecho_conc,s_picoeuk_conc,s_beads_conc,
	    	    //		s_prochloro_size,s_synecho_size,s_picoeuk_size,s_beads_size,
	    	    //		s_epoch_ms, batch_id);
	    	    cur_lat = s_lat;
	    	    cur_lon = s_lon;
	    	    //cur_mon = SeaflowUtil.getMonth(s_date);
	    	    if(i > (SeaflowConstants.MAX_SQL_BATCH/10)){
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
    	voltQueueSQL(insertCurPos,cur_lat,cur_lon,cur_mon);//WARNING - Hardcoded month
	    voltExecuteSQL();
	    //voltQueueSQL(avgSFL);
	    //voltQueueSQL(deleteSFL);
	    //VoltTable v[] = voltExecuteSQL();
	    
	    
	   
        return 0;
    }
}