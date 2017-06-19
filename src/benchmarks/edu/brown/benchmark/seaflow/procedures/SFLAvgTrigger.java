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

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

@ProcInfo(
		singlePartition = true
		)

public class SFLAvgTrigger extends VoltTrigger{
	@Override
    protected String toSetStreamName() {
        return "SFLFull_win";
    }
    
    public final SQLStmt insertIntoSFLAvg = new SQLStmt(
    		"INSERT INTO SFLAVG_tbl (s_id, s_cruise, s_date, s_lat, s_lon, s_salinity, s_ocean_tmp, s_par," +
    		"s_prochloro_conc,s_synecho_conc,s_picoeuk_conc,s_beads_conc," +
    		"s_prochloro_size,s_synecho_size,s_picoeuk_size,s_beads_size,s_epoch_ms) " +
    		"SELECT MIN(s_id), s_cruise, MIN(s_date), AVG(s_lat), AVG(s_lon), AVG(s_salinity), AVG(s_ocean_tmp), AVG(s_par), " +
			"AVG(s_prochloro_conc), AVG(s_synecho_conc), AVG(s_picoeuk_conc), AVG(s_beads_conc), " +
			"AVG(s_prochloro_size), AVG(s_synecho_size), AVG(s_picoeuk_size), AVG(s_beads_size), " +
			"MIN(s_epoch_ms) " +
			"FROM SFLFull_win GROUP BY s_cruise;"
		);
}
