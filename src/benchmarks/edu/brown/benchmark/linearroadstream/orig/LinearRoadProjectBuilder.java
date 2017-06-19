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
package edu.brown.benchmark.linearroadstream.orig;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.linearroadstream.orig.procedures.CalculateSegStats;
import edu.brown.benchmark.linearroadstream.orig.procedures.GetAccountBalance;
import edu.brown.benchmark.linearroadstream.orig.procedures.GetDailyExpenditure;
import edu.brown.benchmark.linearroadstream.orig.procedures.GetTravelEstimate;
import edu.brown.benchmark.linearroadstream.orig.procedures.InsertPosition;
import edu.brown.benchmark.linearroadstream.orig.procedures.InsertTimestamp;

public class LinearRoadProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = LinearRoadClient.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = LinearRoadLoader.class;

    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
        InsertPosition.class, InsertTimestamp.class, //GetAccountBalance.class, GetDailyExpenditure.class, GetTravelEstimate.class, 
        CalculateSegStats.class
    };
    public static final String PARTITIONING[][] = new String[][] {
            // { "TABLE NAME", "PARTITIONING COLUMN NAME" }
            { "cur_position", "part_id" },
            { "cur_seg_stats", "part_id" },
            { "cur_avg_speeds", "part_id" },
            { "potential_accidents", "part_id" },
            { "segment_history", "part_id" },
            { "tolls_per_vehicle", "part_id" },
            { "current_ts", "part_id" },
            { "query_audit_tbl", "part_id"},
            { "start_seg_stats", "part_id" },
            { "prev_input", "part_id" },
    };

    public LinearRoadProjectBuilder() {
        super("linearroadstreamorig", LinearRoadProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}