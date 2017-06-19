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

package org.voltdb.sysprocs;

import java.util.List;
import java.util.Map;

import org.voltdb.DependencySet;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.conf.HStoreConf;

/** 
 * Get the value of a HStoreConf parameter from an HStoreSite
 */
@ProcInfo(
    singlePartition = true,
    partitionParam = 0
)
public class GetConfiguration extends VoltSystemProcedure {

    public static final ColumnInfo nodeResultsColumns[] = {
        new ColumnInfo("SITE", VoltType.INTEGER),
        new ColumnInfo("NAME", VoltType.STRING),
        new ColumnInfo("VALUE", VoltType.STRING),
        new ColumnInfo("CREATED", VoltType.TIMESTAMP)
    };

    @Override
    public void initImpl() {
        // Nothing
        System.out.println("initImpl!!! in get configuration");
        executor.registerPlanFragment(SysProcFragmentId.PF_getConfiguration, this);
    }

    @Override
    public DependencySet executePlanFragment(Long txn_id,
                                             Map<Integer, List<VoltTable>> dependencies,
                                             int fragmentId,
                                             ParameterSet params,
                                             PartitionExecutor.SystemProcedureExecutionContext context) {
        // Nothing to do
        System.out.println("executePlanFragment!!! in get configuration");
        assert(fragmentId == SysProcFragmentId.PF_getConfiguration);
        HStoreConf hstore_conf = executor.getHStoreConf();
        String confNames[] = (String[])params.toArray();
        for (int i = 0; i < confNames.length; i++) {
            if (hstore_conf.hasParameter((String)params.toArray()[i]) == false) {
                String msg = String.format("Invalid configuration parameter '%s'", confNames[i]);
                throw new VoltAbortException(msg);
            }
        } // FOR
        
        VoltTable vt = new VoltTable(nodeResultsColumns);
        TimestampType timestamp = new TimestampType();
        for (int i = 0; i < confNames.length; i++) {
            Object val = hstore_conf.get(confNames[i]);
            vt.addRow(executor.getSiteId(),         
                      confNames[i], 
                      val.toString(),
                      timestamp);
        } // FOR
        DependencySet result = new DependencySet(SysProcFragmentId.PF_getConfiguration, vt);
        return (result);
    }
    
    public VoltTable[] run(String confNames[]) {
        System.out.println("run!!! in get configuration");
//        HStoreConf hstore_conf = executor.getHStoreConf();
//        for (int i = 0; i < confNames.length; i++) {
//            if (hstore_conf.hasParameter(confNames[i]) == false) {
//                String msg = String.format("Invalid configuration parameter '%s'", confNames[i]);
//                throw new VoltAbortException(msg);
//            }
//        } // FOR
//        
//        VoltTable result = new VoltTable(nodeResultsColumns);
//        TimestampType timestamp = new TimestampType();
//        for (int i = 0; i < confNames.length; i++) {
//            Object val = hstore_conf.get(confNames[i]);
//            result.addRow(executor.getSiteId(),  		
//                          confNames[i], 
//                          val.toString(),
//                          timestamp);
//        } // FOR
//        return (result);
        return executeLocal(SysProcFragmentId.PF_getConfiguration, new ParameterSet());
    }
}
