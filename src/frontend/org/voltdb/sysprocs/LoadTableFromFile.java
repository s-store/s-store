/**
 * 
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
package org.voltdb.sysprocs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.DependencySet;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltSystemProcedure.SynthesizedPlanFragment;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.catalog.Table;
import org.voltdb.exceptions.ServerFaultException;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.PartitionExecutor.SystemProcedureExecutionContext;

/**
 * Initiate a data loading for ETL
 * 
 * @author jdu
 */
@ProcInfo(singlePartition = false)

public class LoadTableFromFile extends VoltSystemProcedure {

	private static final Logger LOG = Logger.getLogger(LoadTableFromFile.class);

    public static final ColumnInfo nodeResultsColumns[] = { new ColumnInfo("SITE", VoltType.INTEGER) };

    static final int DEP_loadingRemoteDistribute = (int)
            SysProcFragmentId.PF_loadingRemoteDistribute | HStoreConstants.MULTIPARTITION_DEPENDENCY;
    static final int DEP_loadingRemoteAggregate = (int)
            SysProcFragmentId.PF_loadingRemoteAggregate;

    /*
     * (non-Javadoc)
     * @see org.voltdb.VoltSystemProcedure#initImpl()
     */
    @Override
    public void initImpl() {
        executor.registerPlanFragment(SysProcFragmentId.PF_loadingRemoteDistribute, this);
        executor.registerPlanFragment(SysProcFragmentId.PF_loadingRemoteAggregate, this);
    }

    /*
     * (non-Javadoc)
     * @see org.voltdb.VoltSystemProcedure#executePlanFragment(java.lang.Long,
     * java.util.Map, int, org.voltdb.ParameterSet,
     * edu.brown.hstore.PartitionExecutor.SystemProcedureExecutionContext)
     */
    @Override
    public DependencySet executePlanFragment(Long txn_id, Map<Integer, List<VoltTable>> dependencies, int fragmentId, ParameterSet params, SystemProcedureExecutionContext context) {
//        DependencySet result = null;
        assert(params.toArray().length == 4);
        int coordinator = (int) params.toArray()[0];
        String tableName = (String) params.toArray()[1];
        String destinationShim = (String) params.toArray()[2];
        String destinationFile = (String) params.toArray()[3];
        int currentPartitionId = context.getPartitionExecutor().getPartitionId();

        switch (fragmentId) {

            case SysProcFragmentId.PF_loadingRemoteDistribute: {
                ArrayList<Integer> catalogIds = new ArrayList<Integer>();
                catalogIds.add(context.getSite().getId());
                Table loadTable = this.catalogContext.getTableByName(tableName);
                LOG.info("Start loading...");
                long countedRows = executor.getExecutionEngine().loadTableFromFile(
                		loadTable, destinationShim, destinationFile);
                VoltTable result = new VoltTable(new ColumnInfo[] {new ColumnInfo("CountedRows", VoltType.BIGINT)});
                result.addRow(countedRows);
                LOG.info("Loaded rows: " + countedRows);
                return new DependencySet(DEP_loadingRemoteDistribute, result);
            }
            case SysProcFragmentId.PF_loadingRemoteAggregate: {
            	LOG.info("Start accumulating...");
                VoltTable result = VoltTableUtil.union(dependencies.get(DEP_loadingRemoteDistribute));
                int i = 0;
                long aggRows = 0;
                while (i < result.getRowCount()) {
                	aggRows += result.fetchRow(i).getLong(0);
                	i++;
                }
                VoltTable aggResult = new VoltTable(new ColumnInfo[] {new ColumnInfo("CountedRows", VoltType.BIGINT)});
                aggResult.addRow(aggRows);
                LOG.info("Loaded all rows: " + aggRows);
                return new DependencySet(DEP_loadingRemoteAggregate, aggResult);
            }
            default:
                String msg = "Unexpected sysproc fragmentId '" + fragmentId + "'";
                throw new ServerFaultException(msg, txn_id);
        }

    }
    
    
    public VoltTable[] run(int coordinator, String loadingTable, String destinationShim, String destinationFile) throws VoltAbortException {
        ParameterSet params = new ParameterSet();

        params.setParameters(coordinator, loadingTable, destinationShim, destinationFile);
        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];
        // create a work fragment to gather procedure data from each of the sites.
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_loadingRemoteDistribute;
        pfs[1].outputDependencyIds = new int[]{ DEP_loadingRemoteDistribute };
        pfs[1].inputDependencyIds = new int[]{};
        pfs[1].multipartition = true;
        pfs[1].parameters = params;

        // create a work fragment to aggregate the results.
        // Set the MULTIPARTITION_DEPENDENCY bit to require a dependency from every site.
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_loadingRemoteAggregate;
        pfs[0].outputDependencyIds = new int[]{ DEP_loadingRemoteAggregate };
        pfs[0].inputDependencyIds = new int[]{ DEP_loadingRemoteDistribute };
        pfs[0].multipartition = false;
        pfs[0].parameters = params;

        // distribute and execute these fragments providing pfs and id of the
        // aggregator's output dependency table.
        VoltTable[] results = executeSysProcPlanFragments(pfs, DEP_loadingRemoteAggregate);
        return results;

    }
}
