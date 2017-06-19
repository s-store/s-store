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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

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
import org.voltdb.SQLStmt;

import edu.brown.hstore.AntiCacheManager;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.PartitionExecutor.SystemProcedureExecutionContext;
import edu.brown.hstore.internal.ExtractionRequestMessage;
import edu.brown.hstore.internal.ExtractionRequestMessage.RequestType;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;

/**
 * Initiate a data extraction for ETL
 * 
 * @author aelmore
 */
@ProcInfo(singlePartition = false)

public class ExtractionRemote extends VoltSystemProcedure {

    private static final Logger LOG = Logger.getLogger(ExtractionRemote.class);

    public static final ColumnInfo nodeResultsColumns[] = { new ColumnInfo("SITE", VoltType.INTEGER) };
//    private ExtractionRequestMessage requestMsg;

    static final int DEP_extractionRemoteDistribute = (int)
            SysProcFragmentId.PF_extractionRemoteDistribute | HStoreConstants.MULTIPARTITION_DEPENDENCY;
    static final int DEP_extractionRemoteAggregate = (int)
            SysProcFragmentId.PF_extractionRemoteAggregate;

    /*
     * (non-Javadoc)
     * @see org.voltdb.VoltSystemProcedure#initImpl()
     */
    @Override
    public void initImpl() {
        executor.registerPlanFragment(SysProcFragmentId.PF_extractionRemoteDistribute, this);
        executor.registerPlanFragment(SysProcFragmentId.PF_extractionRemoteAggregate, this);
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
        assert(params.toArray().length == 5);
        int coordinator = (int) params.toArray()[0];
        String tableName = (String) params.toArray()[1];
        String destinationShim = (String) params.toArray()[2];
        String destinationFile = (String) params.toArray()[3];
        boolean caching = (boolean) params.toArray()[4];
        int currentPartitionId = context.getPartitionExecutor().getPartitionId();

//        requestMsg = new ExtractionRequestMessage(RequestType.extractRequest, tableName, destinationShim, destinationFile);
        
        switch (fragmentId) {

            case SysProcFragmentId.PF_extractionRemoteDistribute: {
//                hstore_site.initExtraction(requestMsg);
//                VoltTable vt = new VoltTable(nodeResultsColumns);
//
//                vt.addRow(hstore_site.getSiteId());
//
//            	result = new DependencySet(SysProcFragmentId.PF_extractionRemoteDistribute, 
//            			requestMsg.getResults());
//            	break;
                ArrayList<Integer> catalogIds = new ArrayList<Integer>();
                catalogIds.add(context.getSite().getId());
                Table extractTable = this.catalogContext.getTableByName(tableName);
                long countedRows = executor.getExecutionEngine().extractTable(
                		extractTable, destinationShim, destinationFile, caching);
                VoltTable result = new VoltTable(new ColumnInfo[] {new ColumnInfo("CountedRows", VoltType.BIGINT)});
                result.addRow(countedRows);
                return new DependencySet(DEP_extractionRemoteDistribute, result);
            }
            case SysProcFragmentId.PF_extractionRemoteAggregate: {
//                try {
//                    hstore_site.terminateExtractionSysProc();
//
//                } catch (Exception ex) {
//                    throw new ServerFaultException(ex.getMessage(), txn_id);
//                }
//                List<VoltTable> siteResults = dependencies.get(SysProcFragmentId.PF_extractionRemoteDistribute);
//                if (siteResults == null || siteResults.isEmpty()) {
//                    String msg = "Missing site results";
//                    throw new ServerFaultException(msg, txn_id);
//                }
//
//                VoltTable vt = VoltTableUtil.union(siteResults);
//
//                result = new DependencySet(SysProcFragmentId.PF_extractionRemoteAggregate, vt);
//                break;
                VoltTable result = VoltTableUtil.union(dependencies.get(DEP_extractionRemoteDistribute));
                int i = 0;
                long aggRows = 0;
                while (i < result.getRowCount()) {
                	aggRows += result.fetchRow(i).getLong(0);
                	i++;
                }
                VoltTable aggResult = new VoltTable(new ColumnInfo[] {new ColumnInfo("CountedRows", VoltType.BIGINT)});
                aggResult.addRow(aggRows);
                return new DependencySet(DEP_extractionRemoteAggregate, aggResult);
            }
            default:
                String msg = "Unexpected sysproc fragmentId '" + fragmentId + "'";
                throw new ServerFaultException(msg, txn_id);
        }

    }
    
    
    public VoltTable[] run(int coordinator, String extractionTable, String destinationShim, String destinationFile, 
    		String caching) throws VoltAbortException {
//        String msg = String.format("Coordinator=%s, extractionTable=%s, destinationShim=%s", coordinator, extractionTable, destinationShim);
//        LOG.info(String.format("RUN : Init extraciton. %s", msg));
        ParameterSet params = new ParameterSet();

        params.setParameters(coordinator, extractionTable, destinationShim, destinationFile, Boolean.parseBoolean(caching));
//        return this.executeOncePerSite(SysProcFragmentId.PF_extractionRemoteDistribute, SysProcFragmentId.PF_extractionRemoteAggregate, params)[0];
        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];
        // create a work fragment to gather procedure data from each of the sites.
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_extractionRemoteDistribute;
        pfs[1].outputDependencyIds = new int[]{ DEP_extractionRemoteDistribute };
        pfs[1].inputDependencyIds = new int[]{};
        pfs[1].multipartition = true;
        pfs[1].parameters = params;

        // create a work fragment to aggregate the results.
        // Set the MULTIPARTITION_DEPENDENCY bit to require a dependency from every site.
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_extractionRemoteAggregate;
        pfs[0].outputDependencyIds = new int[]{ DEP_extractionRemoteAggregate };
        pfs[0].inputDependencyIds = new int[]{ DEP_extractionRemoteDistribute };
        pfs[0].multipartition = false;
        pfs[0].parameters = params;

        // distribute and execute these fragments providing pfs and id of the
        // aggregator's output dependency table.
        VoltTable[] results = executeSysProcPlanFragments(pfs, DEP_extractionRemoteAggregate);
        
        return results;

    }
}
