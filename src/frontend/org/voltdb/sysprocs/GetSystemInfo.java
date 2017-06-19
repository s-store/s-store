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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.voltdb.DependencySet;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.types.IndexType;

import edu.brown.hstore.PartitionExecutor.SystemProcedureExecutionContext;

/**
 * Access the meta data of the database. Tables, views, columns, procedures, indexes etc.
 */
@ProcInfo(
    singlePartition = false
)

public class GetSystemInfo extends VoltSystemProcedure {
    private static final Logger HOST_LOG = Logger.getLogger(GetSystemInfo.class);

    @Override
    public void initImpl() {}

    @Override
    public DependencySet executePlanFragment(Long txn_id,
                                             Map<Integer, List<VoltTable>> dependencies,
                                             int fragmentId,
                                             ParameterSet params,
                                             SystemProcedureExecutionContext context) {
    	return null;
    }

    /**
     * Returns specific info about database depending on selector.
     * requested.
     * @param selector     Selector requested TABLES, COLUMNS, PROCEDURES, INDEXES etc
     * @return             The information about the specified selector.
     * @throws VoltAbortException
     */
    public VoltTable run(String selector) throws VoltAbortException {
    	VoltTable result = null;
    	
        int i = 0;
        
        /*
         * access the specified information type and return as a table.
         */
        if(selector.equals("TABLES")){
        	HOST_LOG.log(Priority.INFO, "accessing table info");
        	
        	ColumnInfo c1 = new ColumnInfo("TABLE_NAME", VoltType.STRING);
        	ColumnInfo c2 = new ColumnInfo("TABLE_TYPE", VoltType.STRING);
    		ColumnInfo c3 = new ColumnInfo("ROW_COUNT", VoltType.INTEGER);
    		ColumnInfo c4 = new ColumnInfo("COL_COUNT", VoltType.INTEGER);
    		
    		result = new VoltTable(c1, c2, c3, c4);
            Collection<Table> dataTables = catalogContext.getDataTables();
            Collection<Table> viewTables = catalogContext.getViewTables();
        	
        	for(Table t : dataTables){
        		result.addRow(t.fullName(), "TABLE", t.getEstimatedtuplecount(), t.getColumns().size());
        		i++;
        	}
        	
        	for(Table t : viewTables){
        		result.addRow(t.fullName(), "VIEW", t.getEstimatedtuplecount(), t.getColumns().size());
        		i++;
        	}
        } else if (selector.equals("COLUMNS")){
        	HOST_LOG.log(Priority.INFO, "accessing column info");
        	
        	ColumnInfo c1 = new ColumnInfo("TABLE_NAME", VoltType.STRING);
        	ColumnInfo c2 = new ColumnInfo("COLUMN_NAME", VoltType.STRING);
        	ColumnInfo c3 = new ColumnInfo("TYPE_NAME", VoltType.STRING);
        	ColumnInfo c4 = new ColumnInfo("COLUMN_SIZE", VoltType.INTEGER);
        	ColumnInfo c5 = new ColumnInfo("IS_NULLABLE", VoltType.BOOLEAN);
        	ColumnInfo c6 = new ColumnInfo("RELATIVE_INDEX", VoltType.INTEGER);
        	
        	result = new VoltTable(c1, c2, c3, c4, c5, c6);
            Collection<Table> dataTables = catalogContext.getDataTables();
            Collection<Table> viewTables = catalogContext.getViewTables();
        	
        	for(Table t : dataTables){
        		Collection<Column> c = t.getColumns();
        		for(Column val : c){
        			result.addRow(t.fullName(), val.getName(), VoltType.get(val.getType()).name(),
        					val.getSize(), val.getNullable(), val.getIndex());
        		}
        	}
        	for(Table t : viewTables){
        		Collection<Column> c = t.getColumns();
        		for(Column val : c){
        			result.addRow(t.fullName(), val.getName(), VoltType.get(val.getType()).name(),
        					val.getSize(), val.getNullable(), val.getIndex());
        		}
        	}
        	
        } else if (selector.equals("INDEXINFO")){
        	HOST_LOG.log(Priority.INFO, "accessing index info");
        	
        	ColumnInfo c1 = new ColumnInfo("TABLE_NAME", VoltType.STRING);
        	ColumnInfo c2 = new ColumnInfo("INDEX_NAME", VoltType.STRING);
        	ColumnInfo c3 = new ColumnInfo("INDEX_TYPE", VoltType.STRING);
        	ColumnInfo c4 = new ColumnInfo("NON_UNIQUE", VoltType.BOOLEAN);
        	ColumnInfo c5 = new ColumnInfo("RELATIVE_INDEX", VoltType.INTEGER);
        	ColumnInfo c6 = new ColumnInfo("COLUMN_NAME", VoltType.STRING);
        	ColumnInfo c7 = new ColumnInfo("COLUMN_INDEX", VoltType.STRING);
        	
        	
        	result = new VoltTable(c1, c2, c3, c4, c5, c6, c7);
            Collection<Table> dataTables = catalogContext.getDataTables();
            Collection<Table> viewTables = catalogContext.getViewTables();
        	
        	for (Table t : dataTables) {
        		Collection<Index> indexes = t.getIndexes();
        		for (Index val : indexes) {
        			//relative index and name of the column(s) of a Index
        			StringBuilder columnsIndex = new StringBuilder();
        			StringBuilder columnsName = new StringBuilder();
        			for (ColumnRef index_column_ref : val.getColumns()) {
        				columnsName.append(index_column_ref.getColumn().getName()).append(",");
        				columnsIndex.append(index_column_ref.getColumn().getIndex()).append(",");
        			}
        			columnsName.deleteCharAt(columnsName.length() - 1);
        			columnsIndex.deleteCharAt(columnsIndex.length() - 1);
        			result.addRow(t.fullName(), val.getName(), IndexType.get(val.getType()).name(),
        					!val.getUnique(), val.getRelativeIndex(),
        					columnsName.toString(), columnsIndex.toString());
        		}
        	}
        	
        	for(Table t : viewTables){
        		Collection<Index> indexes = t.getIndexes();
        		for(Index val : indexes){
        			//relative index and name of the column(s) of a Index
        			StringBuilder columnsIndex = new StringBuilder();
        			StringBuilder columnsName = new StringBuilder();
        			for (ColumnRef index_column_ref : val.getColumns()) {
        				columnsName.append(index_column_ref.getColumn().getName()).append(",");
        				columnsIndex.append(index_column_ref.getColumn().getIndex()).append(",");
        			}
        			columnsName.deleteCharAt(columnsName.length() - 1);
        			columnsIndex.deleteCharAt(columnsIndex.length() - 1);
        			result.addRow(t.fullName(), val.getName(), IndexType.get(val.getType()).name(),
        					!val.getUnique(), val.getRelativeIndex(),
        					columnsName.toString(), columnsIndex.toString());
        		}
        	}
        } else if(selector.equals("PRIMARYKEYS")){
        	ColumnInfo c1 = new ColumnInfo("TABLE_NAME", VoltType.STRING);
        	ColumnInfo c2 = new ColumnInfo("PK_NAME", VoltType.STRING);
        	ColumnInfo c3 = new ColumnInfo("RELATIVE_INDEX", VoltType.INTEGER);
        	ColumnInfo c4 = new ColumnInfo("COLUMN_NAME", VoltType.STRING);
        	ColumnInfo c5 = new ColumnInfo("COLUMN_INDEX", VoltType.STRING);
        	
        	result = new VoltTable(c1, c2, c3, c4, c5);
            Collection<Table> dataTables = catalogContext.getDataTables();
            Collection<Table> viewTables = catalogContext.getViewTables();
        	
        	for(Table t : dataTables){
        		Collection<Index> indexes = t.getIndexes();
        		for(Index val : indexes){
        			if(val.getName().contains("PK") && val.getUnique()){
        				//relative index and name of the column(s) of a primary key
            			StringBuilder columnsIndex = new StringBuilder();
            			StringBuilder columnsName = new StringBuilder();
            			for (ColumnRef index_column_ref : val.getColumns()) {
            				columnsName.append(index_column_ref.getColumn().getName()).append(",");
            				columnsIndex.append(index_column_ref.getColumn().getIndex()).append(",");
            			}
            			columnsName.deleteCharAt(columnsName.length() - 1);
            			columnsIndex.deleteCharAt(columnsIndex.length() - 1);
        				result.addRow(t.fullName(), val.getName(), val.getRelativeIndex(),
        						columnsName.toString(), columnsIndex.toString());
        			}	
        		}
        	}
        	
        	for(Table t : viewTables){
        		Collection<Index> indexes = t.getIndexes();
        		for(Index val : indexes){
        			if(val.getName().contains("PK") && val.getUnique()){
        				//relative index and name of the column(s) of a primary key
            			StringBuilder columnsIndex = new StringBuilder();
            			StringBuilder columnsName = new StringBuilder();
            			for (ColumnRef index_column_ref : val.getColumns()) {
            				columnsName.append(index_column_ref.getColumn().getName()).append(",");
            				columnsIndex.append(index_column_ref.getColumn().getIndex()).append(",");
            			}
            			columnsName.deleteCharAt(columnsName.length() - 1);
            			columnsIndex.deleteCharAt(columnsIndex.length() - 1);
        				result.addRow(t.fullName(), val.getName(), val.getRelativeIndex(),
        						columnsName.toString(), columnsIndex.toString());
        			}
        		}
        	}
        	
        } else if (selector.equals("PROCEDURES")) {
        	HOST_LOG.log(Priority.INFO, "accessing procedure info");
        	
        	ColumnInfo c1 = new ColumnInfo("PROCEDURE_NAME", VoltType.STRING);
        	
        	Collection<Procedure> procedures = catalogContext.getRegularProcedures();
        	
        	result = new VoltTable(c1);
        	
        	for(Procedure proc : procedures){
        		result.addRow(proc.getName());
        	}
        } else if(selector.equals("PROCEDURECOLUMNS")){
        	HOST_LOG.log(Priority.INFO, "accessing procedure column info");
        	
        	ColumnInfo c1 = new ColumnInfo("PROCEDURE_NAME", VoltType.STRING);
        	ColumnInfo c2 = new ColumnInfo("COLUMN_NAME", VoltType.STRING);
        	
        	Collection<Procedure> procedures = catalogContext.getRegularProcedures();
        	
        	result = new VoltTable(c1, c2);
           
        	for(Procedure proc : procedures){
        		for(ProcParameter v : proc.getParameters()){
        			result.addRow(proc.getName(), v.fullName());
        		}
        		
        	}
        }
        
        
        return result;
    }
}
