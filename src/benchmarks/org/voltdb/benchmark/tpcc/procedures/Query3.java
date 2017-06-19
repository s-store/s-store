package org.voltdb.benchmark.tpcc.procedures;

import java.util.HashMap;
import java.util.Map.Entry;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

@ProcInfo(
    singlePartition = false
)
public class Query3 extends VoltProcedure {
    
    public SQLStmt query = new SQLStmt(
            " SELECT ol_o_id, ol_w_id, ol_d_id, SUM(ol_amount) as revenue, o_entry_d " +
            " FROM CUSTOMER, NEW_ORDER, ORDERS, ORDER_LINE " +
            " WHERE   c_id = o_c_id " + 
            " and c_w_id = o_w_id " +
            " and c_d_id = o_d_id " +
            " and no_w_id = o_w_id " +
            " and no_d_id = o_d_id " +
            " and no_o_id = o_id " +
            " and ol_w_id = o_w_id " +
            " and ol_d_id = o_d_id " +
            " and ol_o_id = o_id" +
            " and o_entry_d > '2007-01-02 00:00:00.000000' " +
            //" GROUP BY ol_o_id " +
            " GROUP BY ol_o_id, ol_w_id, ol_d_id, o_entry_d " + // mr_transaction can not support multi-column-keys right now
            //" ORDER BY revenue desc, o_entry_d"); // error: "ORDER BY with complex expressions not yet supported
            " ORDER BY o_entry_d"
    );
    
    
    public VoltTable[] run() {
	    voltQueueSQL(query);
	    VoltTable v[] = voltExecuteSQL();
	    return v;
    }
    
}
