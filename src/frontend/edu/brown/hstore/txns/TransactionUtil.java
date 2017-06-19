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

package edu.brown.hstore.txns;

import org.voltdb.catalog.Procedure;
import org.voltdb.messaging.FastSerializer;

import com.google.protobuf.ByteString;

import edu.brown.hstore.Hstoreservice.TransactionInitRequest;

/**
 * Simple utility methods for transactions
 */
public abstract class TransactionUtil {

    protected static String debugStmtDep(int stmt_counter, int dep_id) {
        return String.format("{StmtCounter:%d, DependencyId:%d}", stmt_counter, dep_id);
    }

    protected static String debugPartDep(int partition, int dep_id) {
        return String.format("{Partition:%d, DependencyId:%d}", partition, dep_id);
    }
    
    protected static String debugStmtFrag(int stmtCounter, int fragment_id) {
        return String.format("{StmtCounter:%d, FragmentId:%d}", stmtCounter, fragment_id);
    }

    public static String formatTxnName(Procedure catalog_proc, Long txn_id) {
        if (catalog_proc != null) {
            return (catalog_proc.getName() + " #" + txn_id);
        }
        return ("#" + txn_id);
    }

    /**
     * Create a TransactionInitRequest builder for the given txn.
     * If paramsSerializer is not null, we will include the procedure ParameterSet
     * in the builder's message.
     * @param ts
     * @param paramsSerializer
     * @return
     */
    public static TransactionInitRequest.Builder createTransactionInitBuilder(LocalTransaction ts, FastSerializer paramsSerializer) {
        TransactionInitRequest.Builder builder = TransactionInitRequest.newBuilder()
                                                        .setBatchId(ts.getBatchId())
                                                        .setClientHandle(ts.getClientHandle()) // added by hawk, 2014/6/16
                                                        //.setInitiateTime(ts.getInitiateTime()) //added by hawk, 2013/11/20
                                                        .setTransactionId(ts.getTransactionId().longValue())
                                                        .setProcedureId(ts.getProcedure().getId())
                                                        .setBasePartition(ts.getBasePartition())
                                                        .addAllPartitions(ts.getPredictTouchedPartitions());
        if (paramsSerializer != null) {
            FastSerializer fs = paramsSerializer;
            try {
                fs.clear();
                ts.getProcedureParameters().writeExternal(fs);
                builder.setProcParams(ByteString.copyFrom(fs.getBBContainer().b));
            } catch (Exception ex) {
                throw new RuntimeException("Failed to serialize ParameterSet for " + ts, ex);
            }
        }
        
        return (builder);
    }
    
}
