/**
 * @author Wang Hao <wanghao.buaa@gmail.com>
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
package edu.brown.stream;

import java.io.IOException;

import org.voltdb.client.ClientResponse;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializable;
import org.voltdb.messaging.FastSerializer;

import edu.brown.hstore.Hstoreservice.Status;

public class WorkflowResponseImpl implements WorkflowResponse, FastSerializable {

    private long initiateTime = -1;
    private long endTime = -1;
    Status status = Status.OK;
    
    public WorkflowResponseImpl() {
        // TODO Auto-generated constructor stub
    }

    @Override
    public boolean isInitialized() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void finish() {
        // TODO Auto-generated method stub

    }

    @Override
    public void readExternal(FastDeserializer in) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void writeExternal(FastSerializer out) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public int getClusterRoundtrip() {
        // TODO Auto-generated method stub
        return (int)(endTime-initiateTime);
    }

    @Override
    public Status getStatus() {
        return status;
    }

    @Override
    public void addClientResponse(ClientResponse cresponse) {
        if(initiateTime==-1)
            initiateTime = cresponse.getInitiateTime();
        else
            initiateTime = Math.min(cresponse.getInitiateTime(),initiateTime);
        endTime = Math.max(cresponse.getInitiateTime()+(long)cresponse.getClusterRoundtrip(),endTime);
        
        if (cresponse.getStatus() != Status.OK)
        {
            // FIXME: we should maintain a map for all procedures, not just a value
            status = cresponse.getStatus();
        }            
    }

}
