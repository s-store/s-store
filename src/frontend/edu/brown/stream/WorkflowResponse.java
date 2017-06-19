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

import org.voltdb.client.ClientResponse;

import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.pools.Poolable;

/**
 *  Interface implemented by the responses that are generated for procedure invocations
 */

public interface WorkflowResponse extends Poolable {
    /**
    * Get an estimate of the amount of time it took for the database
    * to process the stream workflow from the time it was received at the initiating node to the time
    * the ending node got the response and queued it for transmission to the client.
    * This time is an calculated from all the related transactions executed in the workflow
    * @return Time in milliseconds the workflow spent in the cluster
    */
   public int getClusterRoundtrip();
   
   /**
    * Retrieve the status code returned by the server
    * @return Status code
    */
   public Status getStatus();

   
   public void addClientResponse(final ClientResponse cresponse);

}
