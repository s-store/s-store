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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class BatchProducer implements Runnable {

    public BlockingQueue<Tuple> queue = new LinkedBlockingQueue<Tuple>();
    private BlockingQueue<Batch> batchQueue;
    private int timeinterval;

    public BatchProducer(BlockingQueue<Batch> q, int timeinterval) {
        this.batchQueue = q;
        this.timeinterval = timeinterval;
    }

    @Override
    public void run() {
        long success_count = 0;

        long batchInterval = this.timeinterval; 

        try {

            try {
                // this.preProcessBenchmark(icc.client);
            } catch (Exception e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }

            long batchid = 0;
            boolean finishOperation = false;

            long currentTimeStamp = System.currentTimeMillis();
            long nextTimeStamp = currentTimeStamp;

            do {
                currentTimeStamp = nextTimeStamp;
                nextTimeStamp = currentTimeStamp + batchInterval;

                // create new batch
                Batch batch = new Batch();
                batch.setID(batchid++);
                //batch.setTimestamp(currentTimeStamp);
                System.out.println("Creating batch-" + batch.getID() + " at time : " + currentTimeStamp);

                finishOperation = false;
                // get all the tuples in a batch interval
                do {
                    // 1. get tuple, and if it is not null then add it to batch
                    try {
                        // 1.1 get the tuple from queue.
                        // question: if queue is empty what happens
                        Tuple tuple = this.queue.take();

                        if (tuple == null || tuple.getFieldLength() == 0) {
                            //System.out.println("Info - BatchProducer : encounter the last empty tuple");
                            finishOperation = true;
                            //System.out.println("Batch : " + batch.toJSONString());
                            batchQueue.put(batch);
                            break;
                        }

                        // 1.2 add the tuple to batch
                        long current = System.currentTimeMillis();
                        tuple.addField("TIMESTAMP", current);

                        batch.addTuple(tuple);

                        //
                        if (current >= nextTimeStamp) {
                            //System.out.println("Finish packaging batch-" + batch.getID() + " and put it in queue at time : " + current);
                            // put this batch into batch queue
                            batch.setTimestamp(current);
                            //System.out.println("Batch : " + batch.toJSONString());
                            batchQueue.put(batch);
                            // break to next interval
                            break;
                        }
                    } catch (Exception ex) {
                        System.out.println("Queue get error: " + ex.getMessage());
                        Throwable cause = ex.getCause();
                        if (cause != null) {
                            System.out.println("Error cause: " + cause.getMessage());
                        }
                    }

                } while (true);

                // print for debugging
                System.out.println("Batch-" + batch.getID() + " : " + currentTimeStamp + " - #tuples : " + batch.getSize());

                if (finishOperation == true)
                    break;

            } while (true);

        } finally {
            try {
                // put last empty batch
                Batch batch = new Batch();
                this.batchQueue.put(batch);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}