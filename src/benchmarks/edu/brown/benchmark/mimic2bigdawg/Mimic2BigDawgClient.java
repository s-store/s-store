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

package edu.brown.benchmark.mimic2bigdawg;

import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.mimic2bigdawg.procedures.DataIngest;
import edu.brown.benchmark.mimic2bigdawg.procedures.Initialize;
import edu.brown.logging.LoggerUtil;

import org.apache.log4j.Logger;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.EOFException;
import java.io.InterruptedIOException;

import java.net.UnknownHostException;
import java.net.Socket;

public class Mimic2BigDawgClient extends BenchmarkComponent{
    private static final Logger LOG = Logger.getLogger(Mimic2BigDawgClient.class);
    private static final LoggerUtil.LoggerBoolean debug = new LoggerUtil.LoggerBoolean();


    final Callback callback = new Callback();

    public Mimic2BigDawgClient(String[] args) {
        super(args);
    }


    @Override
    public String[] getTransactionDisplayNames() {
        String procNames[] = new String[]{
                Initialize.class.getSimpleName(),
//                DataIngest.class.getSimpleName()
        };
        return (procNames);
    }


    @Override
    public boolean runOnce() {
        return false;
    }

    @Override
    public void runLoop() throws IOException {
    	Socket clientSocket = null;
        boolean response = false;
        int numRow = 0;
        try {
        	clientSocket = new Socket(Mimic2BigDawgConstants.STREAMINGESTOR_HOST, Mimic2BigDawgConstants.STREAMINGESTOR_PORT);
            clientSocket.setSoTimeout(5000);

            BufferedInputStream in = new BufferedInputStream(clientSocket.getInputStream());
            LOG.info("Start ingesting data using streaming generator");
            while (true) {
                int length = in.read();
                if (length == -1 || length == 0) {
                    break;
                }
                byte[] messageByte = new byte[length];
                in.read(messageByte);
                String tuple = new String(messageByte);
                numRow++;
                Client client = this.getClientHandle();
                response = client.callProcedure(callback, "Initialize", tuple);
            }
        } catch (InterruptedIOException e) {
            System.out.println("Timed out!");
        } catch (UnknownHostException e) {
            System.out.println("Sock:" + e.getMessage());
        } catch (EOFException e) {
            System.out.println("EOF:" + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO:" + e.getMessage());
        }
        LOG.info("Received " + numRow + " tuples in total");
    }

    private class Callback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            // Increment the BenchmarkComponent's internal counter on the
            // number of transactions that have been completed
            incrementTransactionCounter(clientResponse, 0);
        }
    }
}
