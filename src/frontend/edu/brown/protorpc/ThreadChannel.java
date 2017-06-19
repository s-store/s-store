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

package edu.brown.protorpc;

import java.util.concurrent.LinkedBlockingQueue;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcChannel;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

/** Runs a service in a separate thread. */
public class ThreadChannel extends Thread implements RpcChannel {
    public final class TableTask implements RpcCallback<Message>, Runnable {
        private final MethodDescriptor method;
        private final Message request;
        private final RpcCallback<Message> finalCallback;
        private Message response;

        public TableTask(MethodDescriptor method, Message request,
                RpcCallback<Message> finalCallback) {
            this.method = method;
            this.request = request;
            this.finalCallback = finalCallback;
        }

        public RpcCallback<Message> getFinalCallback() { return finalCallback; }

        // RpcCallback: used when finished processing inside this thread
        @Override
        public void run(Message parameter) {
            assert response == null;
            assert parameter != null;
            response = parameter;
//            System.out.println("scheduling callback in original thread");
            outputEventLoop.runInEventThread(this);
        }

        // Callback: used to call the finalCallback in the EventLoop thread
        @Override
        public void run() {
//            System.out.println("calling callback in original thread");
            assert response != null;
            finalCallback.run(response);
            response = null;
        }
    }

    private final LinkedBlockingQueue<TableTask> inputQueue = new LinkedBlockingQueue<TableTask>();
//    private final TableService table = new TableServer();
    private final EventLoop outputEventLoop;
    private final Service service;

    public ThreadChannel(EventLoop outputEventLoop, Service service) {
        this.outputEventLoop = outputEventLoop;
        this.service = service;
    }

    public void run() {
        while (true) {
            TableTask task;
            try {
                task = inputQueue.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (task.request == null) {
                // TODO: Abort outstanding transactions?
                return;
            }

            service.callMethod(task.method, null, task.request, task);
        }
    }

    @Override
    public void callMethod(MethodDescriptor method, RpcController controller,
            Message request, Message responsePrototype,
            RpcCallback<Message> done) {
//        System.out.println("added task to thread");
        inputQueue.add(new TableTask(method, request, done));
    }

    public void shutDownThread() {
        inputQueue.add(new TableTask(null, null, null));
    }
}
