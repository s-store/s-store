/***************************************************************************
 *  Copyright (C) 2017 by S-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Portland State University                                              *
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

package streaming;

import com.google.common.util.concurrent.RateLimiter;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;


public class StreamServer extends Thread {
  private int _maxTuples;
  private int _duration;
  private BufferedReader _sourceBuffer;
  private BufferedOutputStream _output;
  private Socket _clientSocket;
  private RateLimiter _rateLimiter;
  private final int END_OF_STREAM_SIG = 0;
  private long _startTime;
  private AtomicInteger _cosumedTuples;

  public StreamServer(Socket aClientSocket, RateLimiter rateLimiter, long startTime, int duration,
      BufferedReader dataSource, AtomicInteger consumedTuples, int maxTupels) {

    try {
      _duration = duration;
      _sourceBuffer = dataSource;
      _rateLimiter = rateLimiter;
      _clientSocket = aClientSocket;
      _startTime = startTime;
      _cosumedTuples = consumedTuples;
      _maxTuples = maxTupels;
      _output = new BufferedOutputStream(_clientSocket.getOutputStream());
      this.start();
    } catch (IOException e) {
      System.out.println(e.getMessage());
    }
  }

  public void run() {
    try {

      System.out.println("Client " +
          _clientSocket.getInetAddress() + ":" +
          _clientSocket.getPort() + " connected. Starting stream..");

      String tuple;
      int localTuples = 0;
      while ((tuple = _sourceBuffer.readLine()) != null) {
        _rateLimiter.acquire();
        _cosumedTuples.incrementAndGet();
        if (_maxTuples != -1 && _cosumedTuples.get() > _maxTuples) {
          break;
        }

        _output.write(tuple.length());
        _output.write(tuple.getBytes());

        localTuples++;
        if (_maxTuples == -1 && System.currentTimeMillis() - _startTime > _duration) {
          break;
        }
      }
      // Send kill signal
      _output.write(END_OF_STREAM_SIG);
      _output.close();
      System.out.println("Stream ended. Streamed " + localTuples + " tuples.");
    } catch (EOFException e) {
      System.out.println("EOF:" + e.getMessage());
    } catch (IOException e) {
      System.out.println("IO:" + e.getMessage());
    } finally {
      try {
        _clientSocket.close();
      } catch (IOException e) {/*close failed*/}
    }
  }
}

