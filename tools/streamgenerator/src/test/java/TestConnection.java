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

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.HashSet;
import streaming.MainIngestor;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class TestConnection {

  private final String IP = "localhost";
  private final String TEST_DATA_FILE = "testdata.txt";
  private final String SAMPLE_TUPLE =
      "|2012-07-07 03:16:55|CMPT|TMB|0|AAAAAAAAAAAADRC|3045|2.82|28|5663|2.89|34.70|36.13|3284.36\n";
  private final int NUM_SAMPLE_TUPLES = 200000;
  private final int DURATION = 3000;
  private final int THROUGHPUT = 10000;
  private final int PORT = 18000;

  private Thread _serverThread;
  private MainRunner _mainRunner;
  private int _maxTuples = -1;

  public class MainRunner implements Runnable {

    MainIngestor _mainIngestor;

    public MainRunner(int port, int maxTuples) {
      System.out.println("Initializing main runner on port " + port);
      _mainIngestor = new MainIngestor(port, THROUGHPUT, DURATION, maxTuples, TEST_DATA_FILE);
    }

    public void stop() {
      _mainIngestor.stopServer();
    }

    public void run() {
      _mainIngestor.startServer();
    }
  }

  public class ConnectionRunner implements Runnable {

    private HashSet<String> _foundTuples;

    public ConnectionRunner(HashSet<String> foundTuples) {
      _foundTuples = foundTuples;
    }

    public void run() {
      Socket clientSocket = null;
      try {
        int serverPort = PORT;

        clientSocket = new Socket(IP, PORT);
        clientSocket.setSoTimeout(5000);

        BufferedInputStream in = new BufferedInputStream(clientSocket.getInputStream());
        String messageString = "";
        int totalTuples = 0;

        long start = System.currentTimeMillis();
        while (true) {
          int length = in.read();
          if(length == -1 || length == 0) {
            break;
          }
          byte[] messageByte = new byte[length];
          in.read(messageByte);
          totalTuples++;
          messageString = new String(messageByte);
          _foundTuples.add(messageString);
        }
        clientSocket.close();
        long end = System.currentTimeMillis();

        double duration = (end - start);
        double tuplesPerSec = totalTuples / (duration / 1000);

        System.out.println("** Emitted " + totalTuples + " tuples in " + duration + " ms");
        System.out.println("** Txn rate: " + tuplesPerSec + " tuples/sec");
      } catch (InterruptedIOException e) {
        System.out.println("Timed out!");
        fail();
      } catch (UnknownHostException e) {
        System.out.println("Sock:" + e.getMessage());
        fail();
      } catch (EOFException e) {
        System.out.println("EOF:" + e.getMessage());
        fail();
      } catch (IOException e) {
        System.out.println("IO:" + e.getMessage());
        fail();
      }
    }
  }

  /**
   * Set up server in separate thread and create dummy data
   */
  public void setup() {

    // Create sample file with plenty of tuples
    try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(TEST_DATA_FILE), "utf-8"))) {
      for (int i = 0; i < NUM_SAMPLE_TUPLES; i++) {
        writer.write(i + SAMPLE_TUPLE);
      }
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }

    _mainRunner = new MainRunner(PORT, _maxTuples);
    _serverThread = new Thread(_mainRunner);
    _serverThread.start();
  }

  @After
  public void cleanUp() {

    _mainRunner.stop();
    try {
      _serverThread.join(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    // Delete sample file
    try {
      Files.delete(new File(TEST_DATA_FILE).toPath());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Test that the server is able to start and has consistent throughput
   */
  @Test
  public void testSingleConnection() {
    setup();
    Socket clientSocket = null;
    try {
      int serverPort = PORT;

      clientSocket = new Socket(IP, serverPort);
      clientSocket.setSoTimeout(5000);

      BufferedInputStream in = new BufferedInputStream(clientSocket.getInputStream());
      String messageString = "";
      int totalTuples = 0;

      long start = System.currentTimeMillis();
      while (true) {
        int length = in.read();
        if (length == -1 || length == 0) {
          break;
        }
        byte[] messageByte = new byte[length];
        in.read(messageByte);
        totalTuples++;
        messageString = new String(messageByte);
      }
      long end = System.currentTimeMillis();
      clientSocket.close();
      double duration = (end - start);
      double tuplesPerSec = totalTuples / (duration / 1000);

      System.out.println("** Emitted " + totalTuples + " tuples in " + duration + " ms");
      System.out.println("** Txn rate: " + tuplesPerSec + " tuples/sec");

      // Test
      // Check that the duration was within 4 seconds of the desired duration
      assertTrue(duration >= DURATION - 2000 && duration <= DURATION + 2000);

      // Test
      // Check that the tp was within 200 tuples per second of the desired duration
      assertTrue(tuplesPerSec >= THROUGHPUT - 100 && tuplesPerSec <= THROUGHPUT + 100);
    } catch (InterruptedIOException e) {
      System.out.println("Timed out!");
      fail();
    } catch (UnknownHostException e) {
      System.out.println("Sock:" + e.getMessage());
      fail();
    } catch (EOFException e) {
      System.out.println("EOF:" + e.getMessage());
      fail();
    } catch (IOException e) {
      System.out.println("IO:" + e.getMessage());
      fail();
    }
  }

  /**
   * Test that two connections will get unique tuples
   */
  @Test
  public void testMultipleConnections() {
    setup();
    HashSet<String> foundTuples1, foundTuples2;
    foundTuples1 = new HashSet<>();
    foundTuples2 = new HashSet<>();
    ConnectionRunner connection1 = new ConnectionRunner(foundTuples1);
    Thread t1 = new Thread(connection1);
    t1.start();

    ConnectionRunner connection2 = new ConnectionRunner(foundTuples2);
    Thread t2 = new Thread(connection2);
    t2.start();

    try {
      t1.join();
      t2.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // Test that there are no duplicates
    foundTuples1.retainAll(foundTuples2);

    assertEquals(0, foundTuples1.size());
  }

  /**
   * Test that two connections will consume exactly the amount of desired tuples.
   */
  @Test
  public void testMaxTuples() {
    _maxTuples = 1000;

    setup();

    HashSet<String> foundTuples1, foundTuples2;
    foundTuples1 = new HashSet<>();
    foundTuples2 = new HashSet<>();
    ConnectionRunner connection1 = new ConnectionRunner(foundTuples1);
    Thread t1 = new Thread(connection1);
    t1.start();

    ConnectionRunner connection2 = new ConnectionRunner(foundTuples2);
    Thread t2 = new Thread(connection2);
    t2.start();

    try {
      t1.join();
      t2.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    assertEquals(1000, foundTuples1.size()+foundTuples2.size());
  }
}
