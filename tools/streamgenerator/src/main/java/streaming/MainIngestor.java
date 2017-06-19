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
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;


public class MainIngestor {

  public static final String APP_NAME = "StreamIngestor";
  private int _serverPort = 18000;
  private int _throughPut = 10000;
  private int _duration = 3000;
  private int _maxTupels = -1;
  private String _dataSourceFile = "";
  private ServerSocket _listenSocket;
  private AtomicInteger _consumedTuples;

  public MainIngestor(int serverPort, int throughPut, int duration, int maxTupels, String dataSourceFile) {
    _serverPort = (serverPort == 0) ? _serverPort : serverPort;
    _throughPut = (throughPut == 0) ? _throughPut : throughPut;
    _duration = (duration == 0) ? _duration : duration;
    _dataSourceFile = dataSourceFile;
    _consumedTuples = new AtomicInteger(0);
    _maxTupels = maxTupels;
  }

  public void startServer() {
    try {
      _listenSocket = new ServerSocket(_serverPort);
      BufferedReader dataSource = new BufferedReader(new FileReader(new File(_dataSourceFile)));
      RateLimiter rateLimiter = RateLimiter.create(_throughPut);
      long startTime = 0;
      System.out.println("Server started on port " + _serverPort + "..");
      while (true) {
        if (_listenSocket.isClosed()) {
          printStats();
          break;
        }
        Socket clientSocket = _listenSocket.accept();
        if (startTime == 0) {
          startTime = System.currentTimeMillis();
        }
        StreamServer c = new StreamServer(clientSocket, rateLimiter, startTime, _duration, dataSource, _consumedTuples, _maxTupels);
        if (System.currentTimeMillis() - startTime > _duration + 3000) {
          printStats();
          break;
        }
      }
    } catch (IOException e) {
      System.out.println("Error: " + e.getMessage() + " (using localhost:" + _serverPort + ")");
    } catch (Exception e) {
      System.out.println("Shutting down due to interruption..");
    }
  }

  public void printStats() {
    double actualTP = (double) _consumedTuples.get() / (_duration / 1000.0);
    System.out.println("Shutting down server..");
    System.out.println("Consumed tuples: " + _consumedTuples.get());
    System.out.println("Actual throughput: " + actualTP + " tuples/sec");
  }

  public void stopServer() {
    try {
      printStats();
      _listenSocket.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    int serverPort = 0, throughPut = 0, duration = 0, maxTuples = -1;
    String dataSourceFile = null;

    Options options = new Options();

    options.addOption("h", "help", false, "Show this dialog");
    options.addOption("p", "port", true, "The port that the server runs on");
    options.addOption("t", "throughput", true, "The amount of tuples per second that will be streamed");
    options.addOption("d", "duration", true, "The amount of time in ms that tuples will be streamed");
    options.addOption("f", "file", true, "The input file with tuples separated by line breaks");
    options.addOption("m", "maxtuples", true, "Max tuples to read from file. This will override any time contraints and close the stream when all tuples have been streamed.");

    CommandLineParser parser = new BasicParser();
    HelpFormatter formater = new HelpFormatter();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);

      if (cmd.hasOption("h")) {
        formater.printHelp(APP_NAME, options);
        System.exit(0);
      }

      if (cmd.hasOption("m")) {
        maxTuples = Integer.parseInt(cmd.getOptionValue("m"));
      }
      if (cmd.hasOption("p")) {
        serverPort = Integer.parseInt(cmd.getOptionValue("p"));
      }
      if (cmd.hasOption("t")) {
        throughPut = Integer.parseInt(cmd.getOptionValue("t"));
      }
      if (cmd.hasOption("d")) {
        duration = Integer.parseInt(cmd.getOptionValue("d"));
      }
      if (cmd.hasOption("f")) {
        dataSourceFile = cmd.getOptionValue("f");
      } else {
        System.err.println("Please specifiy an input file");
        System.exit(1);
      }
    } catch (Exception e) {
      formater.printHelp(APP_NAME, options);
      System.exit(0);
    }

    final MainIngestor mainIngestor = new MainIngestor(serverPort, throughPut, duration, maxTuples, dataSourceFile);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        mainIngestor.printStats();
      }
    });
    mainIngestor.startServer();


  }
}
