/***************************************************************************
 *   Copyright (C) 2009 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/
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

package edu.brown.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

public class LatencyLogger {
	private static final Logger LOG = Logger.getLogger(LatencyLogger.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
	
	private static HashMap<Long, LatencyEntry> entries = new HashMap<Long, LatencyEntry>();
	private static HashMap<String, AtomicInteger> counts = new HashMap<String, AtomicInteger>();
	private static HashMap<String, AtomicLong> totalLatency = new HashMap<String, AtomicLong>();
	private static String filename;
	private static AtomicLong currentId = new AtomicLong();
	
	public static void setFilename(String fn) {
		filename = fn;
	}
	
	public static long newEntry(String className, String description)
	{
		long id = currentId.incrementAndGet();
		LatencyEntry newEntry = new LatencyEntry(id, className, description, System.nanoTime());
		entries.put(id, newEntry);
		return id;
	}
	
	public static void closeEntry(long id)
	{
		LatencyEntry e = entries.get(id);
		e.setEndTime(System.nanoTime());
		AtomicLong lat;
		AtomicInteger count;
		//LOG.info("key " + e.className);
		if(!totalLatency.containsKey(e.className))
		{
			count = new AtomicInteger(1);
			lat = new AtomicLong(e.eventTime);
			counts.put(e.className, count);
			totalLatency.put(e.className, lat);
		}
		else {
			lat = totalLatency.get(e.className);
			count = counts.get(e.className);
			lat.getAndAdd(e.eventTime);
			count.getAndIncrement();
			if(count.get() % 1000 == 0)
			{
				long avgLat = lat.get() / count.get() / 1000;
				LOG.info(e.className + ": " + avgLat + " ms");
				//writer.println(key + ": " + avgLat + " ms");
				//LOG.info("key: " + e.className + " lat: " + lat.get() + " count: " + count.get());
			}
		}
		
		entries.remove(id);
	}
	
	public static void printLog(String fn)
	{
		try {
			PrintWriter writer = new PrintWriter(filename);
		
			writer.println("writing");
			writer.println(fn);
			//writer.println("ProcOne: " + totalLatency.get("ProcOne").get());
			
			
			//Set<String> keys = counts.keySet();
			//Iterator<String> allKeys = keys.iterator();
			/**
			while(allKeys.hasNext()){
				String key = allKeys.next();
				writer.println("total latency: " + key);
				//long avgLat = totalLatency.get(key).get() / counts.get(key).get();
				//LOG.info(key + ": " + avgLat + " ms");
				//writer.println(key + ": " + avgLat + " ms");
			}
			LOG.info("finished printing");*/
			writer.flush();
			writer.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}