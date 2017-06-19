/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original By: VoltDB Inc.											   *
 *  Ported By:  Justin A. DeBrabant (http://www.cs.brown.edu/~debrabant/)  *                                                                      *
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
package edu.brown.benchmark.tpcdi.orig;

import edu.brown.hstore.conf.HStoreConf;


public abstract class TPCDIConstants {

  public static final int NUM_PARTITIONS = HStoreConf.singleton().client.benchmark_param_0;
	public static final int NUM_PER_BATCH = 10;
	public static final int  BATCH_SIZE = 1000;
	public static final int  MAX_ARRAY_SIZE = 10000;
	
	public static final int MAX_LOAD_THREADS = 1; //currently a bug that breaks for load_threads > 1. has to do with dimaccount
    public static final int MAX_SQL_BATCH = 50;
    public static final int LATENCY_THRESHOLD = 1000;
    
    public static final String FILE_DIR = "tpcdidata/Batch1/";
    public static final int STREAMINGESTOR_PORT = 18000;
  public static final String STREAMINGESTOR_HOST = "localhost";
    
    public static final String DIMTRADE_TABLE = "DIMTRADE";
    public static final String TRADETXT_TABLE = "TRADETXT";
    public static final String TRADETYPE_TABLE = "TRADETYPE";
    public static final String STATUSTYPE_TABLE = "STATUSTYPE";
    public static final String DIMSECURITY_TABLE = "DIMSECURITY";
    public static final String DIMCUSTOMER_TABLE = "DIMCUSTOMER";
    public static final String DIMCOMPANY_TABLE = "DIMCOMPANY";
    public static final String DIMACCOUNT_TABLE = "DIMACCOUNT";
    public static final String DIMBROKER_TABLE = "DIMBROKER";
    public static final String DIMESSAGES_TABLE = "DIMESSAGES";
    public static final String DIMDATE_TABLE = "DIMDATE";
    public static final String DIMTIME_TABLE = "DIMTIME";
    
    //public static final String DIMTRADE_FILE = "DimTrade";
    public static final String TRADETXT_FILE = "Trade.txt";
    public static final String TRADETYPE_FILE = "TradeType.txt";
    public static final String STATUSTYPE_FILE = "StatusType.txt";
    public static final String DIMSECURITY_FILE = "FINWIRE";
    public static final String DIMCUSTOMER_FILE = "CustomerMgmt_debug.txt";
    public static final String DIMCOMPANY_FILE = "FINWIRE";
    public static final String DIMACCOUNT_FILE = "CustomerMgmt_debug.txt";
    public static final String DIMBROKER_FILE = "HR.csv";
    //public static final String DIMESSAGES_FILE = "DiMessages";
    public static final String DIMDATE_FILE = "Date.txt";
    public static final String DIMTIME_FILE = "Time.txt";
    
    
    public static final String FINWIRE = "FINWIRE";
    
    public static final String LOADFILES[][] = {
    	//{DIMTRADE_TABLE, DIMTRADE_FILE},
    	//{TRADETXT_TABLE, TRADETXT_FILE},
    	{TRADETYPE_TABLE, TRADETYPE_FILE},
    	{STATUSTYPE_TABLE, STATUSTYPE_FILE},
    	{DIMCOMPANY_TABLE, DIMCOMPANY_FILE},
    	{DIMSECURITY_TABLE, DIMSECURITY_FILE},
    	{DIMDATE_TABLE, DIMDATE_FILE},
    	{DIMTIME_TABLE, DIMTIME_FILE},
    	{DIMBROKER_TABLE, DIMBROKER_FILE},
    	{DIMCUSTOMER_TABLE, DIMCUSTOMER_FILE},
    	{DIMACCOUNT_TABLE, DIMACCOUNT_FILE},
    	//{DIMESSAGES_TABLE, DIMESSAGES_FILE},
    };
    
    public static final int PROC_SUCCESSFUL = 1;
    
    public static final String SAMPLETUPLES[] = {
    "0|2012-07-07 00:02:08|CMPT|TMB|0|AAAAAAAAAAAADMO|2939|9.57|2|12646|10.02|58.95|27.31|1611.19",
    "1|2012-07-07 00:11:02|CMPT|TMB|0|AAAAAAAAAAAADZK|9919|1.54|34|3913|1.52|19.21|30.74|3504.75",
    "2|2012-07-08 12:11:23|CMPT|TLB|0|AAAAAAAAAAAACOZ|5525|9.12|36|7935|9.35|143.51|37.20|20526.18",
    "3|2012-09-15 11:38:32|CMPT|TLB|1|AAAAAAAAAAAACQV|8748|9.95|40|16118|9.84|270.87|34.88|5025.70",
    "4|2012-07-07 00:16:12|CMPT|TMB|0|AAAAAAAAAAAADHY|9455|8.01|19|7341|7.65|124.88|7.66|19114.01",
    "5|2012-07-07 00:19:31|CMPT|TMB|0|AAAAAAAAAAAAAES|7137|5.00|28|13683|4.96|152.48|118.26|4602.46",
    "6|2012-07-21 15:05:09|CMPT|TLB|0|AAAAAAAAAAAACTS|1320|6.93|3|7253|6.61|40.13|29.47|973.84",
    "7|2012-07-07 00:28:07|CMPT|TMB|0|AAAAAAAAAAAAARK|3669|4.19|46|7092|4.02|70.63|39.65|5703.20",
    "8|2012-07-07 00:30:37|CMPT|TMB|1|AAAAAAAAAAAAEHX|4919|4.84|28|23087|4.60|12.42|66.26|2014.95",
    "9|2012-10-03 06:36:11|CMPT|TLB|0|AAAAAAAAAAAACPF|3046|6.85|26|22828|6.98|52.45|53.23|5368.66",
    "10|2012-08-16 21:58:49|CMPT|TLB|0|AAAAAAAAAAAABNO|7710|2.71|32|11769|2.77|2.97|33.18|3130.69",
    "11|2012-08-03 04:43:51|CMPT|TLB|1|AAAAAAAAAAAAABP|946|7.47|6|8818|7.48|23.59|18.17|1290.88",
    "12|2012-07-07 00:54:25|CMPT|TMB|0|AAAAAAAAAAAAAXK|4882|7.40|56|16463|7.12|165.46|35.27|12597.14",
    "13|2012-07-09 00:17:10|CMPT|TLB|0|AAAAAAAAAAAAADW|2355|7.72|24|1843|7.77|84.69|20.63|2146.08",
    "14|2012-07-11 10:31:30|CMPT|TLB|0|AAAAAAAAAAAAEIM|8799|6.23|8|4493|6.48|46.28|94.84|14370.30"};
}
