/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *                                                                         *
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
package edu.brown.benchmark.tpcdi.onesp;

import java.util.Random;

import org.voltdb.catalog.Table;

import edu.brown.rand.RandomDistribution.Zipf;

public abstract class TPCDIUtil {

    public static final Random rand = new Random();
    public static Zipf zipf = null;
    public static final double zipf_sigma = 1.001d;

    public static int getPartitionId(Table t, Object row[]) {
    	int part_id = -1;
    
    	return part_id;
    }
    
    public static int isActive() {
        return number(1, 100) < number(86, 100) ? 1 : 0;
    }

    // modified from tpcc.RandomGenerator
    /**
     * @returns a random alphabetic string with length in range [minimum_length,
     *          maximum_length].
     */
    public static String astring(int minimum_length, int maximum_length) {
        return randomString(minimum_length, maximum_length, 'A', 26);
    }

    // taken from tpcc.RandomGenerator
    /**
     * @returns a random numeric string with length in range [minimum_length,
     *          maximum_length].
     */
    public static String nstring(int minimum_length, int maximum_length) {
        return randomString(minimum_length, maximum_length, '0', 10);
    }

    // taken from tpcc.RandomGenerator
    public static String randomString(int minimum_length, int maximum_length, char base, int numCharacters) {
        int length = number(minimum_length, maximum_length).intValue();
        byte baseByte = (byte) base;
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; ++i) {
            bytes[i] = (byte) (baseByte + number(0, numCharacters - 1));
        }
        return new String(bytes);
    }

    // taken from tpcc.RandomGenerator
    public static Long number(long minimum, long maximum) {
        assert minimum <= maximum;
        long value = Math.abs(rand.nextLong()) % (maximum - minimum + 1) + minimum;
        assert minimum <= value && value <= maximum;
        return value;
    }

    public static String padWithZero(Long n) {
        String meat = n.toString();
        char[] zeros = new char[15 - meat.length()];
        for (int i = 0; i < zeros.length; i++)
            zeros[i] = '0';
        return (new String(zeros) + meat);
    }

    /**
     * Returns sub array of arr, with length in range [min_len, max_len]. Each
     * element in arr appears at most once in sub array.
     */
    public static int[] subArr(int arr[], int min_len, int max_len) {
        assert min_len <= max_len && min_len >= 0;
        int sub_len = number(min_len, max_len).intValue();
        int arr_len = arr.length;

        assert sub_len <= arr_len;

        int sub[] = new int[sub_len];
        for (int i = 0; i < sub_len; i++) {
            int j = number(0, arr_len - 1).intValue();
            sub[i] = arr[j];
            // arr[j] put to tail
            int tmp = arr[j];
            arr[j] = arr[arr_len - 1];
            arr[arr_len - 1] = tmp;

            arr_len--;
        } // FOR

        return sub;
    }

    public static int hashCode(String table, String key) {
    	if(TPCDIConstants.NUM_PARTITIONS == 0)
    		return 0;
    	
    	switch (TPCDIConstants.DATA_CONFIG) {
    	case TPCDIConstants.CONF_NAIVE:
    		return naiveHashCode(table, key);
    	case TPCDIConstants.CONF_ALLSPLIT:
    		return allSplitHashCode(table, key);
    	case TPCDIConstants.CONF_ALLPIPELINE:
    		return allPipelineHashCode(table, key);
    	case TPCDIConstants.CONF_HYBRID:
    		return hybridHashCode(table, key);
    	case TPCDIConstants.CONF_SIMPLESPLIT:
    		return simpleSplitHashCode(table, key);
    	}
    	return (Math.abs(key.hashCode()) % TPCDIConstants.NUM_PARTITIONS);
    }
    
    private static int naiveHashCode(String table, String key){
    	return 0;
    }
    
    private static int allSplitHashCode(String table, String key){
    	switch(table.toUpperCase()) {
    		case TPCDIConstants.DIMTRADE_TABLE:
    		case TPCDIConstants.TRADETXT_TABLE:
    		case TPCDIConstants.DIMSECURITY_TABLE:
    		case TPCDIConstants.DIMCUSTOMER_TABLE:
    		case TPCDIConstants.DIMCOMPANY_TABLE:
    		case TPCDIConstants.DIMACCOUNT_TABLE:
    		case TPCDIConstants.DIMBROKER_TABLE:
    		case TPCDIConstants.DIMESSAGES_TABLE:
    			return (Math.abs(key.hashCode()) % (TPCDIConstants.NUM_PARTITIONS-1)) + 1;
    	}
    		
    	return 0;
    }
    	
    private static int allPipelineHashCode(String table, String key){
		
    	switch(table.toUpperCase()) {
    		case TPCDIConstants.TRADETXT_TABLE:
    		case TPCDIConstants.TRADETYPE_TABLE:
    		case TPCDIConstants.STATUSTYPE_TABLE:
    		case TPCDIConstants.DIMDATE_TABLE:
    		case TPCDIConstants.DIMTIME_TABLE:
    			return ((Math.abs(key.hashCode()) % ((TPCDIConstants.NUM_PARTITIONS-1) / 5 + 1)) * 5 + 1);
    			
    		case TPCDIConstants.DIMSECURITY_TABLE:
    			return ((Math.abs(key.hashCode()) % ((TPCDIConstants.NUM_PARTITIONS-1) / 5 + 1)) * 5 + 2);
    		
    		case TPCDIConstants.DIMCUSTOMER_TABLE:
    		case TPCDIConstants.DIMCOMPANY_TABLE:
    		case TPCDIConstants.DIMACCOUNT_TABLE:
    		case TPCDIConstants.DIMBROKER_TABLE:
    			return ((Math.abs(key.hashCode()) % ((TPCDIConstants.NUM_PARTITIONS-1) / 5 + 1)) * 5 + 3);
    			
    		case TPCDIConstants.DIMESSAGES_TABLE:
    		case TPCDIConstants.DIMTRADE_TABLE:
    			return ((Math.abs(key.hashCode()) % ((TPCDIConstants.NUM_PARTITIONS-1) / 5 + 1)) * 5 + 4);
    	}
    	return 0;
    }
    
    private static int hybridHashCode(String table, String key){
		
    	switch(table.toUpperCase()) {
    		case TPCDIConstants.TRADETXT_TABLE:
    		case TPCDIConstants.TRADETYPE_TABLE:
    		case TPCDIConstants.STATUSTYPE_TABLE:
    		case TPCDIConstants.DIMDATE_TABLE:
    		case TPCDIConstants.DIMTIME_TABLE:
    			return 1;
    			
    		case TPCDIConstants.DIMSECURITY_TABLE:
    			return (Math.abs(key.hashCode()) % (TPCDIConstants.NUM_PARTITIONS-2)) + 2;
    		
    		case TPCDIConstants.DIMCUSTOMER_TABLE:
    		case TPCDIConstants.DIMCOMPANY_TABLE:
    		case TPCDIConstants.DIMACCOUNT_TABLE:
    		case TPCDIConstants.DIMBROKER_TABLE:
    			return (Math.abs(key.hashCode()) % (TPCDIConstants.NUM_PARTITIONS-2)) + 2;
    			
    		case TPCDIConstants.DIMESSAGES_TABLE:
    		case TPCDIConstants.DIMTRADE_TABLE:
    			return (Math.abs(key.hashCode()) % (TPCDIConstants.NUM_PARTITIONS-2)) + 2;
    	}
    	return 0;
    }
    
    private static int simpleSplitHashCode(String table, String key){
    	int halfParts = (TPCDIConstants.NUM_PARTITIONS-1)/2;
		
    	switch(table.toUpperCase()) {
    		case TPCDIConstants.TRADETXT_TABLE:
    		case TPCDIConstants.TRADETYPE_TABLE:
    		case TPCDIConstants.STATUSTYPE_TABLE:
    		case TPCDIConstants.DIMDATE_TABLE:
    		case TPCDIConstants.DIMTIME_TABLE:
    		case TPCDIConstants.DIMSECURITY_TABLE:
    			return (Math.abs(key.hashCode()) % halfParts) + 1;
    		
    		case TPCDIConstants.DIMCUSTOMER_TABLE:
    		case TPCDIConstants.DIMCOMPANY_TABLE:
    		case TPCDIConstants.DIMACCOUNT_TABLE:
    		case TPCDIConstants.DIMBROKER_TABLE:
    		case TPCDIConstants.DIMESSAGES_TABLE:
    		case TPCDIConstants.DIMTRADE_TABLE:
    			return (Math.abs(key.hashCode()) % halfParts) + halfParts + 1;
    	}
    	return 0;
    }

}
