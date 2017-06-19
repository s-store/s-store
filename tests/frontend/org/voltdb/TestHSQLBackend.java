/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.UnknownHostException;
import java.util.Calendar;

import junit.framework.TestCase;

import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ProcCallException;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.catalog.Statement;
import org.voltdb.exceptions.ConstraintFailureException;
import org.voltdb.types.TimestampType;

import com.google.gdata.data.dublincore.Date;
//import com.sun.corba.se.spi.orbutil.fsm.Guard.Result;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.conf.HStoreConf;

public class TestHSQLBackend extends TestCase {

	/**
	 * JUnit test to assure functionality of constructor and shutdown
	 */
    public void testSetUpShutDown(){
        HsqlBackend test = new HsqlBackend(1);
        test.shutdown();
    }
    

    /**
     * JUnit test to assure create table functions
     */
    public void testDDLCreateTable(){
        HsqlBackend test = new HsqlBackend(1);
        test.runDDL("CREATE TABLE test (col1 varchar(10), col2 varchar(10));");
        test.shutdown();
    }
   
    /**
     * JUnit test to assure that table can be created w/ varchar columns and DML 
     * insert query is successfully executed.
     */
    public void testDMLInsertVarChars(){
        HsqlBackend test = new HsqlBackend(1);
        
        test.runDDL("CREATE TABLE testDatabase (col1 varchar(10), col2 varchar(10));");
        test.runDML("INSERT INTO testDatabase VALUES ('x1', 'x2');");
        VoltTable result = test.runDML("SELECT * FROM testDatabase");
        
        assertTrue(result.getRowCount() == 1);
        result.advanceRow();
        assertTrue(result.get(0).equals("x1"));
        assertTrue(result.get(1).equals("x2"));
      
        // assert that values greater than max varchar length cannot be inserted
        boolean thrown = false;
        try{
        	test.runDML("INSERT INTO testDatabase VALUES ('this is too long', 'this is also too long');");
        }
        catch(ExpectedProcedureException v){
        	thrown = true;
        }
        assertTrue(thrown);
       
        test.runDML("INSERT INTO testDatabase VALUES ('next val', 'fakeval');");
        result = test.runDML("SELECT * FROM testDatabase");
        assertTrue(result.getRowCount() == 2);
        
        result.advanceRow();
        assertTrue(result.get(0).equals("x1"));
        assertTrue(result.get(1).equals("x2"));
        
        result.advanceRow();
        assertTrue(result.get(0).equals("next val"));
        assertTrue(result.get(1).equals("fakeval"));  
        
        test.shutdown();
    }
    
    /**
     * JUnit test to create a table w/ INTEGER columns and test DML insertion and specifc row selection
     */
    public void testDMLInsertInteger(){
        HsqlBackend test = new HsqlBackend(1);
        
        test.runDDL("CREATE TABLE testDatabase (col1 INTEGER, col2 INTEGER, CONSTRAINT pk PRIMARY KEY (col1));");
        test.runDML("INSERT INTO testDatabase VALUES (1, 2);");
        VoltTable result = test.runDML("SELECT * FROM testDatabase");
        assertTrue(result.getRowCount() == 1);
        
        result.advanceRow();
        assertTrue((int) result.get(0) == 1);
        assertTrue((int) result.get(1) == 2);
        
        // assert that there is a key constraint violation
        boolean thrown = false;
        try {
        	test.runDML("INSERT INTO testDatabase VALUES (1, 3);");
        } catch (ConstraintFailureException ex){
        	thrown = true;
        }
        assertTrue(thrown);
        
        test.runDML("INSERT INTO testDatabase VALUES (3, 4);");
        result = test.runDML("SELECT * FROM testDatabase");
        assertTrue(result.getRowCount() == 2);
        
        result.advanceToRow(0);
        assertTrue((int) result.get(0) == 1);
        assertTrue((int) result.get(1) == 2);
        
        result.advanceToRow(1);
        assertTrue((int) result.get(0) == 3);
        assertTrue((int) result.get(1) == 4);
        
        result = test.runDML("SELECT * FROM testDatabase WHERE col1 = 3");
        assertTrue(result.getRowCount() == 1);
        
        result.advanceRow();
        assertTrue((int) result.get(0) == 3);
        assertTrue((int) result.get(1) == 4);

        test.shutdown();
    }
   
    /**
     * JUnit test to create a table w/ TINYINT columns and test DML insertion and deletionss
     */
    public void testDMLInsertTinyInteger(){
        HsqlBackend test = new HsqlBackend(1);
        byte first = 1;
        byte second  = 2;
        byte third = 0;
        byte fourth = 4;
        
        test.runDDL("CREATE TABLE testDatabase (col1 TINYINT, col2 TINYINT);");
        test.runDML("INSERT INTO testDatabase VALUES (1, 2);");
        test.runDML("INSERT INTO testDatabase VALUES (0, 4);");
        VoltTable result = test.runDML("SELECT * FROM testDatabase");
        assertTrue(result.getRowCount() == 2);

        result.advanceRow();
        assertTrue((byte) result.get(0) == first);
        assertTrue((byte) result.get(1) == second);
        
        result.advanceRow();
        assertTrue((byte) result.get(0) == third);
        assertTrue((byte) result.get(1) == fourth);
        
        test.runDML("DELETE FROM testDatabase WHERE col1 = 1");
        result = test.runDML("SELECT * FROM testDatabase WHERE col1 = 0");
        assertTrue(result.getRowCount() == 1);
        
        result.advanceRow();
        assertTrue((byte) result.get(0) == third);
        assertTrue((byte) result.get(1) == fourth);
        
        test.runDML("DELETE FROM testDatabase WHERE col2 = 4");
        result = test.runDML("SELECT * FROM testDatabase");
        assertTrue(result.getRowCount() == 0);
        
        test.shutdown();
    }
    
    /**
     * JUnit test to create a table w/ SMALLINT columns and test DML insertion and deletion
     */
    public void testDMLInsertSmallInteger(){
        HsqlBackend test = new HsqlBackend(1);
        byte first = 1;
        byte second  = 2;
        
        test.runDDL("CREATE TABLE testDatabase (col1 TINYINT, col2 TINYINT);");
        test.runDML("INSERT INTO testDatabase VALUES (1, 2);");
        VoltTable result = test.runDML("SELECT * FROM testDatabase");
        assertTrue(result.getRowCount() == 1);

        result.advanceRow();
        assertTrue((byte) result.get(0) == first);
        assertTrue((byte) result.get(1) == second);
        
        test.runDML("INSERT INTO testDatabase VALUES (1, 2);");
        test.runDML("DELETE FROM testDatabase WHERE col1 = 1;");
        result = test.runDML("SELECT * FROM testDatabase");
        assertTrue(result.getRowCount() == 0);
        
        test.shutdown();
    }
   
    /**
     * JUnit test to create a table w/ BIGINT columns and test DML insertion
     */
    public void testDMLInsertBigInteger(){
        HsqlBackend test = new HsqlBackend(1);
        long first = 100000000000L;
        long second  = 200000000000L;
        
        test.runDDL("CREATE TABLE testDatabase (col1 BIGINT, col2 BIGINT);");
        test.runDML("INSERT INTO testDatabase VALUES (100000000000, 200000000000);");
        VoltTable result = test.runDML("SELECT * FROM testDatabase");
        assertTrue(result.getRowCount() == 1);

        result.advanceRow();
        assertTrue((long) result.get(0) == first);
        assertTrue((long) result.get(1) == second);

        test.runDML("INSERT INTO testDatabase VALUES (300000000000, 400000000000);");
        test.runDML("DELETE FROM testDatabase");
        result = test.runDML("SELECT * FROM testDatabase");
        assertTrue(result.getRowCount() == 0);
        
        test.shutdown();
    }
    
    /**
     * JUnit test to create a table w/ BIGINT columns and test DML insertion
     */
    public void testDMLInsertDecimal(){
        HsqlBackend test = new HsqlBackend(1);
        BigDecimal first = new BigDecimal(1.1).setScale(12, BigDecimal.ROUND_DOWN);
        BigDecimal second = new BigDecimal(2.2).setScale(12, BigDecimal.ROUND_DOWN);
        
        test.runDDL("CREATE TABLE testDatabase (col1 DECIMAL, col2 DECIMAL);");
        test.runDML("INSERT INTO testDatabase VALUES (1.1, 2.2);");
        VoltTable result = test.runDML("SELECT * FROM testDatabase");
        assertTrue(result.getRowCount() == 1);

        result.advanceRow();
        assertTrue(first.compareTo((BigDecimal) result.get(0)) == 0);
        assertTrue(second.compareTo((BigDecimal) result.get(1)) == 0);

        test.shutdown();
    }
    
    /**
     * JUnit test to create a table w/ FLOAT columns and test DML insertion
     */
    public void testDMLInsertFloatAsDouble(){
        HsqlBackend test = new HsqlBackend(1);
        double first = 1.1;
        double second = 2.2;
        
        test.runDDL("CREATE TABLE testDatabase (col1 FLOAT, col2 FLOAT);");
        test.runDML("INSERT INTO testDatabase VALUES (1.1, 2.2);");
        VoltTable result = test.runDML("SELECT * FROM testDatabase");
        assertTrue(result.getRowCount() == 1);

        result.advanceRow();
        assertTrue(result.getDouble(0) == first);
        assertTrue(result.getDouble(1) == second);
        test.shutdown();
    }
    
    /**
     * JUnit test to create a table w/ FLOAT columns and test DML insertion
     */
    public void testDMLInsertFloatAsFloat(){
        HsqlBackend test = new HsqlBackend(1);
        float first = 1.1f;
        float second = 2.2f;
   
        test.runDDL("CREATE TABLE testDatabase (col1 FLOAT, col2 FLOAT);");
        test.runDML("INSERT INTO testDatabase VALUES (1.1, 2.2);");
        VoltTable result = test.runDML("SELECT * FROM testDatabase");
        assertTrue(result.getRowCount() == 1);

        result.advanceRow();
        Double val = result.getDouble(0);
        Double val2 = result.getDouble(1);
        assertTrue(val.floatValue()  == first);
        assertTrue(val2.floatValue() == second);
        test.shutdown();
    }
    
    /**
     * JUnit test to create a table w/ String and Integer columns and test DML insertion with params
     */
    public void testDMLParamsInsertStringAndInteger(){
        HsqlBackend test = new HsqlBackend(1);
        test.runDDL("CREATE TABLE testDatabase (col1 varchar(10), col2 INTEGER );");
        test.runSQLWithSubString("INSERT INTO testDatabase VALUES (?, ?);", "x1", 4);
        
        boolean thrown = false;
        try {
			test.runSQLWithSubString("INSERT INTO testDatabase VALUES (?, ?);", 5, "x1");
		} catch (ExpectedProcedureException e2) {
		    thrown = true;
		}
        assertTrue(thrown);
        
        VoltTable result = test.runDML("SELECT * FROM testDatabase");
        assertTrue(result.getRowCount() == 1);
        
        result.advanceRow();
        assertTrue(result.get(0).equals("x1"));
        assertTrue((int)result.get(1)==4);
        
        
        test.runSQLWithSubString("INSERT INTO testDatabase VALUES (?, ?);", "x1", 4, 5);
        assertTrue(thrown);
        
        thrown = false;
        test.runSQLWithSubString("INSERT INTO testDatabase VALUES (?, ?);", "x1");
        assertTrue(thrown);
        
        test.shutdown();
    }
    
    /**
     * JUnit test to create a table w/ TIMESTAMP columns and test DML insertion with params
     */
    public void testDMLParamsTimestamp() throws IOException{
        HsqlBackend test = new HsqlBackend(1);
        Long timestamp1 = Calendar.getInstance().getTimeInMillis();
        Long timestamp2 = Calendar.getInstance().getTimeInMillis();
       
        test.runDDL("CREATE TABLE testDatabase (col1 TIMESTAMP, col2 TIMESTAMP);");
        test.runSQLWithSubString("INSERT INTO testDatabase VALUES (?, ?);", timestamp1, timestamp2);
        VoltTable result = test.runDML("SELECT * FROM testDatabase");
        assertTrue(result.getRowCount() == 1);
        result.advanceRow();
        assertTrue(result.getTimestampAsLong(0) == (timestamp1*1000));
        assertTrue(result.getTimestampAsLong(1) == timestamp2*1000);
        test.shutdown();
    }
   
   /**
    * TODO: Test insert w/ params for tinyint
    */
   
   /**
    * TODO: Test insert w/ params for smallint
    */
   
   /**
    * TODO: Test insert w/ params for bigint
    */
   
   /**
    * TODO: Test insert w/ params for decimal
    */  
   
   /**
    * TODO: Test insert w/ params for float
    */
   
   /**
    * TODO: Test deletion w/ params for tinyint
    */
   
   /**
    * TODO: Test deletion w/ params for smallint
    */
   
   /**
    * TODO: Test deletion w/ params for bigint
    */
   
   /**
    * TODO: Test deletion w/ params for decimal
    */  
   
   /**
    * TODO: Test deletion w/ params for float
    */
   /**
    * TODO: Test update w/ params for tinyint
    */
   
   /**
    * TODO: Test update w/ params for smallint
    */
   
   /**
    * TODO: Test update w/ params for bigint
    */
   
   /**
    * TODO: Test update w/ params for decimal
    */  
   
   /**
    * TODO: Test update w/ params for float
    */
}
