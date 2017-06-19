/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.jdbc;

import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.voltdb.CatalogContext;
import org.voltdb.ServerThread;
import org.voltdb.benchmark.tpcc.TPCCClient;
import org.voltdb.client.ArbitraryDurationProc;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.jdbc.Driver;
import org.voltdb.jdbc.JDBC4Connection;
import org.voltdb.jdbc.JDBC4DatabaseMetaData;
import org.voltdb.jdbc.SQLError;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.conf.HStoreConf;

/**
 * A class to test that the JDBC Driver functions as expected. Tests various JDBC components. 
 * Modified from VoltDB to work with HStore.
 */
public class TestJDBCDriver {
    static String testjar;
    static ServerThread server;
    static Connection conn;
    static Connection myconn;
    static VoltProjectBuilder pb;
    static HStoreConf conf;
    static CatalogContext test;
    
    @BeforeClass
    public static void setUp() throws Exception {
    	conf = HStoreConf.singleton(true);
        
        pb = new VoltProjectBuilder("jdbcQueries");
        pb.addProcedures(org.voltdb.compiler.procedures.TPCCTestProc.class);
        pb.addSchema(TPCCClient.class.getResource("tpcc-ddl.sql"));
        pb.addStmtProcedure("InsertOrders", "INSERT INTO ORDERS VALUES (?, ?, ?, ?, ?, ?, ?, ?);", "ORDERS.O_W_ID: 2");
        pb.addStmtProcedure("InsertWarehouse", "INSERT INTO WAREHOUSE VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);", "WAREHOUSE.W_ID: 0");
        pb.addStmtProcedure("SelectOrders", "SELECT * FROM ORDERS;");
        pb.addProcedures(ArbitraryDurationProc.class);
       
        testjar = "/tmp/jdbcdrivertest.jar";
        boolean success = pb.compile(testjar);
        assert(success);
     
        startServer();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        stopServer();
        File f = new File(testjar);
        f.delete();
    }

    private static void startServer() throws ClassNotFoundException, SQLException {
    	CatalogContext catalogContext = CatalogUtil.loadCatalogContextFromJar(new File(testjar)) ;
        HStoreConf hstore_conf = HStoreConf.singleton(HStoreConf.isInitialized()==false);
        Map<String, String> confParams = new HashMap<String, String>();
        hstore_conf.loadFromArgs(confParams);
        server = new ServerThread(catalogContext, hstore_conf, 0);
        
        server.start();
        server.waitForInitialization();

        Class.forName("org.voltdb.jdbc.Driver");

        conn = DriverManager.getConnection("jdbc:voltdb://localhost:21212");  
        myconn = null;
    }

    private static Connection getJdbcConnection(String url, Properties props) throws Exception
    {
        Class.forName("org.voltdb.jdbc.Driver");
        return DriverManager.getConnection(url, props);
    }

    private static void stopServer() throws SQLException {
        if (conn != null) {
            conn.close();
            conn = null;
        }
        if (myconn != null) {
            myconn.close();
            myconn = null;
        }
        if (server != null) {
            try { server.shutdown(); } catch (InterruptedException e) { /*empty*/ }
            server = null;
        }
    }

    @Test
    public void testURLParsing() throws Exception
    {
        String url = "jdbc:voltdb://server1:21212,server2?prop1=true&prop2=false";
        String[] servers = Driver.getServersFromURL(url);
        assertEquals("server1:21212", servers[0]);
        assertEquals("server2", servers[1]);
        Map<String, String> props = Driver.getPropsFromURL(url);
        assertEquals(2, props.size());
        assertEquals("true", props.get("prop1"));
        assertEquals("false", props.get("prop2"));
    }

    @Test
    public void testTableTypes() throws SQLException {
        ResultSet types = conn.getMetaData().getTableTypes();
        int count = 0;
        List<String> typeList = Arrays.asList(JDBC4DatabaseMetaData.tableTypes);
        while (types.next()) {
            assertTrue(typeList.contains(types.getString("TABLE_TYPE")));
            count++;
        }
        assertEquals(count, typeList.size());
    }

    /**
     * Retrieve table of the given types and check if the count matches the
     * expected values
     *
     * @param types The table type
     * @param expected Expected total count
     * @throws SQLException
     */
    private void tableTest(String[] types, String pattern, int expected) throws SQLException {  
        ResultSet tables = conn.getMetaData().getTables(testjar, "blah", pattern,
                                                types);
        int count = 0;
        List<String> typeList = Arrays.asList(JDBC4DatabaseMetaData.tableTypes);
        if (types != null) {
            typeList = Arrays.asList(types);
        }
 
        while (tables.next()) {
            assertFalse(tables.getString("TABLE_NAME").isEmpty());
            assertTrue(typeList.contains(tables.getString("TABLE_TYPE")));
            count++;
        }
        assertEquals(expected, count);
    }

    @Test
    public void testAllTables() throws SQLException {
        // TPCC has 10 tables
        tableTest(null, "%", 10); //was 13
    }

    @Test
    public void testFilterTableByType() throws SQLException {
        for (String type : JDBC4DatabaseMetaData.tableTypes) {
            int expected = 0;
            // TPCC has 10 tables and no views
            if (type.equals("TABLE")) {
                expected = 10;
            }
            tableTest(new String[] {type}, "%", expected);
        }
    }

    @Test
    public void testFilterTableByName() throws SQLException {
        // schema has 1 "ORDERS" tables
        tableTest(null, "ORDERS", 1);
         // schema has 1 "ORDER_" table
        tableTest(null, "ORDER_", 1);
         // schema has 2 tables that start with "O"
        tableTest(null, "O%", 2);
         // schema has 5 tables with names containing "ST"
        tableTest(null, "%ST%", 5);
        // schema has 10 tables
        tableTest(null, "", 10); 
        // schema has 10 tables, but won't match the types array
        tableTest(new String[] {""}, "", 0);
    }


    @Test
    public void testFilterTableByNameNoMatch() throws SQLException {
        // No matches
        tableTest(null, "%xyzzy", 0);
        tableTest(null, "_", 0);
        tableTest(null, "gobbly_gook", 0);
        tableTest(null, "noname", 0);
    }

    /**
     * Retrieve columns of a given table and check if the count is expected.
     *
     * @param table Table name
     * @param column Column name or null to get all
     * @param expected Expected number of columns
     * @throws SQLException
     */
    private void tableColumnTest(String table, String column, int expected)
    throws SQLException {
        ResultSet columns = conn.getMetaData().getColumns("blah", "blah",
                                                          table, column);
        int count = 0;
        while (columns.next()) {
            assertFalse(columns.getString("COLUMN_NAME").isEmpty());
            count++;
        }
        assertEquals(expected, count);
    }
 
    @Test
    public void testAllColumns() throws SQLException {
        tableColumnTest("WAREHOUSE", null, 9);
        tableColumnTest("WAREHOUSE", "%", 9);
    }


    @Test
    public void testFilterColumnByName() throws SQLException {
        tableColumnTest("WAREHOUSE", "W_ID", 1);
    }

    @Test
    public void testFilterColumnByWildcard() throws SQLException {
        tableColumnTest("CUSTOMER%", null, 26);
        tableColumnTest("CUSTOMER%", "", 26);
        tableColumnTest("CUSTOMER%", "%MIDDLE", 1);
        tableColumnTest("CUSTOMER", "____", 1);
        tableColumnTest("%", "%ID", 32);//was 13
        tableColumnTest(null, "%ID", 32); ///// was 13
        tableColumnTest(null, "", 97); //was 73
    }

    /**
     * Retrieve index info of a table and check the count.
     *
     * @param table
     *            Table name
     * @param unique
     *            Unique or not
     * @param expected
     *            Expected count
     * @throws SQLException
     */
    private void indexInfoTest(String table, boolean unique, int expected)
    throws SQLException {
        ResultSet indexes = conn.getMetaData().getIndexInfo("blah", "blah",
                                                            table, unique,
                                                            false);
        int count = 0;
        while (indexes.next()) {
            assertEquals(table, indexes.getString("TABLE_NAME"));
            if (unique) {
                assertEquals(false, indexes.getBoolean("NON_UNIQUE"));
            }
            count++;
        }
        assertEquals(expected, count);
    }

    @Test
    public void testAllIndexes() throws SQLException {
        indexInfoTest("ORDERS", false, 3);
    }
    
    @Test
    public void testFilterIndexByUnique() throws SQLException {
        indexInfoTest("ORDERS", true, 2); // was 3
    }

    @Test
    public void testAllPrimaryKeys() throws SQLException {
        ResultSet keys = conn.getMetaData().getPrimaryKeys("blah", "blah",
                                                           "ORDERS");
        int count = 0;
        while (keys.next()) {
            assertEquals("ORDERS", keys.getString("TABLE_NAME"));
            count++;
        }
        assertEquals(1, count);
    }

    @Test
    public void testAllProcedures() throws SQLException {
        ResultSet procedures =
                conn.getMetaData().getProcedures("blah", "blah", "%");
        int count = 0;
        List<String> names = Arrays.asList(new String[] {"ArbitraryDurationProc", 
        		"TPCCTestProc", "InsertOrders", "InsertWarehouse", "SelectOrders"});
        while (procedures.next()) {
            String procedure = procedures.getString("PROCEDURE_NAME");
            if (procedure.contains(".")) {
                // auto-generated CRUD
            } else {
                assertTrue(names.contains(procedure));
            }
            count++;
        }
        System.out.println("Procedure count is: " + count);
        assertEquals(5, count);
    }

    @Test
    public void testFilterProcedureByName() {
        try {
            conn.getMetaData().getProcedures("blah", "blah", "InsertA");
        } catch (SQLException e) {
            return;
        }
        fail("Should fail, we don't support procedure filtering by name");
    }

    /**
     * Retrieve columns of a given procedure and check if the count is expected.
     *
     * @param procedure Procedure name
     * @param column Column name or null to get all
     * @param expected Expected number of columns
     * @throws SQLException
     */
    private void procedureColumnTest(String procedure, String column,
                                     int expected)
    throws SQLException {
        ResultSet columns =
                conn.getMetaData().getProcedureColumns("blah", "blah",
                                                       procedure, column);
        int count = 0;
        while (columns.next()) {
        	
            assertEquals(procedure, columns.getString("PROCEDURE_NAME"));
            assertFalse(columns.getString("COLUMN_NAME").isEmpty());
            count++;
        }
        assertEquals(expected, count);
    }

    @Test
    public void testAllProcedureColumns() throws SQLException {
        procedureColumnTest("InsertOrders", null, 8);
        procedureColumnTest("InsertOrders", "%", 8);
    }

    @Test
    public void testFilterProcedureColumnsByName() throws SQLException {
        ResultSet procedures =
                conn.getMetaData().getProcedures("blah", "blah", "%");
        int count = 0;
        while (procedures.next()) {
            String proc = procedures.getString("PROCEDURE_NAME");
            // Skip CRUD
            if (proc.contains(".")) {
                continue;
            }

            ResultSet columns = conn.getMetaData().getProcedureColumns("b", "b",
                                                                       proc,
                                                                       null);
            while (columns.next()) {
                String column = columns.getString("COLUMN_NAME");
                procedureColumnTest(proc, column, 1);
                count++;
            }
        }
        assertEquals(19, count);
    }

    @Test
    public void testBadProcedureName() throws SQLException {
        CallableStatement cs = conn.prepareCall("{call Oopsy(?)}");
        cs.setLong(1, 99);
        try {
            cs.execute();
        } catch (SQLException e) {
            assertEquals(e.getSQLState(), SQLError.GENERAL_ERROR);
        }
    }

    @Test
    public void testDoubleInsert() throws SQLException {
        CallableStatement cs = conn.prepareCall("{call InsertOrders(?, ?, ?, ?, ?, ?, ?, ?)}");
        cs.setLong(1, 1L);
        cs.setLong(2, 1L);
        cs.setLong(3, 1L);
        cs.setLong(4, 1L);
        cs.setLong(5, 1L);
        cs.setLong(6, 1L);
        cs.setLong(7, 1L);
        cs.setLong(8, 1L);
        cs.execute();
        try {
        	 cs.setLong(1, 1L);
             cs.setLong(2, 1L);
             cs.setLong(3, 1L);
             cs.setLong(4, 1L);
             cs.setLong(5, 1L);
             cs.setLong(6, 1L);
             cs.setLong(7, 1L);
             cs.setLong(8, 1L);
            cs.execute();
        } catch (SQLException e) {
            // Since it's a GENERAL_ERROR we need to look for a string by pattern.
            assertEquals(e.getSQLState(), SQLError.GENERAL_ERROR);
            assertTrue(e.getMessage().contains("violation of constraint"));
        }
    }

    public void testVersionMetadata() throws SQLException {
        int major = conn.getMetaData().getDatabaseMajorVersion();
        int minor = conn.getMetaData().getDatabaseMinorVersion();
        assertTrue(major >= 2);
        assertTrue(minor >= 0);
    }

    @Test
    public void testLostConnection() throws SQLException, ClassNotFoundException {
        // Break the current connection and try to execute a procedure call.
        CallableStatement cs = conn.prepareCall("{call Oopsy(?)}");
        stopServer();
        cs.setLong(1, 99);
        try {
            cs.execute();
        } catch (SQLException e) {
            assertEquals(e.getSQLState(), SQLError.CONNECTION_FAILURE);
        }
        // Restore a working connection for any remaining tests
        startServer();
    }

    @Test
    public void testSetMaxRows() throws SQLException    {
        // Add 10 rows
        PreparedStatement ins = conn.prepareCall("{call InsertOrders(?, ?, ?, ?, ?, ?, ?, ?)}");
        for (int i = 0; i < 10; i++) {
       	 ins.setLong(1, i);
         ins.setLong(2, i);
         ins.setLong(3, i);
         ins.setLong(4, i);
         ins.setLong(5, i);
         ins.setLong(6, i);
         ins.setLong(7, i);
         ins.setLong(8, i);
            ins.execute();
        }

        // check for our 10 rows
        PreparedStatement cs = conn.prepareCall("{call SelectOrders}");
        ResultSet rs = cs.executeQuery();
        int count = 0;
        while (rs.next()) {
            count++;
        }
        assertEquals(10, count);

        // constrain to 5 and try again.
        cs.setMaxRows(5);
        assertEquals(5, cs.getMaxRows());
        rs = cs.executeQuery();
        count = 0;
        while (rs.next()) {
            count++;
        }
        assertEquals(5, count);

        // Verify 0 gets us everything again
        cs.setMaxRows(0);
        assertEquals(0, cs.getMaxRows());
        rs = cs.executeQuery();
        count = 0;
        while (rs.next()) {
            count++;
        }
        assertEquals(10, count);

        // Go for spot-on
        cs.setMaxRows(10);
        assertEquals(10, cs.getMaxRows());
        rs = cs.executeQuery();
        count = 0;
        while (rs.next()) {
            count++;
        }
        assertEquals(10, count);
    }

    private void checkSafeMode(Connection myconn)
    {
        boolean threw = false;
        try {
            myconn.commit();
        }
        catch (SQLException bleh) {
            threw = true;
        }
        assertTrue(threw);
        threw = false;
        // autocommit true should never throw
        try {
            myconn.setAutoCommit(true);
        }
        catch (SQLException bleh) {
            threw = true;
        }
        assertFalse(threw);
        threw = false;
        try {
            myconn.setAutoCommit(false);
        }
        catch (SQLException bleh) {
            threw = true;
        }
        assertTrue(threw);
        threw = false;
        try {
            myconn.rollback();
        }
        catch (SQLException bleh) {
            threw = true;
        }
        assertTrue(threw);
    }

    private void checkCarlosDanger(Connection myconn)
    {
        boolean threw = false;
        try {
            myconn.commit();
        }
        catch (SQLException bleh) {
            threw = true;
        }
        assertFalse(threw);
        threw = false;
        // autocommit true should never throw
        try {
            myconn.setAutoCommit(true);
        }
        catch (SQLException bleh) {
            threw = true;
        }
        assertFalse(threw);
        threw = false;
        try {
            myconn.setAutoCommit(false);
        }
        catch (SQLException bleh) {
            threw = true;
        }
        assertFalse(threw);
        threw = false;
        try {
            myconn.rollback();
        }
        catch (SQLException bleh) {
            threw = true;
        }
        assertFalse(threw);
    }

    @Test
    public void testSafetyOffThroughProperties() throws Exception
    {
        Properties props = new Properties();
        // Check default behavior
        myconn = getJdbcConnection("jdbc:voltdb://localhost:21212", props);
        checkSafeMode(myconn);
        myconn.close();

        // Check commit and setAutoCommit
        props.setProperty(JDBC4Connection.COMMIT_THROW_EXCEPTION, "true");
        props.setProperty(JDBC4Connection.ROLLBACK_THROW_EXCEPTION, "true");
        myconn = getJdbcConnection("jdbc:voltdb://localhost:21212", props);
        checkSafeMode(myconn);
        myconn.close();

        props.setProperty(JDBC4Connection.COMMIT_THROW_EXCEPTION, "false");
        props.setProperty(JDBC4Connection.ROLLBACK_THROW_EXCEPTION, "false");
        myconn = getJdbcConnection("jdbc:voltdb://localhost:21212", props);
        checkCarlosDanger(myconn);
        myconn.close();
    }

    @Test
    public void testSafetyOffThroughURL() throws Exception
    {
        Properties props = new Properties();
        // Check default behavior
        myconn = getJdbcConnection("jdbc:voltdb://localhost:21212", props);
        checkSafeMode(myconn);
        myconn.close();

        // Check commit and setAutoCommit
        myconn = getJdbcConnection("jdbc:voltdb://localhost:21212?" +
                JDBC4Connection.COMMIT_THROW_EXCEPTION + "=true" + "&" +
                JDBC4Connection.ROLLBACK_THROW_EXCEPTION + "=true", props);
        checkSafeMode(myconn);
        myconn.close();

        myconn = getJdbcConnection("jdbc:voltdb://localhost:21212?" +
                JDBC4Connection.COMMIT_THROW_EXCEPTION + "=false" + "&" +
                JDBC4Connection.ROLLBACK_THROW_EXCEPTION + "=false", props);
        checkCarlosDanger(myconn);
        myconn.close();
    }

    @Test
    public void testSafetyOffThroughSystemProp() throws Exception {
        String tmppath = "/tmp/";
        String propfile = tmppath + "/properties";
        // start clean
        File tmp = new File(propfile);
        if (tmp.exists()) {
            tmp.delete();
        }
        try {
            Properties props = new Properties();
            props.setProperty(JDBC4Connection.COMMIT_THROW_EXCEPTION, "false");
            props.setProperty(JDBC4Connection.ROLLBACK_THROW_EXCEPTION, "false");
            FileOutputStream out = null;
            try {
                out = new FileOutputStream(propfile);
                props.store(out, "");
            } catch (FileNotFoundException e) {
                fail();
            } catch (IOException e) {
                fail();
            }        finally {
                if (out != null) {
                    try {
                        out.close();
                    } catch (IOException e) { }
                }
            }

            System.setProperty(Driver.JDBC_PROP_FILE_PROP, propfile);
            props = new Properties();
            myconn = getJdbcConnection("jdbc:voltdb://localhost:21212", props);
            checkCarlosDanger(myconn);
            myconn.close();
        }
        finally {
            // end clean
            if (tmp.exists()) {
                tmp.delete();
            }
        }
    }
}
