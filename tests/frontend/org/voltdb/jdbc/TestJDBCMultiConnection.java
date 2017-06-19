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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.voltdb.CatalogContext;
import org.voltdb.ServerThread;
import org.voltdb.benchmark.tpcc.TPCCClient;
import org.voltdb.compiler.VoltProjectBuilder;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.conf.HStoreConf;


/**
 * Test class to make sure JDBC can handle multiple connections at a time. 
 * Modified to work with Hstore.
 */
public class TestJDBCMultiConnection {
    static String m_testJar;
    static ServerThread m_server;
    
    // Use multiple connections to make sure reference counting doesn't prevent
    // detection of broken connections.
    static Connection[] m_connections = new Connection[3];
    static VoltProjectBuilder m_projectBuilder;

    @BeforeClass
    public static void setUp() throws Exception {
        m_projectBuilder = new VoltProjectBuilder("jdbcQueries");
        m_projectBuilder.addProcedures(org.voltdb.compiler.procedures.TPCCTestProc.class);
        m_projectBuilder.addSchema(TPCCClient.class.getResource("jdbc-multiconn-test-ddl.sql"));
        
        m_projectBuilder.addPartitionInfo("TT", "A1");
        m_projectBuilder.addPartitionInfo("ORDERS", "A1");
        m_projectBuilder.addStmtProcedure("InsertA", "INSERT INTO TT VALUES(?,?);", "TT.A1: 0");
        m_projectBuilder.addStmtProcedure("SelectB", "SELECT * FROM TT;");

        m_testJar = "/tmp/jdbcmulticonntest.jar";
        boolean success = m_projectBuilder.compile(m_testJar);
        assert(success);        
        
        // Set up server and connections.
        startServer();
        connectClients();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        stopServer();
        File f = new File(m_testJar);
        f.delete();
    }

    private static void startServer()
    {
    	CatalogContext catalogContext = CatalogUtil.loadCatalogContextFromJar(new File(m_testJar)) ;
        HStoreConf hstore_conf = HStoreConf.singleton(HStoreConf.isInitialized()==false);
        Map<String, String> confParams = new HashMap<String, String>();
        hstore_conf.loadFromArgs(confParams);
        m_server = new ServerThread(catalogContext, hstore_conf, 0);
        
        m_server.start();
        m_server.waitForInitialization();
    }

    private static void connectClients()
    {
        try {
            Class.forName("org.voltdb.jdbc.Driver");
            for (int i = 0; i < m_connections.length; ++i) {
                m_connections[i] = DriverManager.getConnection("jdbc:voltdb://localhost:21212");
            }
        } catch(ClassNotFoundException e) {
            System.err.println(e);
            fail();
        } catch(SQLException e) {
            System.err.println(e);
            fail();
        }
    }

    private static void stopServer() throws SQLException {
        for (int i = 0; i < m_connections.length; ++i) {
            if (m_connections[i] != null) {
                m_connections[i].close();
                m_connections[i] = null;
            }
        }
        if (m_server != null) {
            try { m_server.shutdown(); } catch (InterruptedException e) { /*empty*/ }
            m_server = null;
        }
    }

    /*
     * Test that multiple client connections properly handle disconnection.
     */
    @Test
    public void testMultiDisconnect() throws Exception
    {
        class Tester
        {
            List<CallableStatement> m_callableStatements = new ArrayList<CallableStatement>();
            int m_value = 0;

            void makeStatements() throws SQLException
            {
                for (Connection connection : m_connections) {
                    CallableStatement cs = connection.prepareCall("{call InsertA(?, ?)}");
                    cs.setInt(1, m_value+100);
                    cs.setInt(2, m_value+1000);
                    m_value++;
                    m_callableStatements.add(cs);
                }
            }

            void testStatements(int expectConnectionFailures)
            {
                if (expectConnectionFailures < 0) {
                    expectConnectionFailures = m_callableStatements.size();
                }
                for (int i = 0; i < m_connections.length; ++i) {
                    try {
                        m_callableStatements.get(i).execute();
                        assertEquals(0, expectConnectionFailures);
                    }
                    catch(SQLException e) {
                        assertTrue(expectConnectionFailures > 0);
                        expectConnectionFailures--;
                        assertEquals(e.getSQLState(), SQLError.CONNECTION_FAILURE);
                    }
                }
            }
        }

        // Expect connection/query successes.
        {
            Tester tester = new Tester();
            tester.makeStatements();
            tester.testStatements(0);
        }

        // Shut down unceremoneously
        m_server.shutdown();

        // Expect connection failures.
        {
            Tester tester = new Tester();
            tester.makeStatements();
            tester.testStatements(-1);
        }

        // Restart server.
        startServer();

        // Expect connection/query successes.
        {
            Tester tester = new Tester();
            tester.makeStatements();
            tester.testStatements(0);
        }
    }
}
