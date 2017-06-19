/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

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

package org.voltdb.sysprocs;

import java.util.HashMap;
import java.util.TreeSet;
import java.util.Iterator;

import org.voltdb.sysprocs.saverestore.SnapshotUtil;

/**
 * The snapshot registry contains information about snapshots that executed
 * while the system was running.
 *
 */
public class SnapshotRegistry {
    private static final int m_maxStatusHistory = 10;

    private static final TreeSet<Snapshot> m_snapshots = new TreeSet<Snapshot>(
            new java.util.Comparator<Snapshot>() {

                @Override
                public int compare(Snapshot o1, Snapshot o2) {
                    return new Long(o1.timeStarted).compareTo(o2.timeStarted);
                }

            });

    public static class Snapshot {
        public final long timeStarted;
        public final long timeFinished;

        public final String path;
        public final String nonce;
        public final boolean result; //true success, false failure

        public final long bytesWritten;

        private final HashMap< String, Table> tables = new HashMap< String, Table>();

        private Snapshot(long startTime, int hostId, int siteId, int partitionId, String path, String nonce,
                         org.voltdb.catalog.Table tables[]) {
            timeStarted = startTime;
            this.path = path;
            this.nonce = nonce;
            timeFinished = 0;
            synchronized (this.tables) {
                for (org.voltdb.catalog.Table table : tables) {
                    String filename =
                        SnapshotUtil.constructFilenameForTable(table,
                                                               nonce,
                                                               Integer.toString(hostId),
                                                               Integer.toString(siteId),
                                                               Integer.toString(partitionId)
                                                               );
                    this.tables.put(table.getTypeName(), new Table(table.getTypeName(), filename));
                }
            }
            result = false;
            bytesWritten = 0;
        }

        private Snapshot(Snapshot incomplete, long timeFinished) {
            timeStarted = incomplete.timeStarted;
            path = incomplete.path;
            nonce = incomplete.nonce;
            this.timeFinished = timeFinished;
            synchronized (tables) {
                tables.putAll(incomplete.tables);
            }
            long bytesWritten = 0;
            boolean result = true;
            for (Table t : tables.values()) {
                bytesWritten += t.size;
                if (t.error != null) {
                    result = false;
                }
            }
            this.bytesWritten = bytesWritten;
            this.result = result;
        }

        public interface TableUpdater {
            public Table update(Table t);
        }

        public interface TableIterator {
            public void next(Table t);
        }

        public void iterateTables(TableIterator ti) {
            synchronized (tables) {
                for (Table t : tables.values()) {
                    ti.next(t);
                }
            }
        }

        public void updateTable(String name, TableUpdater tu) {
            synchronized (tables) {
                assert(tables.get(name) != null);
                tables.put(name, tu.update(tables.get(name)));
            }
        }

        public class Table {
            public final String name;
            public final String filename;
            public final long size;
            public final Exception error;

            private Table(String name, String filename) {
                this.name = name;
                this.filename = filename;
                size = 0;
                error = null;
            }

            public Table(Table t, long size, Exception error) {
                this.name = t.name;
                this.filename = t.filename;
                this.size = size;
                this.error = error;
            }
        }
    }

    public static synchronized Snapshot startSnapshot(long startTime, int hostId, int siteId, int partitionId, String path, String nonce, org.voltdb.catalog.Table tables[]) {
        final Snapshot s = new Snapshot(startTime, hostId, siteId, partitionId, path, nonce, tables);

        m_snapshots.add(s);
        if (m_snapshots.size() > m_maxStatusHistory) {
            Iterator<Snapshot> iter = m_snapshots.iterator();
            iter.next();
            iter.remove();
        }

        return s;
    }

    public static synchronized void discardSnapshot(Snapshot s) {
        m_snapshots.remove(s);
    }

    public static synchronized Snapshot finishSnapshot(Snapshot incomplete) {
        boolean removed = m_snapshots.remove(incomplete);
        assert(removed);
        final Snapshot completed = new Snapshot(incomplete, System.currentTimeMillis());
        m_snapshots.add(completed);
        return completed;
    }

    public static synchronized TreeSet<Snapshot> getSnapshotHistory() {
        return new TreeSet<Snapshot>(m_snapshots);
    }

    public static synchronized void clear() {
        m_snapshots.clear();
    }
}
