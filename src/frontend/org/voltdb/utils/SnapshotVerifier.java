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

package org.voltdb.utils;

import java.util.*;
import java.io.*;

import org.voltdb.sysprocs.saverestore.*;
import org.voltdb.sysprocs.saverestore.SnapshotUtil.Snapshot;
import org.voltdb.sysprocs.saverestore.SnapshotUtil.SnapshotFilter;
import org.voltdb.sysprocs.saverestore.SnapshotUtil.SpecificSnapshotFilter;

/**
 * A command line utility for scanning and validating snapshots. Provides detailed information about the files
 * that make up a snapshot, what partitions they contains, and whether they are corrupted or intact. In the event
 * that a table file is corrupted it will also specify what partitions can still be salvaged.
 *
 */
public class SnapshotVerifier {

    public static void main(String args[]) {
        if (args.length == 0) {
            //printHelpAndQuit(0);
        } else if (args[0].equals("--help")) {
            printHelpAndQuit(0);
        }

        FileFilter filter = new SnapshotFilter();
        boolean specifiedSingle = false;
        HashSet<String> snapshotNames = new HashSet<String>();
        for (int ii = 0; ii < args.length; ii++) {
            if (args[ii].equals("--dir")) {
                ii++;
                continue;
            }
            specifiedSingle = true;
            snapshotNames.add(args[ii]);
        }

        if (specifiedSingle) {
            filter = new SpecificSnapshotFilter(snapshotNames);
        }

        List<String> directories = new ArrayList<String>();
        for (int ii = 0; ii < args.length; ii++) {
            if (args[ii].equals("--dir")) {
                if (ii + 1 >= args.length) {
                    System.err.println("Error: No directories specified after --dir");
                    printHelpAndQuit(-1);
                    break;
                }
                directories.add(args[ii + 1]);
                ii++;
            }
        }
        if (directories.isEmpty()) {
            directories.add(".");
        }

        TreeMap<Long, Snapshot> snapshots = new TreeMap<Long, Snapshot>();
        for (String directory : directories) {
            SnapshotUtil.retrieveSnapshotFiles( new File(directory), snapshots, filter, 0, true);
        }

        if (snapshots.isEmpty()) {
            System.out.println("Snapshot corrupted");
            System.out.println("No files found");
        }
        for (Map.Entry<Long, Snapshot> s : snapshots.entrySet()) {
            System.out.println(SnapshotUtil.generateSnapshotReport(s.getKey(), s.getValue()).getSecond());
        }
    }

    private static void printHelpAndQuit( int code) {
        System.out.println("Usage\nSpecific snapshot: java -cp <classpath> -Djava.library.path=<library path> org.voltdb.utils.SnapshotVerifier snapshot_name --dir dir1 --dir dir2 --dir dir3");
        System.out.println("All snapshots: java -cp <classpath> -Djava.library.path=<library path> org.voltdb.utils.SnapshotVerifier --dir dir1 --dir dir2 --dir dir3");
        System.exit(code);
    }
}
