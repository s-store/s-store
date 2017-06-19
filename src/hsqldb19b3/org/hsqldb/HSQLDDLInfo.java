/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
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

package org.hsqldb;

/**
 *  Information about DDL passed to HSQL that allows it to be better understood by VoltDB
 */
public class HSQLDDLInfo {

    /**
     * CREATE, ALTER or DROP
     */
    public static enum Verb {
        CREATE, ALTER, DROP;

        public static Verb get(String name) {
            if (name.equalsIgnoreCase("CREATE")) {
                return CREATE;
            }
            else if (name.equalsIgnoreCase("ALTER")) {
                return ALTER;
            }
            else if (name.equalsIgnoreCase("DROP")) {
                return DROP;
            }
            else {
                return null;
            }
        }
    }

    /**
     * TABLE, INDEX or VIEW
     */
    public static enum Noun {
        TABLE, INDEX, VIEW;

        public static Noun get(String name) {
            if (name.equalsIgnoreCase("TABLE")) {
                return TABLE;
            }
            else if (name.equalsIgnoreCase("INDEX")) {
                return INDEX;
            }
            else if (name.equalsIgnoreCase("VIEW")) {
                return VIEW;
            }
            else {
                return null;
            }
        }
    }

    public final HSQLDDLInfo.Verb verb;
    public final HSQLDDLInfo.Noun noun;
    // the index/table/view that goes with the noun (subject of the verb)
    public final String name;
    // used today to hold the table that new indexes are created on
    // CREATE INDEX name ON TABLE secondName ...
    public final String secondName;
    public final boolean cascade;
    public final boolean ifexists;

    public HSQLDDLInfo(HSQLDDLInfo.Verb verb,
                       HSQLDDLInfo.Noun noun,
                       String name,
                       String secondName,
                       boolean cascade,
                       boolean ifexists)
    {
        this.verb = verb;
        this.noun = noun;
        this.name = name;
        this.secondName = secondName;
        this.cascade = cascade;
        this.ifexists = ifexists;
    }
}
