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

package org.voltdb.jdbc;

import java.sql.*;

public class SQLError
{
    public static final String CONNECTION_UNSUCCESSFUL = "08001";
    public static final String CONNECTION_CLOSED = "08003";
    public static final String CONNECTION_FAILURE = "08006";
    public static final String GENERAL_ERROR = "s1000";
    public static final String ILLEGAL_ARGUMENT = "s1009";
    public static final String ILLEGAL_STATEMENT = "s1010";
    public static final String QUERY_PARSING_ERROR = "s1011";
    public static final String INVALID_QUERY_TYPE = "s1012";
    public static final String UNTERMINATED_STRING = "22024";
    public static final String COLUMN_NOT_FOUND = "42S22";
    public static final String PARAMETER_NOT_FOUND = "42S23";
    public static final String CONVERSION_NOT_FOUND = "42S72";
    public static final String TRANSLATION_NOT_FOUND = "42S82";
    public static final String PARAMETER_STILL_NULL = "s1000";

    public static SQLException get(String sqlState)
    {
        return new SQLException(Resources.getString("SQLState." + sqlState), sqlState);
    }

    public static SQLException get(String sqlState, Object... args)
    {
        return new SQLException(Resources.getString("SQLState." + sqlState + "." + args.length, args), sqlState);
    }

    public static SQLException get(Throwable cause)
    {
        return new SQLException(GENERAL_ERROR, cause);
    }

    public static SQLException get(Throwable cause, String sqlState)
    {
        return new SQLException(Resources.getString("SQLState." + sqlState), sqlState, cause);
    }

    public static SQLException get(Throwable cause, String sqlState, Object... args)
    {
        return new SQLException(Resources.getString("SQLState." + sqlState + "." + args.length, args), sqlState, cause);
    }

    public static SQLException noSupport()
    {
        return new SQLFeatureNotSupportedException();
    }

    public static boolean isConnectionError(String status)
    {
        return (status.equals(CONNECTION_UNSUCCESSFUL) ||
                status.equals(CONNECTION_CLOSED) ||
                status.equals(CONNECTION_FAILURE));
    }
}

