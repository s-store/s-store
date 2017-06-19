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

/* Copyright (C) 2017 by S-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Portland State University 
 *
 * Author: S-Store Team (sstore.cs.brown.edu)
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef STDOUTLOGPROXY_H_
#define STDOUTLOGPROXY_H_
#include "LogDefs.h"
#include "LogProxy.h"
#include <iostream>

namespace voltdb {
/**
 * A log proxy implementation that logs all messages to stdout.
 */
class StdoutLogProxy : public LogProxy {

    /**
     * Log a statement on behalf of the specified logger at the specified log level
     * @param LoggerId ID of the logger that received this statement
     * @param level Log level of the statement
     * @param statement null terminated UTF-8 string containing the statement to log
     */
    void log(LoggerId loggerId, LogLevel level, const char *statement) const {
        std::string loggerName;
        switch (loggerId) {
        case voltdb::LOGGERID_HOST:
            loggerName = "HOST";
            break;
        case voltdb::LOGGERID_SQL:
            loggerName = "SQL";
            break;
#ifdef ARIES
        case voltdb::LOGGERID_MM_ARIES:
            loggerName = "MM_ARIES";
            break;
#endif
        default:
            loggerName = "UNKNOWN";
            break;
        }

        std::string logLevel;
        switch (level) {
        case LOGLEVEL_ALL:
            logLevel = "ALL";
            break;
        case LOGLEVEL_TRACE:
            logLevel = "TRACE";
            break;
        case LOGLEVEL_DEBUG:
            logLevel = "DEBUG";
            break;
        case LOGLEVEL_INFO:
            logLevel = "INFO";
            break;
        case LOGLEVEL_WARN:
            logLevel = "WARN";
            break;
        case LOGLEVEL_ERROR:
            logLevel = "ERROR";
            break;
        case LOGLEVEL_FATAL:
            logLevel = "FATAL";
            break;
        case LOGLEVEL_OFF:
            logLevel = "OFF";
            break;
        default:
            logLevel = "UNKNOWN";
            break;
        }
        std::cout << loggerName << " - " << logLevel << " - " << statement << std::endl;
    }
    virtual ~StdoutLogProxy() {

    }
};
}

#endif /* STDOUTLOGPROXY_H_ */
