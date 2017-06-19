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

#ifndef LOGGER_H_
#define LOGGER_H_
#include "AriesLogProxy.h"
#include "LogDefs.h"
#include "LogProxy.h"
#include <string>
#include <assert.h>

namespace voltdb {

/**
 * A logger caches the current log level for a counterpart logger elsewhere and forwards log statements as necessary.
 */
class Logger {
    friend class LogManager;
public:

    /**
     * Constructor that initializes with logging off and caches a reference to a log proxy where log statements
     * will be forwarded to.
     * @param proxy Log proxy where log statements should be forwarded to
     */
    inline Logger(LogProxy *proxy, LoggerId id) : m_level(LOGLEVEL_OFF), m_id(id), m_logProxy(proxy) {}

    /**
     * Check if a specific log level is loggable
     * @param level Level to check for loggability
     * @returns true if the level is loggable, false otherwise
     */
    inline bool isLoggable(LogLevel level) const {
        assert (level != voltdb::LOGLEVEL_OFF && level != voltdb::LOGLEVEL_ALL); //: "Should never log as ALL or OFF";
        if (level >= m_level) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Log a statement at the level specified in the template parameter.
     * @param level Log level to attempt to log the statement at
     * @param statement Statement to log
     */
    inline void log(const voltdb::LogLevel level, const std::string *statement) const {
        assert(level != voltdb::LOGLEVEL_OFF && level != voltdb::LOGLEVEL_ALL); //: "Should never log as ALL or OFF";
        if (level >= m_level && m_logProxy != NULL) {
            m_logProxy->log( m_id, level, statement->c_str());
        }
    }

    /**
     * Log a statement at the level specified in the template parameter.
     * @param level Log level to attempt to log the statement at
     * @param statement null terminated UTF-8 string containg the statement to log
     */
    inline void log(const voltdb::LogLevel level, const char *statement) const {
        assert (level != voltdb::LOGLEVEL_OFF && level != voltdb::LOGLEVEL_ALL); //: "Should never log as ALL or OFF";
        if (level >= m_level && m_logProxy != NULL) {
            m_logProxy->log( m_id, level, statement);
        }
    }

	/**
	 * For Aries logging only
	 */
	inline void log(const voltdb::LogLevel level, const char *data, size_t len) const {
		assert (level != voltdb::LOGLEVEL_OFF && level != voltdb::LOGLEVEL_ALL); //: "Should never log as ALL or OFF";

		if (m_id != LOGGERID_MM_ARIES) {
			return;
		}

		AriesLogProxy* ariesProxy = const_cast<AriesLogProxy*>(dynamic_cast<const AriesLogProxy*>(m_logProxy));

		if (ariesProxy == NULL) {
			return;
		}

		ariesProxy->logBinaryOutput(data, len);
	}

private:
    /**
     * Currently active log level containing a cached value of the log level of some logger elsewhere
     */
    LogLevel m_level;
    LoggerId m_id;

    /**
     * LogProxy that log statements will be forwarded to.
     */
    const LogProxy *m_logProxy;
};

}
#endif /* LOGGER_H_ */
