/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

#ifndef ARIESLOGPROXY_H_
#define ARIESLOGPROXY_H_

#include "logging/LogDefs.h"
#include "logging/LogProxy.h"

#include <iostream>
#include <cstdio>
#include <fstream>

namespace voltdb {
class VoltDBEngine;

/**
 * A log proxy implementation geared toward Aries. Implements an
 * extra function to log binary output to files.
 */
class AriesLogProxy : public LogProxy {
public:
	~AriesLogProxy();

	void log(LoggerId loggerId, LogLevel level, const char *statement) const;
	static AriesLogProxy* getAriesLogProxy(VoltDBEngine* engine);
	void logBinaryOutput(const char *data, size_t size);
	//void setEngine(VoltDBEngine*);

	std::string getLogFileName();
	static std::string defaultLogfileName;

private:
	AriesLogProxy(VoltDBEngine*);
	AriesLogProxy(VoltDBEngine*, std::string logfileName);

	void init(VoltDBEngine*, std::string logfileName);

	void logLocally(const char *data, size_t size);
	void logToEngineBuffer(const char *data, size_t size);

	std::string logFileName;
	FILE* logFile;
	int logFileFD; // fd equivalent for fsync

	bool jniLogging;
	VoltDBEngine* engine;
};

}

#endif /* ARIESLOGPROXY_H_ */
