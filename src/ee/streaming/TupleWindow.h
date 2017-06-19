/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
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

#ifndef HSTORETUPLEWINDOW_H
#define HSTORETUPLEWINDOW_H

#include "common/types.h"
#include "common/ValueFactory.hpp"
#include "storage/persistenttable.h"
#include "streaming/WindowTableTemp.h"

namespace voltdb {

class TableColumn;
class TableIndex;
class TableIterator;
class TableFactory;
class TupleSerializer;
class SerializeInput;
class Topend;
class ReferenceSerializeOutput;
class ExecutorContext;
class MaterializedViewMetadata;
class RecoveryProtoMsg;
class WindowTableTemp;

class TupleWindow: public WindowTableTemp {
	friend class TableFactory;
	friend class TableTuple;
	friend class TableIndex;
	friend class TableIterator;
	friend class PersistentTableStats;
	friend class ValueFactory;

private:
	// no default ctor, no copy, no assignment
	TupleWindow();
	TupleWindow(TupleWindow const&);

	int m_groupByIndex;

	/** Stage Table's column indices */
	int m_wsColumn;
	int m_weColumn;

	/** Key table's column indices */
	int m_gkColumn;
	int m_tcColumn;
	int m_awColumn;
	int m_ceColumn;

	/** Keep count of the new tuples to help determine when to start and stop a window */
	int32_t m_tupleCount;

	/** The current active windows ID (the most recent closed window ID)  */
	int32_t m_activeWindowID;

	/** The Ending range of Window IDs a new arriving tuple may belong to */
	int32_t m_currentEndWindowID;

	/** The table that stored the data for each of group by key.
	 * This table is internal to keep track of the start and end
	 * widow IDs associated with each key */
	Table* m_keyTable;

	/** The table that stored all staging tuples.
	 * All new tuple that are not part of the active Windows */
	Table* m_stageTable;

	/** Saving the executor context for later initialize the staging table */
	ExecutorContext* m_ctx;

	/** Handle insert tuple when no grouping is speicified. */
	bool insertTupleGroupByNone(TableTuple &source);

	/** Handle insert tuple with a specific key */
	bool insertTupleGroupByIndex(TableTuple& source, int key);

	/** Get the Group Key integer value from incoming tuple*/
	const int32_t& getGroupKeyFromSourceInteger(TableTuple &source);

	/** gk == Group Key from internal groupKey table*/
	int getGKColumn();
	const int32_t& getGK(TableTuple &source);

	/** tc == Tuple Count */
	int getTCColumn();
	const int32_t& getTC(TableTuple &source);

	/** aw == Active Window ID */
	int getAWColumn();
	const int32_t& getAW(TableTuple &source);

	/** ce == Current End Window ID */
	int getCEColumn();
	const int32_t& getCE(TableTuple &source);

	/** Get the window's data for a given key, the tuple return is always a tuple from the m_keyTable */
	TableTuple getKeyData(int32_t keyValue);

	/** Update the window's data for a given key */
	void saveKeyData(TableTuple &source);

public:
	~TupleWindow();
	TupleWindow(ExecutorContext *ctx, bool exportEnabled, int32_t windowSize,
			int32_t slideSize = 1, int32_t groupByIndex = GROUP_BY_NONE);

	/** Instantiate the internal tables */
	void initWin();

	/** Set the Windows Start and End IDs for each tuple */
	void initTupleWindowTuple(TableTuple &source, int32_t startWinID,
			int32_t endWinID);

	/** Set the value for a keyTable's tuple */
	void initKeyTableTuple(TableTuple &source, int32_t gk, int32_t tc,
			int32_t aw, int32_t ce);

	/** Set the column index for each of the special column required by TupleWindow */
	void setColumnIndices();

	/** WS == Window Start ID */
	int getWSColumn();
	const int32_t& getWS(TableTuple &source);

	/** WE == Window End ID */
	int getWEColumn();
	const int32_t& getWE(TableTuple &source);

	/** Handle opening a new Window */
	void windowStarts();

	/** Handle closing a window and making it the active one, the keyValue is ignored when m_groupByIndex is GROUP_BY_NONE*/
	void windowEnds(int32_t keyValue = GROUP_BY_NONE);

	/** Get the tuple count in the staging table, this is mainly for testing purpose*/
	int64_t getStageActiveTupleCount();

	/** Get the tuple count in the key table, this is mainly for testing purpose*/
	int64_t getKeyActiveTupleCount();

	// ------------------------------------------------------------------
	// OPERATIONS
	// ------------------------------------------------------------------
	using PersistentTable::insertTuple;
	bool insertTuple(TableTuple &source);

	bool deleteTuple(TableTuple &tuple, bool deleteAllocatedStrings);
	void deleteAllTuples(bool deleteAllocatedStrings);

	const std::string WS_COLUMN;
	const std::string WE_COLUMN;
	const std::string GK_COLUMN;
	const std::string TC_COLUMN;
	const std::string AW_COLUMN;
	const std::string CE_COLUMN;

	std::string debug();
};
}

#endif
