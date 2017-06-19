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

#include <sstream>
#include <cassert>
#include <cstdio>
#include <list>

#include "storage/tablefactory.h"
#include "common/ValueFactory.hpp"
#include "streaming/TupleWindow.h"
#include "streaming/WindowTableTemp.h"

namespace voltdb {
/**
 * This value has to match the value in CopyOnWriteContext.cpp
 */
#define TABLE_BLOCKSIZE 2097152
#define MAX_EVICTED_TUPLE_SIZE 2500

TupleWindow::TupleWindow(ExecutorContext *ctx, bool exportEnabled,
		int windowSize, int slideSize, int groupByIndex) :
		WindowTableTemp(ctx, exportEnabled, windowSize, slideSize), WS_COLUMN(
				"WSTART"), WE_COLUMN("WEND"), GK_COLUMN("_GROUP_KEY_"), TC_COLUMN(
				"_TUPLE_COUNT_"), AW_COLUMN("_ACTIVE_WIN_ID_"), CE_COLUMN(
				"_CURR_END_WIN_ID_") {
	VOLT_DEBUG("TupleWindow Constructor");
	m_groupByIndex = groupByIndex;
	m_wsColumn = -1;
	m_weColumn = -1;

	m_tupleCount = 0;
	m_activeWindowID = -1;
	m_currentEndWindowID = -1;

	m_ctx = ctx;
}

TupleWindow::~TupleWindow() {
}

void TupleWindow::initWin() {
	//TODO:  Look into improving performance by using index on WS_COLUMN
	m_stageTable = TableFactory::getPersistentTable(this->databaseId(), m_ctx,
			this->name() + "_stage", const_cast<TupleSchema*>(this->schema()),
			this->columnNames(), -1, false, false);

	if (m_groupByIndex != GROUP_BY_NONE) {
		if (m_groupByIndex > this->columnCount()) {
			throw "Invalid column index used in grouping";
		}
		//TODO:  Validate the data type of the group by column.
		VOLT_DEBUG("Grouping on column index %d", m_groupByIndex);
		std::string keyTableColumnNames[] = { GK_COLUMN, TC_COLUMN, AW_COLUMN,
				CE_COLUMN };
		std::vector<voltdb::ValueType> columnTypes;
		std::vector<int32_t> columnLengths;
		std::vector<bool> columnAllowNull;
		for (int ctr = 0;
				ctr
						< sizeof(keyTableColumnNames)
								/ sizeof(keyTableColumnNames[0]); ctr++) {
			//TODO:  Expand support for other value types
			columnTypes.push_back(voltdb::VALUE_TYPE_INTEGER);
			columnLengths.push_back(
					NValue::getTupleStorageSize(voltdb::VALUE_TYPE_INTEGER));
			columnAllowNull.push_back(false);
		}
		//TODO:  Create unique index on keyValue column
		voltdb::TupleSchema *keyTableSchema =
				voltdb::TupleSchema::createTupleSchema(columnTypes,
						columnLengths, columnAllowNull, true);

		m_keyTable = TableFactory::getPersistentTable(this->databaseId(), m_ctx,
				this->name() + "_groupkey", keyTableSchema, keyTableColumnNames,
				-1, false, false);
	}
	setColumnIndices();
}

/** All get**Column() are simply column index accessor mostly exist for testing */
int TupleWindow::getWSColumn() {
	VOLT_DEBUG("WS COLUMN %s: %d", WS_COLUMN.c_str(), m_wsColumn);
	return m_wsColumn;
}

int TupleWindow::getWEColumn() {
	VOLT_DEBUG("WE COLUMN %s: %d", WE_COLUMN.c_str(), m_weColumn);
	return m_weColumn;
}

int TupleWindow::getGKColumn() {
	VOLT_DEBUG("GK COLUMN %s: %d", GK_COLUMN.c_str(), m_gkColumn);
	return m_gkColumn;
}

int TupleWindow::getTCColumn() {
	VOLT_DEBUG("TC COLUMN %s: %d", TC_COLUMN.c_str(), m_tcColumn);
	return m_tcColumn;
}

int TupleWindow::getAWColumn() {
	VOLT_DEBUG("AW COLUMN %s: %d", AW_COLUMN.c_str(), m_awColumn);
	return m_awColumn;
}

int TupleWindow::getCEColumn() {
	VOLT_DEBUG("CE COLUMN %s: %d", CE_COLUMN.c_str(), m_ceColumn);
	return m_ceColumn;
}

const int32_t& TupleWindow::getGroupKeyFromSourceInteger(TableTuple &source) {
	VOLT_DEBUG("Group By Index: %d in tuple %s", m_groupByIndex,
			source.debug("incoming tuple").c_str());
	return source.getNValue(m_groupByIndex).getInteger();
}

const int32_t& TupleWindow::getWS(TableTuple &source) {
	return source.getNValue(m_wsColumn).getInteger();
}

const int32_t& TupleWindow::getWE(TableTuple &source) {
	return source.getNValue(m_weColumn).getInteger();
}

const int32_t& TupleWindow::getGK(TableTuple &source) {
	return source.getNValue(m_gkColumn).getInteger();
}

const int32_t& TupleWindow::getTC(TableTuple &source) {
	return source.getNValue(m_tcColumn).getInteger();
}

const int32_t& TupleWindow::getAW(TableTuple &source) {
	return source.getNValue(m_awColumn).getInteger();
}

const int32_t& TupleWindow::getCE(TableTuple &source) {
	return source.getNValue(m_ceColumn).getInteger();
}

TableTuple TupleWindow::getKeyData(int32_t keyValue) {
	VOLT_DEBUG("Enter getKeyData(%d)", keyValue);
	if (m_groupByIndex == GROUP_BY_NONE) {
		throw("This is not a group window");
	}
	VOLT_DEBUG("Searching for key %d", keyValue);
	//TODO:  Replace this with Index lookup
	TableTuple tuple(m_keyTable->schema());
	TableIterator key_iter(static_cast<Table*>(m_keyTable));
	while (key_iter.hasNext()) {
		key_iter.next(tuple);
		if (TupleWindow::getGK(tuple) == keyValue) {
			VOLT_DEBUG("\tFound key data");
			m_tupleCount = TupleWindow::getTC(tuple);
			m_activeWindowID = TupleWindow::getAW(tuple);
			m_currentEndWindowID = TupleWindow::getCE(tuple);
			VOLT_DEBUG("Key Tuple %s", tuple.debug("m_KeyTable").c_str());
			return tuple;
		}
	}
	m_tupleCount = 0;
	m_activeWindowID = -1;
	m_currentEndWindowID = -1;
	//Allocate space for new tuple
	tuple.move(new char[tuple.tupleLength()]);
	initKeyTableTuple(tuple, keyValue, m_tupleCount, m_activeWindowID,
			m_currentEndWindowID);
	if (!(m_keyTable->insertTuple(tuple))) {
		VOLT_INFO("Failed to insert new tuple into table '%s'",
				m_keyTable->name().c_str());
		return tuple;
	}
	//FIXME:  The 'tuple' from insert is not the same as the one that got inserted.
	// It cannot be use for the update process, so now try to scan for the inserted tuple from the table.
	TableIterator key_iter2(static_cast<Table*>(m_keyTable));
	while (key_iter2.hasNext()) {
		key_iter2.next(tuple);
		if (TupleWindow::getGK(tuple) == keyValue) {
			VOLT_DEBUG("Key Tuple %s", tuple.debug("m_KeyTable").c_str());
			VOLT_DEBUG("Exit getKeyData");
			return tuple;
		}
	}
	VOLT_DEBUG("Can't find the inserted tuple");
	return tuple;
}

void TupleWindow::saveKeyData(TableTuple &source) {
	VOLT_DEBUG("Enter saveKeyData");
	VOLT_DEBUG("Current Key Tuple %s", source.debug("m_KeyTable").c_str());
	TableTuple &tempTuple =
			((PersistentTable*) m_keyTable)->getTempTupleInlined(source);
	tempTuple.setNValue(m_tcColumn,
			ValueFactory::getIntegerValue(m_tupleCount));
	tempTuple.setNValue(m_awColumn,
			ValueFactory::getIntegerValue(m_activeWindowID));
	tempTuple.setNValue(m_ceColumn,
			ValueFactory::getIntegerValue(m_currentEndWindowID));

	if (!(m_keyTable->updateTuple(tempTuple, source, false))) {
		VOLT_INFO("Failed to update tuple from table '%s'",
				m_keyTable->name().c_str());
		return;
	}
	VOLT_DEBUG("Updated Key Tuple %s", source.debug("m_KeyTable").c_str());
	VOLT_DEBUG("exit saveKeyData");
}

void TupleWindow::initTupleWindowTuple(TableTuple &source, int32_t startWinID,
		int32_t endWinID) {
	VOLT_DEBUG("Start initTupleWindowTuple()");
	VOLT_DEBUG("startWinID = %d\tendWinID = %d, m_wsColumn=%d, m_weColumn=%d",
			startWinID, endWinID, m_wsColumn, m_weColumn);
	source.setNValue(m_wsColumn, ValueFactory::getIntegerValue(startWinID));
	source.setNValue(m_weColumn, ValueFactory::getIntegerValue(endWinID));
	VOLT_DEBUG("Exit initTupleWindowTuple()");
}

void TupleWindow::initKeyTableTuple(TableTuple &source, int32_t gk, int32_t tc,
		int32_t aw, int32_t ce) {
	VOLT_DEBUG("Start initKeyTableTuple()");
	VOLT_DEBUG(
			"Key = %d\tTupleCount = %d\tActiveWindowID = %d\tCurrentEndWindowID = %d",
			gk, tc, aw, ce);
	source.setNValue(m_gkColumn, ValueFactory::getIntegerValue(gk));
	source.setNValue(m_tcColumn, ValueFactory::getIntegerValue(tc));
	source.setNValue(m_awColumn, ValueFactory::getIntegerValue(aw));
	source.setNValue(m_ceColumn, ValueFactory::getIntegerValue(ce));
	VOLT_DEBUG("Exit initKeyTableTuple()");
}

void TupleWindow::setColumnIndices() {
	m_wsColumn = m_stageTable->columnIndex(WS_COLUMN);
	VOLT_DEBUG("WS COLUMN %s: %d", WS_COLUMN.c_str(), m_wsColumn);
	assert(m_wsColumn >= 0);
	m_weColumn = m_stageTable->columnIndex(WE_COLUMN);
	VOLT_DEBUG("WE COLUMN %s: %d", WE_COLUMN.c_str(), m_weColumn);
	assert(m_weColumn >= 0);

	if (m_groupByIndex != GROUP_BY_NONE) {
		m_gkColumn = m_keyTable->columnIndex(GK_COLUMN);
		VOLT_DEBUG("GK COLUMN %s: %d", GK_COLUMN.c_str(), m_gkColumn);
		assert(m_gkColumn >= 0);
		m_tcColumn = m_keyTable->columnIndex(TC_COLUMN);
		VOLT_DEBUG("TC COLUMN %s: %d", TC_COLUMN.c_str(), m_tcColumn);
		assert(m_tcColumn >= 0);
		m_awColumn = m_keyTable->columnIndex(AW_COLUMN);
		VOLT_DEBUG("AW COLUMN %s: %d", AW_COLUMN.c_str(), m_awColumn);
		assert(m_awColumn >= 0);
		m_ceColumn = m_keyTable->columnIndex(CE_COLUMN);
		VOLT_DEBUG("CE COLUMN %s: %d", CE_COLUMN.c_str(), m_ceColumn);
		assert(m_ceColumn >= 0);
	}
}

void TupleWindow::windowStarts() {
	VOLT_DEBUG("Enter windowStarts");
	m_currentEndWindowID++;
	VOLT_DEBUG("New Window ID: %d", m_currentEndWindowID);
	VOLT_DEBUG("Exit windowStarts");
}

void TupleWindow::windowEnds(int32_t keyValue) {
	VOLT_DEBUG("Enter windowEnds");
	VOLT_DEBUG("Closing Window ID: %d", m_activeWindowID);
	TableTuple tuple(m_schema);
	TableTuple copy_tuple(m_schema);
	//Allocate space for temporaly tuple
	copy_tuple.move(new char[copy_tuple.tupleLength()]);
	//Remove expired tuple from window
	TableIterator active_iter(static_cast<Table*>(this));
	while (active_iter.hasNext()) {
		active_iter.next(tuple);
		if ((m_groupByIndex != GROUP_BY_NONE)) {
			//this is a group operation
			//TODO:  improve with index the groupKey column in the m_keyTable
			int groupKey = TupleWindow::getGroupKeyFromSourceInteger(tuple);
			if (groupKey != keyValue) {
				continue;
			}
		}
		VOLT_DEBUG(
				"Checking for is ending with Window:  %s ending with expired Window ID: %d",
				tuple.debug("").c_str(), m_activeWindowID);
		if (TupleWindow::getWE(tuple) == m_activeWindowID) {
			VOLT_DEBUG("Delete expired tuple");
			if (!(PersistentTable::deleteTuple(tuple, true))) {
				VOLT_INFO("Failed to delete expired tuple from table '%s'",
						this->name().c_str());
				return;
			}
		}
	}
	//Move tuple from stage to this window
	TableIterator stage_iter(static_cast<Table*>(m_stageTable));
	while (stage_iter.hasNext()) {
		stage_iter.next(tuple);
		if ((m_groupByIndex != GROUP_BY_NONE)) {
			//this is a group operation
			//TODO:  improve with index the groupKey column in the m_keyTable
			int groupKey = TupleWindow::getGroupKeyFromSourceInteger(tuple);
			if (groupKey != keyValue) {
				continue;
			}
		}
		VOLT_DEBUG("Checking for is in Window:  %s is in Window ID %d?",
				tuple.debug("").c_str(), (m_activeWindowID + 1));
		if ((TupleWindow::getWS(tuple) <= (m_activeWindowID + 1))
				&& ((m_activeWindowID + 1) <= TupleWindow::getWE(tuple))) {
			VOLT_DEBUG("Copying tuple");
			copy_tuple.copyForPersistentInsert(tuple);
			VOLT_DEBUG("Move tuple from stage to this active window");
			if (!(PersistentTable::insertTuple(copy_tuple))) {
				VOLT_INFO("Failed to insert tuple into table %s",
						this->name().c_str());
				return;
			}
			if (!(m_stageTable->deleteTuple(tuple, true))) {
				VOLT_INFO("Failed to delete moved tuple from table '%s'",
						m_stageTable->name().c_str());
				return;
			}
		}
	}
	// Advance to new active Window ID after removed tuples from the expired window and finished setup up the new window
	m_activeWindowID++;

	if (hasTriggers()) {
		VOLT_DEBUG("Fire Window Trigger");
		setFireTriggers(true);
	}
	//Delete space for temporaly tuple
	copy_tuple.freeObjectColumns();
	delete[] copy_tuple.address();
	VOLT_DEBUG("Exit windowEnds");
}

int64_t TupleWindow::getStageActiveTupleCount() {
	return m_stageTable->activeTupleCount();
}

bool TupleWindow::insertTupleGroupByNone(TableTuple& source) {
	bool windowEndsOccurred = false;
	m_tupleCount++;
	if (m_slideSize == 1 || (m_tupleCount % m_slideSize) == 1) {
		windowStarts();
	}
	if (m_activeWindowID == -1) {
		if ((m_tupleCount - m_windowSize) >= 0) {
			//first window ends
			windowEnds();
			windowEndsOccurred = true;
		}
	} else if (m_slideSize == 1
			|| (m_tupleCount - m_windowSize + 1) % m_slideSize == 1) {
		windowEnds();
		windowEndsOccurred = true;
	}

	VOLT_DEBUG(
			"windowSize: %d, slideSize: %d, tupleCount: %d, activeWindowID: %d, currentEndWindowID: %d",
			m_windowSize, m_slideSize, m_tupleCount, m_activeWindowID,
			m_currentEndWindowID);
	if (windowEndsOccurred) {
		// Put tuple in main
		initTupleWindowTuple(source, m_activeWindowID, m_currentEndWindowID);
		if (!(PersistentTable::insertTuple(source))) {
			VOLT_INFO("Failed to insert tuple into table '%s'",
					this->name().c_str());
			return false;
		}
	} else if ((m_slideSize > m_windowSize)
			&& ((((m_tupleCount - 1) % m_slideSize) + 1) > m_windowSize)) {
		VOLT_DEBUG("Ignore tuple as it is not belong to any valid window");
		return true;
	} else {
		// Put tuple in stage
		initTupleWindowTuple(source, m_activeWindowID + 1,
				m_currentEndWindowID);
		if (!(m_stageTable->insertTuple(source))) {
			VOLT_INFO("Failed to insert tuple into table '%s'",
					m_stageTable->name().c_str());
			return false;
		}
	}
	return true;
}

bool TupleWindow::insertTupleGroupByIndex(TableTuple& source, int keyValue) {
	VOLT_DEBUG("Enter insertTupleGroupByIndex(%s, %d)",
			source.debug("incoming tuple").c_str(), keyValue);
	bool windowEndsOccurred = false;
	m_tupleCount++;
	if (m_slideSize == 1 || (m_tupleCount % m_slideSize) == 1) {
		windowStarts();
	}
	if (m_activeWindowID == -1) {
		if ((m_tupleCount - m_windowSize) >= 0) {
			//first window ends
			windowEnds(keyValue);
			windowEndsOccurred = true;
		}
	} else if (m_slideSize == 1
			|| (m_tupleCount - m_windowSize + 1) % m_slideSize == 1) {
		windowEnds(keyValue);
		windowEndsOccurred = true;
	}

	VOLT_DEBUG(
			"windowSize: %d, slideSize: %d, tupleCount: %d, activeWindowID: %d, currentEndWindowID: %d",
			m_windowSize, m_slideSize, m_tupleCount, m_activeWindowID,
			m_currentEndWindowID);
	if (windowEndsOccurred) {
		// Put tuple in main
		initTupleWindowTuple(source, m_activeWindowID, m_currentEndWindowID);
		if (!(PersistentTable::insertTuple(source))) {
			VOLT_INFO("Failed to insert tuple into table '%s'",
					this->name().c_str());
			return false;
		}
	} else if ((m_slideSize > m_windowSize)
			&& ((((m_tupleCount - 1) % m_slideSize) + 1) > m_windowSize)) {
		VOLT_DEBUG("Ignore tuple as it is not belong to any valid window");
		return true;
	} else {
		// Put tuple in stage
		initTupleWindowTuple(source, m_activeWindowID + 1,
				m_currentEndWindowID);
		if (!(m_stageTable->insertTuple(source))) {
			VOLT_INFO("Failed to insert tuple into table '%s'",
					m_stageTable->name().c_str());
			return false;
		}
	}
	VOLT_DEBUG("exit insertTupleGroupByIndex");
	return true;
}

bool TupleWindow::insertTuple(TableTuple &source) {
	VOLT_DEBUG("TupleWindow INSERT TUPLE");

	if (m_groupByIndex == GROUP_BY_NONE) {
		return insertTupleGroupByNone(source);
	} else {
		int groupKey = getGroupKeyFromSourceInteger(source);
		VOLT_DEBUG("key = %d, tuple = %s", groupKey,
				source.debug("incoming tuple").c_str());
		TableTuple keyTuple = getKeyData(groupKey);
		insertTupleGroupByIndex(source, groupKey);
		saveKeyData(keyTuple);
	}
	return true;
}

bool TupleWindow::deleteTuple(TableTuple &tuple, bool deleteAllocatedStrings) {
	VOLT_DEBUG("TupleWindow DELETE TUPLE");
	return PersistentTable::deleteTuple(tuple, deleteAllocatedStrings);
}

void TupleWindow::deleteAllTuples(bool deleteAllocatedStrings) {
	VOLT_DEBUG("TupleWindow DELETE ALL TUPLES");
	m_stageTable->deleteAllTuples(true);
	PersistentTable::deleteAllTuples(deleteAllocatedStrings);
}

std::string TupleWindow::debug() {
	std::ostringstream output;
	TableIterator win_itr(this);
	TableTuple tuple(m_schema);
	VOLT_DEBUG("Enter TupleWindow::debug()");

	output << "DEBUG TupleWindow: " << this->activeTupleCount() << " tuples, "
			<< m_stageTable->activeTupleCount() << " staged\n";
	bool fullDetail = false;
	if (fullDetail) {
		output << this->Table::debug().c_str() << "\n\n";
		output << m_stageTable->Table::debug().c_str() << "\n\n";
	} else {
		int stageLabel = 0;
		int winLabel = 0;
		TableIterator active_iter(static_cast<Table*>(this));
		output << "\nvvvvvvv\n";
		while (active_iter.hasNext()) {
			active_iter.next(tuple);
			output << "\tWINDOW " << winLabel << ": " << tuple.debug("").c_str()
					<< "\n";
			winLabel++;
		}
		output << "\n-------\n";
		TableIterator stage_iter(static_cast<Table*>(m_stageTable));
		while (stage_iter.hasNext()) {
			stage_iter.next(tuple);
			output << "\tSTAGED " << stageLabel << ": "
					<< tuple.debug("").c_str() << "\n";
			stageLabel++;
		}
		output << "\n^^^^^^^\n";
	}
	return output.str();
}
}

