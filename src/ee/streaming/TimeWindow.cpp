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
#include "streaming/TimeWindow.h"
#include "streaming/WindowTableTemp.h"

namespace voltdb 
{
/**
 * This value has to match the value in CopyOnWriteContext.cpp
 */
#define TABLE_BLOCKSIZE 2097152
#define MAX_EVICTED_TUPLE_SIZE 2500

TimeWindow::TimeWindow(ExecutorContext *ctx, bool exportEnabled, int windowSize, int slideSize)
    : WindowTableTemp(ctx, exportEnabled, windowSize, slideSize), 
    TS_COLUMN("TIME"), WS_COLUMN("WSTART"), WE_COLUMN("WEND")
{
    VOLT_DEBUG("TimeWindow Constructor");
    m_tsColumn = -1;
    m_wsColumn = -1;
    m_weColumn = -1;
    m_clockTS  = -1;
    m_firstClockTS = -1;
    m_isFirstTuple = true;
    m_slideModulo = 0;
    m_activeWindowID = 0;
    m_currentEndWindowID = -1;
    m_ctx = ctx;
}

TimeWindow::~TimeWindow()
{
}

void TimeWindow::initWin()
{
    setColumnIndices();
    //TODO:  Look into improving performance by using index on WS_COLUMN
    m_stageTable = TableFactory::getPersistentTable(this->databaseId(), m_ctx, this->name() + "_stage", const_cast<TupleSchema*>(this->schema()), this->columnNames(), -1, false, false);
}

void TimeWindow::initTimeWindowTuple(TableTuple &source, int32_t startWinID, int32_t endWinID)
{    
    VOLT_DEBUG("Start initTimeWindowTuple()");
    VOLT_DEBUG("startWinID = %d\tendWinID = %d", startWinID, endWinID);
    source.setNValue(m_wsColumn, ValueFactory::getIntegerValue(startWinID));
    source.setNValue(m_weColumn, ValueFactory::getIntegerValue(endWinID));
}

void TimeWindow::setColumnIndices()
{
    VOLT_DEBUG("Before calling columnIndex() TS COLUMN %s: %d", TS_COLUMN.c_str(), m_tsColumn);
    m_tsColumn = columnIndex(TS_COLUMN);
    VOLT_DEBUG("TS COLUMN %s: %d", TS_COLUMN.c_str(), m_tsColumn);
    assert(m_tsColumn >= 0); //Time window MUST have a time stamp column
    m_wsColumn = columnIndex(WS_COLUMN);
    VOLT_DEBUG("WS COLUMN %s: %d", WS_COLUMN.c_str(), m_wsColumn);
    assert(m_wsColumn >= 0); //Time window MUST have a window start column
    m_weColumn = columnIndex(WE_COLUMN);
    VOLT_DEBUG("WE COLUMN %s: %d", WE_COLUMN.c_str(), m_weColumn);
    assert(m_weColumn >= 0); //Time window MUST have a window end column
}

/** All get**Column() are simply column index accessor mostly exist for testing */
int TimeWindow::getTSColumn()
{
    VOLT_DEBUG("TS COLUMN %s: %d", TS_COLUMN.c_str(), m_tsColumn);
    return m_tsColumn;
}

int TimeWindow::getWSColumn()
{
    VOLT_DEBUG("WS COLUMN %s: %d", WS_COLUMN.c_str(), m_wsColumn);
    return m_wsColumn;
}

int TimeWindow::getWEColumn()
{
    VOLT_DEBUG("WE COLUMN %s: %d", WE_COLUMN.c_str(), m_weColumn);
    return m_weColumn;
}

const int32_t& TimeWindow::getTS(TableTuple &source)
{
    return source.getNValue(m_tsColumn).getInteger();
}

const int32_t& TimeWindow::getWS(TableTuple &source)
{
    return source.getNValue(m_wsColumn).getInteger();
}

const int32_t& TimeWindow::getWE(TableTuple &source)
{
    return source.getNValue(m_weColumn).getInteger();
}

void TimeWindow::setClockTS(int32_t ts)
{
    // Update timestamp
    if (m_isFirstTuple)
    {
        m_isFirstTuple = false;
        m_firstClockTS = ts;
        m_slideModulo = m_firstClockTS % m_slideSize;
        m_clockTS = ts;
    }
    else if (m_clockTS < ts) 
    {
        m_clockTS = ts;
    } 
    else 
    {
        return;
    }
    
    // Detect window starts
    if ((m_clockTS % m_slideSize) == m_slideModulo) 
    {
        windowStarts();
    }
    // Detect window ends
    if (m_activeWindowID == 0) 
    {
        //Looking for the first window to end
        if ((m_clockTS - m_firstClockTS) >= m_windowSize) 
        {
            windowEnds();
        }
    }
    // subtract the m_windowSize to line up to the window starts point
    else if ((m_clockTS - m_windowSize) % m_slideSize == m_slideModulo)
    {
        windowEnds();
    }
}

void TimeWindow::windowStarts()
{
    VOLT_DEBUG("Enter windowStarts");
    m_currentEndWindowID++;
    VOLT_DEBUG("New Window ID: %d", m_currentEndWindowID);
    VOLT_DEBUG("Exit windowStarts");
}

void TimeWindow::windowEnds()
{
    VOLT_DEBUG("Enter windowEnds");
    VOLT_DEBUG("Closing Window ID: %d", m_activeWindowID);
    TableTuple tuple(m_schema);    
    TableTuple copy_tuple(m_schema);    
    //Allocate space for temporaly tuple
    copy_tuple.move(new char[copy_tuple.tupleLength()]);
    //Remove expired tuple from window
    TableIterator active_iter(static_cast<Table*>(this));
    while(active_iter.hasNext())
    {
        active_iter.next(tuple);
        VOLT_DEBUG("Checking for is ending with Window:  %s ending with Window ID: %d", tuple.debug("").c_str(), m_activeWindowID - 1);
        if (TimeWindow::getWE(tuple) == (m_activeWindowID - 1))
        {
            VOLT_DEBUG("Delete expired tuple");
            if (! (PersistentTable::deleteTuple(tuple, true)))
            {
                VOLT_DEBUG("Failed to delete expired tuple from this active window table");
                return;
            }
        }
    }
    //Move tuple from stage to this window
    TableIterator stage_iter(static_cast<Table*>(m_stageTable));
    while(stage_iter.hasNext())
    {
        stage_iter.next(tuple);
        VOLT_DEBUG("Checking for is in Window:  %s is in Window ID %d?", tuple.debug("").c_str(), m_activeWindowID);
        if ((TimeWindow::getWS(tuple) <= m_activeWindowID) && (m_activeWindowID <= TimeWindow::getWE(tuple)))
        {
            VOLT_DEBUG("Copying tuple");
            copy_tuple.copyForPersistentInsert(tuple);
            VOLT_DEBUG("Move tuple from stage to this active window");
            if (!(PersistentTable::insertTuple(copy_tuple)))
            {
                VOLT_DEBUG("Failed to insert tuple into this active window table");
                return;
            }
            if (!(m_stageTable->deleteTuple(tuple, true)))
            {
                VOLT_DEBUG("Failed to remove moved tuple from stage table");
                return;
            }
        }
    }
    m_activeWindowID++;

    if(hasTriggers())
    {
        VOLT_DEBUG("Fire Window Trigger");
        setFireTriggers(true);
    }
    //Delete space for temporaly tuple
    copy_tuple.freeObjectColumns();
    delete [] copy_tuple.address();
    VOLT_DEBUG("Exit windowEnds");
}

int64_t TimeWindow::getStageActiveTupleCount()
{
    return m_stageTable->activeTupleCount();
}

bool TimeWindow::insertTuple(TableTuple &source)
{
    VOLT_DEBUG("TimeWindow INSERT TUPLE");
    VOLT_DEBUG("latestTS: %d, windowSize: %d, slideSize: %d",
            m_clockTS, m_windowSize, m_slideSize);
    const int32_t ts = getTS(source);
    VOLT_DEBUG("Tuple time: %d, clock time: %d", ts, m_clockTS);

    // Ideally, this is done by system clock instead of from tuple's timestamp
    setClockTS(ts);
    VOLT_DEBUG("Updated clock time: %d", m_clockTS);
    
    
    if ((m_slideSize > m_windowSize) && (((ts + m_slideModulo) % m_slideSize) >= m_windowSize))
    {
        VOLT_DEBUG("Ignore tuple as its timestamp is not within any valid window");
        return true;
    }
    initTimeWindowTuple(source, m_activeWindowID, m_currentEndWindowID);

    if (!(m_stageTable->insertTuple(source)))
    {
        VOLT_DEBUG("Failed to insert tuple into staging table");
        return false;
    }
    return true;
}

bool TimeWindow::deleteTuple(TableTuple &tuple, bool deleteAllocatedStrings)
{
    VOLT_DEBUG("TimeWindow DELETE TUPLE");
    return PersistentTable::deleteTuple(tuple, deleteAllocatedStrings);
}

void TimeWindow::deleteAllTuples(bool deleteAllocatedStrings)
{
    VOLT_DEBUG("TimeWindow DELETE ALL TUPLES");
    m_stageTable->deleteAllTuples(true);
    PersistentTable::deleteAllTuples(deleteAllocatedStrings);
}

std::string TimeWindow::debug()
{
    std::ostringstream output;
    TableIterator win_itr(this);
    TableTuple tuple(m_schema);
    VOLT_DEBUG("Enter TimeWindow::debug()");
    
    output << "DEBUG TimeWindow: " << this->activeTupleCount() << " tuples, " << m_stageTable->activeTupleCount() << " staged\n";
    bool fullDetail = false;
    if (fullDetail) {
        output << this->Table::debug().c_str() << "\n\n";
        output << m_stageTable->Table::debug().c_str() << "\n\n";
    }
    else 
    {
        int stageLabel = 0;
        int winLabel = 0;
        TableIterator active_iter(static_cast<Table*>(this));
        output << "\nvvvvvvv\n";
        while(active_iter.hasNext())
        {
            active_iter.next(tuple);
            output << "\tWINDOW " << winLabel << ": " << tuple.debug("").c_str() << "\n";
            winLabel++;
        }
        output << "\n-------\n";
        TableIterator stage_iter(static_cast<Table*>(m_stageTable));
        while(stage_iter.hasNext())
        {
            stage_iter.next(tuple);
            output << "\tSTAGED " << stageLabel << ": " << tuple.debug("").c_str() << "\n";
            stageLabel++;
        }
        output << "\n^^^^^^^\n";
    }
     return output.str();
}
}

