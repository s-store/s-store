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

#ifndef HSTORETIMEWINDOW_H
#define HSTORETIMEWINDOW_H

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

class TimeWindow : public WindowTableTemp {
    friend class TableFactory;
    friend class TableTuple;
    friend class TableIndex;
    friend class TableIterator;
    friend class PersistentTableStats;
    friend class ValueFactory;

  private:
    // no default ctor, no copy, no assignment
    TimeWindow();
    TimeWindow(TimeWindow const&);
    
    int m_tsColumn;
    int m_wsColumn;
    int m_weColumn;

    /** The current active windows ID (the most recent closed window ID)  */
    int32_t m_activeWindowID;

    /** The Ending range of Window IDs a new arriving tuple may belong to */
    int32_t m_currentEndWindowID;

    /** The clock that keep track of the current timestamp.   */
    int32_t m_clockTS;
    
    /** The clock timestamp of the first tuple */
    int32_t m_firstClockTS;
    
    /** Indicates working with the very first tuple */
    bool m_isFirstTuple;
    
    /** The value of first clock modulo by the window size to detect window slide events */
    int32_t m_slideModulo;
    
    /** Ideally, this should be system clock and not readable and can only be
     * updated by the system */
    void setClockTS(int32_t timestamp);
    
    /** The table that stored all staging tuples.  
     * All new tuple that are not part of the active Windows */
    Table* m_stageTable;
    
    /** Saving the executor context for later initialize the staging table */
    ExecutorContext* m_ctx;


  public:
    ~TimeWindow();
    TimeWindow(ExecutorContext *ctx, bool exportEnabled, int windowSize, int slideSize = 1);

    void initWin();
    
    /** Set the value for a given tuple of its Windows Start and End IDs */
    void initTimeWindowTuple(TableTuple &source, int32_t startWinID, int32_t endWinID);
    
    /** Set the column index for each of the special column required by TimeWindow */
    void setColumnIndices();
    
    /** TS == Timestamp */
    int getTSColumn();
    const int32_t& getTS(TableTuple &source);
    
    /** WS == Window Start ID */
    int getWSColumn();
    const int32_t& getWS(TableTuple &source);
    
    /** WE == Window End ID */
    int getWEColumn();
    const int32_t& getWE(TableTuple &source);

    /** Handle opening a new Window */
    void windowStarts();

    /** Handle closing a window and making it the active one */
    void windowEnds();
    
    /** Get the tuple count in the staging table, this is mainly for testing purpose*/
    int64_t getStageActiveTupleCount();
    
    // ------------------------------------------------------------------
    // OPERATIONS
    // ------------------------------------------------------------------
    using PersistentTable::insertTuple;
    bool insertTuple(TableTuple &source);

    bool deleteTuple(TableTuple &tuple, bool deleteAllocatedStrings);
    void deleteAllTuples(bool deleteAllocatedStrings);
    
    const std::string TS_COLUMN;
    const std::string WS_COLUMN;
    const std::string WE_COLUMN;

    std::string debug();
};
}

#endif
