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

#include <storage/PersistentTableUndoUpdateAction.h>
#include <cassert>

namespace voltdb {

/*
 * Undo whatever this undo action was created to undo. In this case
 * the string allocations of the new tuple must be freed and the tuple
 * must be overwritten with the old one.
 */
void PersistentTableUndoUpdateAction::undo() {
    //Get the address of the tuple in the table and then update it
    //with the old tuple.  If the indexes haven't been updates then it
    //has to be looked up
    TableTuple tupleInTable;
    if (m_revertIndexes) {
        tupleInTable = m_table->lookupTuple(m_newTuple);
    } else {
        //TableScan will find the already updated tuple since the copy
        //is done immediately
        if (m_table->primaryKeyIndex() == NULL) {
            tupleInTable = m_table->lookupTuple(m_newTuple);
        } else {
            //IndexScan will find it under the old tuple entry since the
            //index was never updated
            tupleInTable = m_table->lookupTuple(m_oldTuple);
        }
    }
    m_table->updateTupleForUndo(m_oldTuple, tupleInTable, m_revertIndexes, m_wrapperOffset);

    /*
     * Free the strings from the new tuple that updated in the old tuple.
     */
    for (std::vector<const char*>::iterator i = newUninlineableColumns.begin();
         i != newUninlineableColumns.end(); i++)
    {
        delete (*i);
    }
}

/*
 * Release any resources held by the undo action. It will not need to
 * be undone in the future. In this case the string allocations of the
 * old tuple must be released.
 */
void PersistentTableUndoUpdateAction::release() {
    /*
     * Free the strings from the old tuple that were updated.
     */
    for (std::vector<const char*>::iterator i = oldUninlineableColumns.begin();
         i != oldUninlineableColumns.end(); i++)
    {
        delete [] (*i);
    }
}

PersistentTableUndoUpdateAction::~PersistentTableUndoUpdateAction() {
}

}
