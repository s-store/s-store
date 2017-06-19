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

#include "StringRef.h"

#include "Pool.hpp"

using namespace voltdb;
using namespace std;

StringRef*
StringRef::create(size_t size, Pool* dataPool)
{
    StringRef* retval;
    if (dataPool != NULL)
    {
        retval =
            new(dataPool->allocate(sizeof(StringRef))) StringRef(size, dataPool);
    }
    else
    {
        retval = new StringRef(size);
    }
    return retval;
}

void
StringRef::destroy(StringRef* sref)
{
    delete sref;
}

StringRef::StringRef(size_t size)
{
    m_size = size + sizeof(StringRef*);
    m_tempPool = false;
    m_stringPtr = new char[m_size];
    setBackPtr();
}

StringRef::StringRef(std::size_t size, Pool* dataPool)
{
    m_tempPool = true;
    m_stringPtr =
        reinterpret_cast<char*>(dataPool->allocate(size + sizeof(StringRef*)));
    setBackPtr();
}

StringRef::~StringRef()
{
    if (!m_tempPool)
    {
        delete[] m_stringPtr;
    }
}

char*
StringRef::get()
{
    return m_stringPtr + sizeof(StringRef*);
}

const char*
StringRef::get() const
{
    return m_stringPtr + sizeof(StringRef*);
}

void
StringRef::updateStringLocation(void* location)
{
    m_stringPtr = reinterpret_cast<char*>(location);
}

void
StringRef::setBackPtr()
{
    StringRef** backptr = reinterpret_cast<StringRef**>(m_stringPtr);
    *backptr = this;
}
