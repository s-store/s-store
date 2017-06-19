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

#ifndef VALUEFACTORY_HPP_
#define VALUEFACTORY_HPP_

#include "common/NValue.hpp"

namespace voltdb {
class ValueFactory {
public:

	static inline NValue getBooleanValue(bool value) {
		return value ? NValue::getTrue() : NValue::getFalse();
	}

    static inline NValue getTinyIntValue(int8_t value) {
        return NValue::getTinyIntValue(value);
    }

    static inline NValue getSmallIntValue(int16_t value) {
        return NValue::getSmallIntValue(value);
    }

    static inline NValue getIntegerValue(int32_t value) {
        return NValue::getIntegerValue(value);
    }

    static inline NValue getBigIntValue(int64_t value) {
        return NValue::getBigIntValue(value);
    }

    static inline NValue getTimestampValue(int64_t value) {
        return NValue::getTimestampValue(value);
    }

    static inline NValue getDoubleValue(double value) {
        return NValue::getDoubleValue(value);
    }

    static inline NValue getStringValue(std::string value) {
        return NValue::getStringValue(value);
    }

    static inline NValue getNullStringValue() {
        return NValue::getNullStringValue();
    }

    static inline NValue getBinaryValue(std::string value) {
        // uses hex encoding
        return NValue::getBinaryValue(value);
    }

    static inline NValue getBinaryValue(unsigned char* value, int32_t len) {
        return NValue::getBinaryValue(value, len);
    }

    static inline NValue getNullBinaryValue() {
        return NValue::getNullBinaryValue();
    }

    /** Returns valuetype = VALUE_TYPE_NULL. Careful with this! */
    static inline NValue getNullValue() {
        return NValue::getNullValue();
    }

    static inline NValue getDecimalValueFromString(const std::string &txt) {
        return NValue::getDecimalValueFromString(txt);
    }

    static inline NValue getAddressValue(void *address) {
        return NValue::getAddressValue(address);
    }

    // What follows exists for test only!

    static inline NValue castAsBigInt(NValue value) {
        return value.castAsBigInt();
    }

    static inline NValue castAsInteger(NValue value) {
        return value.castAsInteger();
    }

    static inline NValue castAsSmallInt(NValue value) {
        return value.castAsSmallInt();
    }

    static inline NValue castAsTinyInt(NValue value) {
        return value.castAsTinyInt();
    }

    static inline NValue castAsDouble(NValue value) {
        return value.castAsDouble();
    }

    static inline NValue castAsDecimal(NValue value) {
        return value.castAsDecimal();
    }

    static inline NValue castAsString(NValue value) {
        return value.castAsString();
    }
};
}
#endif /* VALUEFACTORY_HPP_ */
