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

#ifndef ENDIANNESS_H
#define ENDIANNESS_H

#include <boost/detail/endian.hpp>
#include <algorithm>
/* endian.h is not used in this file but other file which include endianness.h expect it */
#include <endian.h> 

namespace endianness {

/**
   it swaps the bytes in place!!!
 */
template <typename T>
inline void swap_bytes(T& value)
{
    // could static assert that T is a POD - plain old data type
    char& raw = reinterpret_cast<char&>(value);
    std::reverse(&raw, &raw + sizeof(T));
}

#if defined(BOOST_LITTLE_ENDIAN)
    //host_endian = little_endian
    template<class T>
    inline void fromBigEndianToHost(T& value) {
	swap_bytes(value);
    }
    template<class T>
    inline void fromHostToBigEndian(T& value) {
	swap_bytes(value);
    }
#elif defined(BOOST_big_endian)
    //host_endian = big_endian
    // this system works in the big endian order of bytes
    template<class T> inline void fromBigEndianToHost(T& value) {}
    template<class T> inline void fromHostToBigEndian(T& value) {}

#else
#error "unable to determine system endianness"
#endif

} // namespace endianness

#endif // #define ENDIANNESS_H
