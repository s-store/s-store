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

#ifndef BUFFER_H
#define BUFFER_H

#include <stdio.h>
// imitate the buffer for copy.c from PostgreSQL

// #define RAW_BUF_SIZE 65536 // this is equal to the default size of the pipe

typedef struct Buffer {
    char * raw_buf; /* this is the buffer */
    size_t size; /* size of the buffer */
    size_t raw_buf_index; /* next byte to process */
    size_t raw_buf_len; /* total # of bytes stored */
    FILE * file; /* file to be read from */

} Buffer;

void BufferNew(Buffer* buffer, FILE * file, size_t size);
void BufferDispose(Buffer* buffer);

/* imitate the fread: buffer read
   write the bytes to the address
   read (size*1) bytes
   count has to be 1
   from the buffer
*/
size_t BufferRead(void* address, size_t size, size_t count, Buffer* buffer);

#endif // BUFFER_H
