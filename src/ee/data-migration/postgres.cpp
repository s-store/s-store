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

#include <stdint.h>
#include <iostream>
#include <endian.h>
#include <inttypes.h>
#include "postgres.h"
#include <stdlib.h>

#define __STDC_FORMAT_MACROS

static const char BinarySignature[11] = {'P','G','C','O','P','Y','\n','\377','\r','\n','\0'};

uint16_t Postgres::readColNumber(FILE *fp) {
    // read 2 bytes representing number of columns stored in each line
    // we have to read the colNumber and check if it is the end of the file
    // -1 represents the end of the binary data
    uint16_t colNumber;
    if(fread(&colNumber,2,1,fp) != 1) exit(1);
    colNumber = be16toh(colNumber);
    return colNumber;
}

uint16_t Postgres::readColNumberBuffer(Buffer * buffer) {
    // read 2 bytes representing number of columns stored in each line
    // fseek(this->fp,2,SEEK_CUR);
    // we have to read the colNumber and check if it is the end of the file
    // -1 represents the end of the binary data
    uint16_t colNumber;
    BufferRead(&colNumber,2,1,buffer);
    colNumber = be16toh(colNumber);
    return colNumber;
}

/* read first 19 bytes */
char* Postgres::readHeader(FILE *fp) {
    unsigned int headerSize = 19;
    char *buffer = new char[headerSize];
    if(fread(buffer,headerSize,1,fp) != 1) exit(1);
    return buffer;
}

void Postgres::writeHeader(FILE *fp) {
    // std::cout << "Write PostgreSQL header\n";
    // write 19 bytes

    // 11 bytes for the signature
    fwrite(&BinarySignature,11,1,fp);

    // 4 bytes for flags
    uint32_t flagsField = 0;
    uint32_t flagsFieldFormatted = htobe32(flagsField);
    fwrite(&flagsFieldFormatted,4,1,fp);

    // 4 bytes for the length of the header extension area
    int32_t headerExtensionLen = 0;
    int32_t headerExtensionLenFormatted = htobe32(headerExtensionLen);
    fwrite(&headerExtensionLenFormatted,4,1,fp);
}

/** skip first 19 bytes */
void Postgres::skipHeader(FILE *fp) {
    fseek(fp,19,SEEK_CUR);
}

/* Each tuple begins with a 16-bit integer count of the number of fields in the tuple. */
void Postgres::writeColNumber(FILE *fp, uint16_t colNumber) {
    uint16_t colNumberPostgres = htobe16(colNumber);
    fwrite(&colNumberPostgres,2,1,fp);
}

/* The file trailer consists of a 16-bit integer word containing -1.
       This is easily distinguished from a tuple's field-count word. */
void Postgres::writeFileTrailer(FILE *fp) {
    uint16_t trailer = -1;
    uint16_t trailerPostgres = htobe16(trailer);
    fwrite(&trailerPostgres,2,1,fp);
}
