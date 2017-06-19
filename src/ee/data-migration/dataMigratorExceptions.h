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

#ifndef DATA_MIGRATOR_EXCEPTIONS
#define DATA_MIGRATOR_EXCEPTIONS

#include <string>
#include <exception>
#include <stdexcept>

/**
 * Throw when something went wrong with the migration/export/loading process.
 */
class DataMigratorException : public std::exception {
  public:
    DataMigratorException(const char* m) : msg(m) {}
    DataMigratorException(std::string & m) : msg(m) {}
    ~DataMigratorException() throw() {}
    const char* what() const throw() {
        return msg.c_str();
    }

  protected:
    /** the message about what went wrong */
    std::string msg;
};

/**
 * The data migrator in binary format is highly dependent on what data types are supported.
 * If a given data type is not supported by the data migrator, then throw the exception.
 * This type is not supported at all (for all databases).
 */
class TypeAttributeMapException : public DataMigratorException {
  public:
    TypeAttributeMapException(const char* m) : DataMigratorException(m) {}
    TypeAttributeMapException(std::string & m) : DataMigratorException(m) {}
    ~TypeAttributeMapException() throw() {}
    const char* what() const throw() {
        return msg.c_str();
    }
};

/**
 * This type is not supported for a database.  
 */
class DataMigratorTypeNotSupported : public DataMigratorException {
  public:
    DataMigratorTypeNotSupported(const char* m) : DataMigratorException(m) {}
    DataMigratorTypeNotSupported(std::string & m) : DataMigratorException(m) {}
    ~DataMigratorTypeNotSupported() throw() {}
    const char* what() const throw() {
        return msg.c_str();
    }
};

/**
 * Throw when whenever a data migration function is not implemented but only we have a scaffolding.
 */
class DataMigratorNotImplementedException : public DataMigratorException
{
  public:
    DataMigratorNotImplementedException() : DataMigratorException("Function not implemented yet!") {}
    DataMigratorNotImplementedException(const char* m) : DataMigratorException(m) {}
    DataMigratorNotImplementedException(std::string & m) : DataMigratorException(m) {}
    ~DataMigratorNotImplementedException() throw() {}
    const char* what() const throw() {
        return msg.c_str();
    }
};

#endif // DATA_MIGRATOR_EXCEPTIONS

