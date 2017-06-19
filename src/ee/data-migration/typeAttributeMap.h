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

#ifndef TYPE_ATTRIBUTE_MAP_H
#define TYPE_ATTRBUTE_MAP_H

#include <vector>
#include <map>
#include <memory>
#include <string>

#include "utils.h"
#include "attribute.h"
#include "boost/smart_ptr/shared_ptr.hpp"
#include "boost/smart_ptr/make_shared.hpp"

/** Map a given type (of an attribute) to the handler that will transform it to an internal representation and finally to any other desired format. */
class TypeAttributeMap {

  private:
    std::map<std::string,boost::shared_ptr<Attribute> > typeAttributeMap;
    boost::shared_ptr<Attribute> getAttribute(const std::string& type);

  public:
    TypeAttributeMap();
    ~TypeAttributeMap();
    void getAttributesFromTypesVector(std::vector<boost::shared_ptr<Attribute> > & attributes, const std::vector<std::string> & types);
    void getAttributesFromTypes(std::vector<boost::shared_ptr<Attribute> > & attributes, const char *types);
    void getSupportedTypes(std::vector<std::string> & supportedTypes);
};

#endif // TYPE_ATTRIBUTE_MAP_H
