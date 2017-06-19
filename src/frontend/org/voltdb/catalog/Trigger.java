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

/* WARNING: THIS FILE IS AUTO-GENERATED
            DO NOT MODIFY THIS SOURCE
            ALL CHANGES MUST BE MADE IN THE CATALOG GENERATOR */
/***************************************************************************
 *  Copyright (C) 2017 by S-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Portland State University                                              *
 *                                                                         *
 *  Author:  The S-Store Team (sstore.cs.brown.edu)                        *
 *                                                                         *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package org.voltdb.catalog;

/**
 * Trigger objects on a table, with a statement attached
 */
public class Trigger extends CatalogType {

    int m_id;
    int m_triggerType;
    boolean m_forEach;
    CatalogMap<Statement> m_statements;

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        m_fields.put("id", m_id);
        m_fields.put("sourceTable", null);
        m_fields.put("triggerType", m_triggerType);
        m_fields.put("forEach", m_forEach);
        m_statements = new CatalogMap<Statement>(catalog, this, path + "/" + "statements", Statement.class);
        m_childCollections.put("statements", m_statements);
    }

    public void update() {
        m_id = (Integer) m_fields.get("id");
        m_triggerType = (Integer) m_fields.get("triggerType");
        m_forEach = (Boolean) m_fields.get("forEach");
    }

    /** GETTER: Unique identifier for this Trigger. Allows for faster look-ups */
    public int getId() {
        return m_id;
    }

    /** GETTER: Table on which the trigger is placed. */
    public Table getSourcetable() {
        Object o = getField("sourceTable");
        if (o instanceof UnresolvedInfo) {
            UnresolvedInfo ui = (UnresolvedInfo) o;
            Table retval = (Table) m_catalog.getItemForRef(ui.path);
            assert(retval != null);
            m_fields.put("sourceTable", retval);
            return retval;
        }
        return (Table) o;
    }

    /** GETTER: Insert / Update / Delete */
    public int getTriggertype() {
        return m_triggerType;
    }

    /** GETTER: Is this for each tuple, or each statement */
    public boolean getForeach() {
        return m_forEach;
    }

    /** GETTER: What to execute when this trigger is activated"			 */
    public CatalogMap<Statement> getStatements() {
        return m_statements;
    }

    /** SETTER: Unique identifier for this Trigger. Allows for faster look-ups */
    public void setId(int value) {
        m_id = value; m_fields.put("id", value);
    }

    /** SETTER: Table on which the trigger is placed. */
    public void setSourcetable(Table value) {
        m_fields.put("sourceTable", value);
    }

    /** SETTER: Insert / Update / Delete */
    public void setTriggertype(int value) {
        m_triggerType = value; m_fields.put("triggerType", value);
    }

    /** SETTER: Is this for each tuple, or each statement */
    public void setForeach(boolean value) {
        m_forEach = value; m_fields.put("forEach", value);
    }

}
