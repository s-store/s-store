/**
 * 
 */
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

package edu.brown.catalog;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.types.IndexType;
import org.voltdb.types.ConstraintType;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.NotImplementedException;

import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;

/**
 * @author pavlo
 */
public class CatalogExporter implements JSONSerializable {

    private final Catalog catalog;

    /**
     * Constructor
     * 
     * @param catalog
     */
    public CatalogExporter(Catalog catalog) {
        this.catalog = catalog;
    }

    /*
     * (non-Javadoc)
     * @see edu.brown.utils.JSONSerializable#toJSON(org.json.JSONStringer)
     */
    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        Database catalog_db = CatalogUtil.getDatabase(this.catalog);
        // Procedures
        stringer.key("PROCEDURES").object();
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            if (catalog_proc.getSystemproc())
                continue;
            stringer.key(catalog_proc.getName()).object();
            for (Statement catalog_stmt : catalog_proc.getStatements()) {
                stringer.key(catalog_stmt.getName()).value(catalog_stmt.getSqltext());
            } // FOR
            stringer.endObject();
        } // FOR
        stringer.endObject();

        // Tables
        stringer.key("TABLES").object();
        for (Table catalog_tbl : catalog_db.getTables()) {
            stringer.key(catalog_tbl.getName()).object();
            // Column
            stringer.key("COLUMNS").object();
            for (Column catalog_col : CatalogUtil.getSortedCatalogItems(catalog_tbl.getColumns(), "index")) {
                stringer.key(catalog_col.getName()).object();
                stringer.key("TYPE").value(VoltType.get(catalog_col.getType()).name());
                stringer.key("SIZE").value(catalog_col.getSize());
                stringer.key("NULLABLE").value(catalog_col.getNullable());
                stringer.key("RELATIVE_INDEX").value(catalog_col.getRelativeIndex());
                stringer.endObject();

            } // FOR
            stringer.endObject();
            // Indexes
            stringer.key("INDEXES").object();
            for (Index catalog_index : catalog_tbl.getIndexes()) {
                stringer.key(catalog_index.getName()).object();
                stringer.key("TYPE").value(IndexType.get(catalog_index.getType()).name());
                stringer.key("RELATIVE_INDEX").value(catalog_index.getRelativeIndex());
                stringer.key("COLUMNS").object();
                for (ColumnRef index_colume_ref : catalog_index.getColumns()) {
                    stringer.key(index_colume_ref.getName()).object();
                    String fullName = index_colume_ref.getColumn().fullName();
                    stringer.key("COLUMN_REF").value(fullName.split("\\.")[1]);
                    stringer.key("TABLE_REF").value(fullName.split("\\.")[0]);
                    stringer.endObject();
                } // FOR
                stringer.endObject();
                stringer.endObject();
            } // FOR
            stringer.endObject();
            // Constraints
            stringer.key("CONSTRAINTS").object();
            for (Constraint catalog_constraint : catalog_tbl.getConstraints()) {
                stringer.key(catalog_constraint.getName()).object();
                stringer.key("TYPE").value(ConstraintType.get(catalog_constraint.getType()).name());
                if (ConstraintType.get(catalog_constraint.getType()) == ConstraintType.FOREIGN_KEY) {
                    stringer.key("FOREIGN_TABLE").value(catalog_constraint.getForeignkeytable().getName());
                    stringer.key("FOREIGN_COLUMNS").object();
                    for (ColumnRef index_colume_ref : catalog_constraint.getForeignkeycols()) {
                        stringer.key(index_colume_ref.getName()).object();
                        stringer.key("COLUMN_REF").value(index_colume_ref.getColumn().getName());
                        stringer.endObject();
                    } // FOR
                    stringer.endObject();
                }
                stringer.key("RELATIVE_INDEX").value(catalog_constraint.getRelativeIndex());
                stringer.endObject();
            } // FOR
            stringer.endObject();

            stringer.endObject();
        } // FOR
        stringer.endObject();
        // Schema
        stringer.key("SCHEMA").object();
        stringer.key("DDL").value(Encoder.hexDecodeToString(catalog_db.getSchema()));
        stringer.endObject();
    }

    /*
     * (non-Javadoc)
     * @see edu.brown.utils.JSONSerializable#fromJSON(org.json.JSONObject,
     * org.voltdb.catalog.Database)
     */
    @Override
    public void fromJSON(JSONObject jsonObject, Database catalogDb) throws JSONException {
        throw new NotImplementedException("Cannot import JSON catalog");
    }

    /*
     * (non-Javadoc)
     * @see edu.brown.utils.JSONSerializable#load(java.lang.String,
     * org.voltdb.catalog.Database)
     */
    @Override
    public void load(File inputPath, Database catalogDb) throws IOException {
        throw new NotImplementedException("Cannot import JSON catalog");
    }

    /*
     * (non-Javadoc)
     * @see edu.brown.utils.JSONSerializable#save(java.lang.String)
     */
    @Override
    public void save(File outputPath) throws IOException {
        JSONUtil.save(this, outputPath);
    }

    /*
     * (non-Javadoc)
     * @see org.json.JSONString#toJSONString()
     */
    @Override
    public String toJSONString() {
        return (JSONUtil.toJSONString(this));
    }

    /**
     * @param vargs
     * @throws Exception
     */
    public static void main(String[] vargs) throws Exception {
//        for (int i = 0; i < vargs.length; i++) {
//            System.out.println(i + vargs[i]);
//        }
        Properties prop = new Properties();
        InputStream inputStream = CatalogExporter.class.getClassLoader().getResourceAsStream("log4j.properties");
        prop.load(inputStream);
        String benchmark = prop.getProperty("log4j.logger.edu.brown.benchmark");
        System.out.println(benchmark);
        String[] arfs = new String[2];
        arfs[0] = "catalog.jar=./voter.jar";
        arfs[1] = "catalog.output=./asdasdas.json";
        ArgumentsParser args = ArgumentsParser.load(arfs);
        args.require(ArgumentsParser.PARAM_CATALOG, ArgumentsParser.PARAM_CATALOG_OUTPUT);

        Catalog catalog = args.catalog;
        File output = args.getFileParam(ArgumentsParser.PARAM_CATALOG_OUTPUT);
        new CatalogExporter(catalog).save(output);
    }

}
