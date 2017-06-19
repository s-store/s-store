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
package edu.brown.benchmark.seaflow.procedures;

import edu.brown.benchmark.seaflow.SeaflowConstants;
import edu.brown.benchmark.seaflow.SeaflowUtil;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class Steering extends VoltProcedure {

    public final SQLStmt getCurrentLocation = new SQLStmt(
            "SELECT c_lat, c_lon, c_month FROM cur_location_tbl WHERE c_id = 1;"
    );

    public final SQLStmt getTemperature = new SQLStmt(
            "SELECT a_temp, a_lat, a_lon FROM argo_tbl WHERE a_lat = ? AND a_lon = ? and a_month = ? and a_depth = 20;"
            );

    public final SQLStmt getTemperatureStrip = new SQLStmt(
            "SELECT a_temp, a_lat, a_lon FROM argo_tbl WHERE a_lat >= ? AND a_lat <= ? AND a_lon >= ? AND a_lon <= ? AND a_month = ? AND a_depth = 20;"
    );

    public final SQLStmt updateSteering = new SQLStmt(
            "UPDATE steering_tbl SET st_rotation = ? WHERE st_id = 1;"
            );

    public final SQLStmt getSteering = new SQLStmt(
            "SELECT st_rotation FROM steering_tbl WHERE st_id = 1;"
    );

    public VoltTable run() {

        voltQueueSQL(getCurrentLocation);
        VoltTable i[] = voltExecuteSQL();

        Double lat = i[0].fetchRow(0).getDouble("c_lat");
        Double lon = i[0].fetchRow(0).getDouble("c_lon");
        Integer month = new Integer( (int) i[0].fetchRow(0).getLong("c_month"));

//        Double a = SeaflowUtil.roundToHalf(lon);
//        Double b = SeaflowUtil.roundToHalf(lat);

        int k = 3;

        List<Double> mult = new ArrayList<>();
        List<Vector2D> vect = new ArrayList<>();

        voltQueueSQL(getTemperatureStrip, lon-k, lat+k, lon-k, lat+k, month);
        VoltTable v = voltExecuteSQL()[0];

        for(int j=0; j < v.getRowCount(); j++) {
            VoltTableRow row = v.fetchRow(j);
            Double thisLat = row.getDouble("a_lat");
            Double thisLon = row.getDouble("a_lon");
            Double thisTemp = row.getDouble("a_temp");
            if(row.wasNull())
            	continue;

            if(thisLon != lon && thisLat != lat) {
                vect.add( new Vector2D( 1 / (thisLon - lon), 1 / ( thisLat - lat)) );
                mult.add( thisTemp );
            }
        }

        
        Vector2D sum = new Vector2D(mult, vect);

        double tangent = Math.abs(sum.getY().doubleValue()) / Math.abs(sum.getX().doubleValue());
        double degrees = Math.toDegrees(Math.atan(tangent));
        
        if (sum.getX() >= 0 && sum.getY() >= 0) {
            degrees += 0.0;
        } else if (sum.getX() <= 0 && sum.getY() >= 0) {
            degrees += 90.0;
        } else if (sum.getX() <= 0 && sum.getY() <= 0) {
            degrees += 180.0;
        } else if(sum.getX() >= 0 && sum.getY() <= 0) {
            degrees += 270.0;
        }

        voltQueueSQL(updateSteering, degrees);
        voltQueueSQL(getSteering);

        VoltTable result[] = voltExecuteSQL();

        return result[1];

    }

    // import org.apache.commons.math3.geometry.euclidean.twod.Vector2D;
    public class Vector2D {
        private Double x;
        private Double y;

        public Vector2D(Double x, Double y) {
            this.x = x;
            this.y = y;
        }

        public Vector2D(Double a1, Vector2D u1, Double a2, Vector2D u2, Double a3, Vector2D u3, Double a4, Vector2D u4) {
            this.x = a1 * u1.x + a2 * u2.x + a3 * u3.x + a4 * u4.x;
            this.y = a1 * u1.y + a2 * u2.y + a3 * u3.y + a4 * u4.y;
        }

        public Vector2D(List<Double> mult, List<Vector2D> vect) {
            this.x = new Double(0);
            this.y = new Double(0);

            for (int i = 0; i < mult.size(); i++) {
                this.x += mult.get(i) * vect.get(i).x;
                this.y += mult.get(i) * vect.get(i).y;
            }
        }

        public Double getX() {
            return this.x;
        }

        public Double getY() {
            return this.y;
        }

    }
}
