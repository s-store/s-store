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

package edu.brown.api.results;

import org.json.JSONException;
import org.json.JSONObject;

import edu.brown.api.BenchmarkInterest;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.JSONUtil;

/**
 * JSON Results Printer
 * @author pavlo
 */
public class JSONResultsPrinter implements BenchmarkInterest {
   
    private final HStoreConf hstore_conf;
    private boolean stop = false;
    
    public JSONResultsPrinter(HStoreConf hstore_conf) {
        this.hstore_conf = hstore_conf;
    }
    
    @Override
    public String formatFinalResults(BenchmarkResults results) {
        if (this.stop) return (null);
        
        FinalResult fr = new FinalResult(results);
        JSONObject json = null;
        try {
            json = new JSONObject(fr.toJSONString());
            if (hstore_conf.client.output_clients == false) {
//                json.remove("CLIENTRESULTS");
//                json.remove("WKFRESULTS");
                for (String key : CollectionUtil.iterable(json.keys())) {
                    if (key.toLowerCase().startsWith("client")) {
                        json.remove(key);        
                    }
                } // FOR
            }
        } catch (JSONException ex) {
            throw new RuntimeException(ex);
        }
        return "<json>\n" + JSONUtil.format(json) + "\n</json>";
    }

    @Override
    public void benchmarkHasUpdated(BenchmarkResults currentResults) {
        // Nothing
    }

    @Override
    public void markEvictionStart() {
        // Nothing
    }

    @Override
    public void markEvictionStop() {
        // Nothing
    }

    @Override
    public void stop() {
        this.stop = true;
    }
}
