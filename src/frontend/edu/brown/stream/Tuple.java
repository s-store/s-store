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
package edu.brown.stream;

import java.util.Map;
import java.util.HashMap;

import org.json.JSONException;
import org.json.JSONObject;

public class Tuple {
    
    private Map<String, Object> m_values;

    private String m_value;

    public Tuple()
    {
        m_values = new HashMap<String, Object>();
    }
    
    public Tuple(String value) {
        m_values = new HashMap<String, Object>();
        
        this.m_value = value;
    }
    
    public String getValue() {
        return m_value;
    }
    
    public void addField(String fieldname, Object fieldvalue)
    {
        m_values.put(fieldname, fieldvalue);
    }
    
    public Object getField(String fieldname)
    {
        return m_values.get(fieldname);
    }
    
    public int getFieldLength()
    {
        return m_values.size();
    }
    
    public void reset()
    {
        m_values.clear();
    }
    
    public JSONObject toJSONObject()
    {
        JSONObject jsonTuple = new JSONObject();
        try {
            for (Map.Entry<String, Object> entry : m_values.entrySet()) {
                String fieldname = entry.getKey();
                Object fieldvalue = entry.getValue();
                jsonTuple.put(fieldname, fieldvalue);
            }
        } catch (JSONException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
       
        return jsonTuple;
    }
    
    public void fromJSONObject(JSONObject jsonTuple)
    {
        this.reset();
        
        try {
            String[] fieldnames = JSONObject.getNames(jsonTuple);
            for(int index=0; index<fieldnames.length; index++)
            {
                String fieldname = fieldnames[index];
                Object fieldvalue = jsonTuple.get(fieldname);
                this.addField(fieldname, fieldvalue);
            }
        } catch (JSONException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public String toJSONString()
    {
        JSONObject jsonTuple = this.toJSONObject();

        return jsonTuple.toString();
    }
    
    public void fromJSONString(String strTuple)
    {
        try {
            JSONObject jsonTuple = new JSONObject(strTuple);
            
            this.fromJSONObject(jsonTuple);
            
        } catch (JSONException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }
    
}
