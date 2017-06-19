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

import java.io.*;
import java.util.ArrayList;

import edu.brown.stream.PhoneCallGenerator.PhoneCall;

public class PhoneCallAccessor {

    public PhoneCallAccessor() {
        
    }
    
    static public ArrayList<PhoneCall> getPhoneCallsFromFile(String filename)
    {
        ArrayList<PhoneCall> list = new ArrayList<PhoneCall> ();
        
        try{    
            FileInputStream fis = new FileInputStream(filename);
            ObjectInputStream ois = new ObjectInputStream(fis);
            list =  (ArrayList<PhoneCall>) ois.readObject();
            ois.close();
            
        }
        catch (Exception e)
        {
            //System.out.println("File Not Found");
            System.out.println(e.getMessage());
        } 
        
        return list;
    }
    
    static public void savePhoneCallsToFile(ArrayList<PhoneCall> calls, String filename) throws FileNotFoundException, IOException
    {
        ObjectOutputStream out = new ObjectOutputStream (new FileOutputStream(filename));
        out.writeObject(calls);
        out.flush();
        out.close();
    }

}
