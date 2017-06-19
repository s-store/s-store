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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.*;


public class WordGenerator {

    private int m_size;
    private List<String> m_words;
    
    private int m_position;
    
    
    public WordGenerator(String strFileName) {
       
        m_size = 0;
        m_position = 0;
        m_words= new ArrayList<String>();
        
        // get the content of file
        FileInputStream fis;
        try {
            
            fis = new FileInputStream(strFileName);
            Scanner scanner = new Scanner(fis);
            
            while(scanner.hasNextLine()){
                String line_msg = scanner.nextLine();
                
                // current we use delimiter to split line into words 
                StringTokenizer st = new StringTokenizer(line_msg, " ,;.");
                while (st.hasMoreTokens()) {
                    m_words.add(st.nextToken().toLowerCase());
                }
            }
            
            scanner.close();

            // get the size of the word array
            m_size = m_words.size();
            
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    } 
    
    public boolean isEmpty()
    {
        if( m_size == 0 )
            return true;
        else
            return false;
    }
    
    public void reset()
    {
        m_position = 0;
    }

    public boolean hasMoreWords()
    {
        if(m_position >= m_size)
            return false;
        else
            return true;
    }
    
    public String nextWord()
    {
        String word = m_words.get(m_position);
        m_position++;
        
        return word;
    }
    
}
