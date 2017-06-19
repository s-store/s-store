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

import java.util.*;

import edu.brown.stream.PhoneCallGenerator.PhoneCall;

public class VoteGenerator {
    
    private ArrayList<PhoneCall> m_votes;
    
    private int m_position = 0;

    // orderedcall.ser or disorderedcall.ser
    public VoteGenerator(String strFileName) {
        m_position = 0;
        
        m_votes = PhoneCallAccessor.getPhoneCallsFromFile(strFileName);
    }
    
    public boolean isEmpty()
    {
        return m_votes.isEmpty();
    }
    
    public int size()
    {
        return this.m_votes.size();
    }

    public void reset()
    {
        m_position = 0;
    }
    
    public boolean hasMoreVotes()
    {
        int size = this.m_votes.size();
        //System.out.println("size and pos: " + size +"-"+m_position);
        if(m_position >= size)
            return false;
        else
            return true;
    }
    
    public synchronized PhoneCall nextVote()
    {
        if(hasMoreVotes()==false)
            return null;
        
        //System.out.println("get call at position: " + m_position);
        PhoneCall call = m_votes.get(m_position);
        m_position++;
        
        return call;
    }
}
