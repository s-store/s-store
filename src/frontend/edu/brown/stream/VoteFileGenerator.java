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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.System;
import java.util.ArrayList;

import edu.brown.stream.PhoneCallGenerator.PhoneCall;

public class VoteFileGenerator {
    
    // Initialize some common constants and variables
    public static final String CONTESTANT_NAMES_CSV = "Jann Arden,Micah Barnes,Justin Bieber,Jim Bryson,Michael Buble," +
			"Leonard Cohen,Celine Dion,Nelly Furtado,Adam Gontier,Emily Haines," +
			"Avril Lavigne,Ashley Leggat,Eileen McGann,Sarah McLachlan,Joni Mitchell," +
			"Mae Moore,Alanis Morissette,Emilie Mover,Anne Murray,Sam Roberts," +
			"Serena Ryder,Tamara Sandor,Nicholas Scribner,Shania Twain,Neil Young";
    public static final int NUM_CONTESTANTS = 6; 

    public VoteFileGenerator() {
        // TODO Auto-generated constructor stub
    }

    public static void main(String[] vargs) throws Exception {

        for(String argument : vargs){
            System.out.println(argument);
        }  
        
        AnotherArgumentsParser args = AnotherArgumentsParser.load( vargs , false);
        
        int shuffle_method = 0; // default
        if (args.hasParam(AnotherArgumentsParser.PARAM_SHUFFLE_METHOD)) {
            shuffle_method = args.getIntParam(AnotherArgumentsParser.PARAM_SHUFFLE_METHOD);
        }
        
        Long shuffle_size = 1000000l;
        if (args.hasParam(AnotherArgumentsParser.PARAM_SHUFFLE_SIZE)) {
            shuffle_size = args.getLongParam(AnotherArgumentsParser.PARAM_SHUFFLE_SIZE);
        }
        
        System.out.println("Shuffle method: " + shuffle_method);
        System.out.println("Shuffle size: " + shuffle_size);
        

        // Phone number generator
        int numContestants = getScaledNumContestants(1);
        PhoneCallGenerator switchboard = new PhoneCallGenerator(1, numContestants, shuffle_size);
        
        // generate the ordered and disordered votes based on parameters
        switchboard.generateVotes();
        
        String filename = "votes-o-" + String.valueOf(shuffle_size) + "-" + switchboard.getTimeLog();
        generateTxtFile(filename);
        filename = "votes-d-" + String.valueOf(shuffle_size) + "-" + switchboard.getTimeLog();
        generateTxtFile(filename);
        
//        // test we have save the correct thing.
//        System.out.println("Testing PhoneCallAccessor.getPhoneCallsFromFile method - orderedcall.ser ... \n");
//        String filename = "orderedcall.ser";
//        ArrayList<PhoneCall> list = PhoneCallAccessor.getPhoneCallsFromFile(filename);
//        
//        for (PhoneCall call : list) 
//            call.debug();
//
//        System.out.println("Testing PhoneCallAccessor.getPhoneCallsFromFile method - disorderedcall.ser ... \n");
//        filename = "disorderedcall.ser";
//        list = PhoneCallAccessor.getPhoneCallsFromFile(filename);
//      
//        for (PhoneCall call : list) 
//            call.debug();
//        
//        System.out.println("Testing class VoteGenerator method with disorderedcall.ser ... ");
//        VoteGenerator vg = new VoteGenerator(filename);
//        
//        PhoneCall current_call = vg.nextVote();
//        while(current_call !=null )
//        {
//            current_call.debug();
//            current_call = vg.nextVote();
//        }
//        
//        while(vg.hasMoreVotes()==true)
//        {
//            PhoneCall current_call = vg.nextVote();
//            current_call.debug();
//        }
        
    }
    
    public static void generateTxtFile(String name) throws IOException
    {
        String filename = name + ".txt";
        BufferedWriter out;
        out = new BufferedWriter(new FileWriter(filename,true));
        
//        File file=new File(filename);
//        FileWriter fw = new FileWriter(file,true);
        //System.out.println(filename);
        
        filename = name + ".ser";
        //System.out.println(filename);
        VoteGenerator vg = new VoteGenerator(filename);
        //System.out.println(filename);
        
        PhoneCall current_call = vg.nextVote();
        while(current_call !=null )
        {
            //current_call.debug();
            //fw.append(current_call.getString());
            out.write(current_call.getString());
            
            current_call = vg.nextVote();
        }
        
        out.close();
    }

    public static int getScaledNumContestants(double scaleFactor) {
        int min_contestants = 1;
        int max_contestants = VoteFileGenerator.CONTESTANT_NAMES_CSV.split(",").length;
        
        int num_contestants = (int)Math.round(VoteFileGenerator.NUM_CONTESTANTS * scaleFactor);
        if (num_contestants < min_contestants) num_contestants = min_contestants;
        if (num_contestants > max_contestants) num_contestants = max_contestants;
        
        return (num_contestants);
    }

}
