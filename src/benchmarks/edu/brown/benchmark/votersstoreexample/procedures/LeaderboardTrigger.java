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
package edu.brown.benchmark.votersstoreexample.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

@ProcInfo (
	partitionInfo = "leaderboard.contestant_number:1",
    singlePartition = true
)

//example of an Execution Engine Trigger. Because this trigger is on a window, it fires every time the window slides.
//If this trigger were on a stream, it would fire with every tuple insert.
public class LeaderboardTrigger extends VoltTrigger {

	//set the stream or window that this trigger is attached to
    @Override
    protected String toSetStreamName() {
        return "trending_leaderboard";
    }
    
    //list all SQL stmts that need to be run every time the trigger is fired
    public final SQLStmt deleteLeaderboard = new SQLStmt(
            "DELETE FROM leaderboard;"
    );

    //when determining what rows to include in the leaderboard table, we need to join against the contestants table to see which ones are still around (we cannot delete from the window itself)
    public final SQLStmt updateLeaderboard = new SQLStmt(
            "INSERT INTO leaderboard (contestant_number, num_votes) SELECT trending_leaderboard.contestant_number, count(*) FROM trending_leaderboard, contestants WHERE trending_leaderboard.contestant_number = contestants.contestant_number GROUP BY trending_leaderboard.contestant_number;"
    );

}
