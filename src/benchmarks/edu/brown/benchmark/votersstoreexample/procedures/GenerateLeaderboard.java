/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
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
//
// Accepts a vote, enforcing business logic: make sure the vote is for a valid
// contestant and that the VoterSStoreExample (phone number of the caller) is not above the
// number of allowed votes.
//

package edu.brown.benchmark.votersstoreexample.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.votersstoreexample.VoterSStoreExampleConstants;

@ProcInfo (
	//you must set a partition number, which will be passed as the "part_id" parameter. If you want partitionNum to be the same partition that you triggered from,
	//you can set partitionNum = -1.  If you are only running on a single node, then you will not need to use partitionNum.  
	partitionNum = 0, 
    singlePartition = true
)
public class GenerateLeaderboard extends VoltProcedure {
	
	protected void toSetTriggerTableName()
	{
		addTriggerTable("proc_one_out");
	}
	
	//retrieve the latest vote from the input stream
	public final SQLStmt getInStreamStmt = new SQLStmt(
	   "SELECT vote_id, phone_number, state, contestant_number, created, ts FROM proc_one_out ORDER BY vote_id LIMIT 1;"
    );
	
	// Insert the vote into a window
    public final SQLStmt trendingLeaderboardStmt = new SQLStmt(
	   "INSERT INTO trending_leaderboard (vote_id, phone_number, state, contestant_number, created, ts) VALUES (?,?,?,?,?,?);"
    );
	
    //garbage collection
	public final SQLStmt deleteProcOneOutStmt = new SQLStmt(
		"DELETE FROM proc_one_out WHERE vote_id = ?;"
	);
	    
	//retrieve the "number of votes" state
    public final SQLStmt checkNumVotesStmt = new SQLStmt(
		"SELECT cnt FROM votes_count WHERE row_id = 1;"
    );
    
    //update the "number of votes" state
    public final SQLStmt updateNumVotesStmt = new SQLStmt(
		"UPDATE votes_count SET cnt = ? WHERE row_id = 1;"
    );
    
    //insert into output stream to trigger DeleteContestant
    public final SQLStmt insertProcTwoOutStmt = new SQLStmt(
    	"INSERT INTO proc_two_out VALUES (?);"	
    );
    
    //find the contestant with the fewest votes
    public final SQLStmt findLowestContestant = new SQLStmt(
		"SELECT * FROM v_votes_by_contestant ORDER BY num_votes ASC LIMIT 1;"
    );
    
    public long run(int part_id) {	
        voltQueueSQL(getInStreamStmt);
        voltQueueSQL(checkNumVotesStmt);
        VoltTable validation[] = voltExecuteSQL();
        
        //check to make sure we recieved a response from both queries
        if(validation.length < 2 || validation[0].getRowCount() <= 0)
        	return VoterSStoreExampleConstants.ERR_NO_VOTE_FOUND;
        
        long voteId = validation[0].fetchRow(0).getLong("vote_id");
        long phoneNumber = validation[0].fetchRow(0).getLong("phone_number");
        String state = validation[0].fetchRow(0).getString("state");
        int contestantNumber = (int)validation[0].fetchRow(0).getLong("contestant_number");
        TimestampType created = validation[0].fetchRow(0).getTimestampAsTimestamp("created");
        long ts = validation[0].fetchRow(0).getLong("ts");
        int numVotes = (int)(validation[1].fetchRow(0).getLong(0)) + 1;
        
        //insert into the window and do garbage collection
        voltQueueSQL(trendingLeaderboardStmt, voteId, phoneNumber, state, contestantNumber, created, ts);
        voltQueueSQL(deleteProcOneOutStmt, voteId); //for procedure triggers, tuple cleanup is NOT automatic.  You will need to manually delete consumed tuples from the stream
        validation = voltExecuteSQL();
       
        voltQueueSQL(updateNumVotesStmt, (numVotes % VoterSStoreExampleConstants.VOTE_THRESHOLD));
        
        // Set the return value to 0: successful vote
        if(numVotes == VoterSStoreExampleConstants.VOTE_THRESHOLD)
        {
        	voltQueueSQL(findLowestContestant);
        	validation = voltExecuteSQL();
        	if(validation[0].getRowCount() > 0)
        	{
        		int lowestContestant = (int)validation[0].fetchRow(0).getLong(0);
        		voltQueueSQL(insertProcTwoOutStmt, lowestContestant); //trigger the next stored procedure
        	}
        	else
        		return VoterSStoreExampleConstants.ERR_NOT_ENOUGH_CONTESTANTS;
        }
        voltExecuteSQL(true);
        
        return VoterSStoreExampleConstants.WINDOW_SUCCESSFUL;
    }
}