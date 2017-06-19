/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include <cstdlib>
#include <ctime>
#include "harness.h"
#include "common/common.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "common/ValuePeeker.hpp"
#include "common/debuglog.h"
#include "common/TupleSchema.h"
#include "common/tabletuple.h"
#include "common/types.h"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/persistenttable.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "storage/tableutil.h"
#include "streaming/TimeWindow.h"
#include "execution/VoltDBEngine.h"
#include <sys/time.h>

using std::string;
using std::vector;
using namespace voltdb;

#define NUM_OF_COLUMNS 5
#define NUM_OF_TUPLES 3
#define WINDOW_SIZE 3
#define SLIDE_SIZE 2
#define TS_COL 0

#define WS_COL 1
#define WE_COL 2
#define USEC 0.000001

voltdb::ValueType COLUMN_TYPES[NUM_OF_COLUMNS] = { 
	voltdb::VALUE_TYPE_INTEGER,
	voltdb::VALUE_TYPE_INTEGER,
	voltdb::VALUE_TYPE_INTEGER,
	voltdb::VALUE_TYPE_INTEGER,
	voltdb::VALUE_TYPE_SMALLINT};
int32_t COLUMN_SIZES[NUM_OF_COLUMNS] = { 
	NValue::getTupleStorageSize(voltdb::VALUE_TYPE_INTEGER),
	NValue::getTupleStorageSize(voltdb::VALUE_TYPE_INTEGER),                           
	NValue::getTupleStorageSize(voltdb::VALUE_TYPE_INTEGER), 
	NValue::getTupleStorageSize(voltdb::VALUE_TYPE_INTEGER),
	NValue::getTupleStorageSize(voltdb::VALUE_TYPE_SMALLINT)};
bool COLUMN_ALLOW_NULLS[NUM_OF_COLUMNS] = { true, true, true, true, true };

class TimeWindowTest : public Test {
    public:
        TimeWindowTest() : table(NULL), window_table(NULL) {
            VOLT_DEBUG("CONSTRUCTOR");
            m_engine = new voltdb::VoltDBEngine();
            m_engine->initialize(1, 1, 0, 0, "");

            srand(0);
        }
        ~TimeWindowTest() {
            delete table;
            delete m_engine;
        }

    protected:

        voltdb::Table* table;
        voltdb::TimeWindow* window_table;
        voltdb::VoltDBEngine *m_engine;
        
        void createWindow(int32_t size, int32_t slide) {
            VOLT_DEBUG("CREATE WINDOW");

            voltdb::CatalogId database_id = 1000;
            char buffer[32];
            std::string *columnNames = new std::string[NUM_OF_COLUMNS];
            std::vector<voltdb::ValueType> columnTypes;
            std::vector<int32_t> columnLengths;
            std::vector<bool> columnAllowNull;
            for (int ctr = 0; ctr < NUM_OF_COLUMNS; ctr++) {
                switch(ctr)
                {
                    case TS_COL : 
                         VOLT_DEBUG("Set TIME");
                        snprintf(buffer, 32, "TIME");
                        break;
                    case WS_COL : 
                         VOLT_DEBUG("Set WSTART");
                        snprintf(buffer, 32, "WSTART");
                        break;
                    case WE_COL : 
                         VOLT_DEBUG("Set WEND");
                        snprintf(buffer, 32, "WEND");
                        break;
                    default :
                         VOLT_DEBUG("Set other column");
                        snprintf(buffer, 32, "column%02d", ctr);
                        break;
                }
                columnNames[ctr] = buffer;
                columnTypes.push_back(COLUMN_TYPES[ctr]);
                columnLengths.push_back(COLUMN_SIZES[ctr]);
                columnAllowNull.push_back(COLUMN_ALLOW_NULLS[ctr]);
            }
            voltdb::TupleSchema *schema = voltdb::TupleSchema::createTupleSchema(columnTypes, columnLengths, columnAllowNull, true);

            table = voltdb::TableFactory::getWindowTable(database_id, m_engine->getExecutorContext(),
                            "test_table", schema, columnNames, -1, false, false, size, slide, TIME_WINDOW);
            window_table = dynamic_cast<TimeWindow*>(table);

            VOLT_DEBUG("END CREATE WINDOW");
        }
};


inline void printTime(const char* s, struct timeval start, struct timeval stop) {
	double time = (double) stop.tv_sec + (double) stop.tv_usec * USEC
			- (double) start.tv_sec - (double) start.tv_usec * USEC;
	double t10000000 = time*1000000.0;
	cout << s << time << " - " << t10000000 << endl;
}


/**
 * This test creates different windows configurations: size, slide, and tuple per insert.
 * Then insert tuples into each one.  
 * The purpose of this test is to see how long does it take to run to completion.
 * Adjust the four for-loops variables to create different combinations and expected runtime.
 * 
 * 
 *  int maxWinSize = 50; step 5
 *  int maxWinSlide = 50; step 5
 *  int maxTupleCount = 50; step 5
 *  
 *  int iterationCount = 10000;
 * 
 * These values would have generated a total of 2.35 billions tuple and run on 100 window 
 * configurations
 * 
 * The configuration above would take about 70 minutes
 * Should only run this without VOLT_DEBUG messages as the number of debug messages are vary 
 * and outputting them add to the overall time.
 */ 
TEST_F(TimeWindowTest, InsertPerformance) {
    VOLT_DEBUG("INSERT PERFORMANCE");
    int maxWinSize = 15;
    int maxWinSlide = 15;
    int maxTupleCount = 15;
    int iterationCount = 10;
    for (int wSize = 1; wSize < maxWinSize; wSize += 5)
    {
        for (int wSlide = 1; wSlide < maxWinSlide; wSlide += 5)
        {
            for (int tCount = 1; tCount < maxTupleCount; tCount += 5)
            {
                createWindow(wSize, wSlide);
                for (int i = 0; i < iterationCount; i++)
                {
                    assert(tableutil::addRandomTuplesFixedColumn(this->table, tCount, TS_COL, ValueFactory::getIntegerValue(i)));
                    VOLT_DEBUG("Table at TS %d: %s", i, window_table->debug().c_str());
                }
                table->deleteAllTuples(true);
            }
        }
    }    
    VOLT_DEBUG("END INSERT PERFORMANCE");
}
/**
TEST_F(TimeWindowTest, InsertPerformance) {
    VOLT_DEBUG("INSERT PERFORMANCE");
    struct timeval start, stop;

    int maxWinSize = 1000;
    int maxWinSlide = 100;
    int maxTupleCount = 1000;
    //int iterationCount = 10;
    int winStepFactor = 10;
    int winSlideFactor = 10;

    for (int wSize = 10; wSize <= maxWinSize; wSize *= winStepFactor)
    {
        for (int wSlide = 1; wSlide <= maxWinSlide; wSlide *= winSlideFactor)
        {
            for (int tCount = 1; tCount <= maxTupleCount; tCount *= 10)
            {
                createWindow(wSize, wSlide);
                for(int i = 0; i < wSize; i++) {
                	assert(tableutil::addRandomTuplesFixedColumn(this->table, tCount, TS_COL, ValueFactory::getIntegerValue(i)));
                }

                gettimeofday(&start, NULL);
                for(int i = 0; i < wSize; i++) {
					assert(tableutil::addRandomTuplesFixedColumn(this->table, tCount, TS_COL, ValueFactory::getIntegerValue(i)));
				}
                gettimeofday(&stop, NULL);
                std::stringstream s;
                s << "Size: " << wSize << "  Slide: " << wSlide << "  Count: " << tCount << "  Time: ";
                printTime(s.str().c_str(), start, stop);

                table->deleteAllTuples(true);
            }
        }
    }

    VOLT_DEBUG("END INSERT PERFORMANCE");
}
*/

/**
 * Test the window active and stage tuple counts at timestamp tuple.
 * The test relies on having the fix number of tuples of the same timestamp
 * to verify the tuple count.
 */
TEST_F(TimeWindowTest, InsertTupleCount) {
    VOLT_DEBUG("INSERT TUPLE");
    int64_t stageTupleCount = 0;
    int maxWinSize = 15;
    int maxWinSlide = 15;
    int iterationCount = 10;
    for (int wSize = 1; wSize < maxWinSize; wSize += 5) 
    {
        for (int wSlide = 1; wSlide < maxWinSlide; wSlide += 5)
        {
            //nested loops to produce test of various window size and slide combination.
            createWindow(wSize, wSlide);
            for (int i = 0; i < iterationCount; i++)
            {
                assert(tableutil::addRandomTuplesFixedColumn(this->table, NUM_OF_TUPLES, TS_COL, ValueFactory::getIntegerValue(i)));
                VOLT_DEBUG("Table at TS %d: %s", i, window_table->debug().c_str());
                // Assert the active and staged tuple counts
                if (i < wSize)
                {
                    // The very first window
                    ASSERT_EQ(0, this->window_table->activeTupleCount());
                    ASSERT_EQ((i + 1) * NUM_OF_TUPLES, this->window_table->getStageActiveTupleCount());
                }
                else
                {
                    ASSERT_EQ(wSize * NUM_OF_TUPLES, this->window_table->activeTupleCount());
                        
                    if (wSlide > wSize)
                    {
                        if ((i % wSlide) >= wSize)
                        {
                            stageTupleCount = 0;
                        }
                        else
                        {
                            stageTupleCount = (((i - wSlide) % wSize) + 1) * NUM_OF_TUPLES;
                        }
                    }
                    else
                    {
                        stageTupleCount = (((i - wSize) % wSlide) + 1) * NUM_OF_TUPLES;
                    }
                    VOLT_DEBUG("Expected Staged Tuple Count is %ld, actual count %ld", stageTupleCount, this->window_table->getStageActiveTupleCount());
                    ASSERT_EQ(stageTupleCount, this->window_table->getStageActiveTupleCount());
                }
            }
            table->deleteAllTuples(true);
        }
    }
    VOLT_DEBUG("END INSERT TUPLE");
}

/**
 * Verify that the tuple contains the same data type specify in the schema.
 */ 
TEST_F(TimeWindowTest, ValueTypes) {
    //
    // Make sure that our table has the right types and that when
    // we pull out values from a tuple that it has the right type too
    //
    VOLT_DEBUG("VALUE TYPES");
    createWindow(1, 1);
    for (int i = 0; i < 10; i++)
    {
        assert(tableutil::addRandomTuplesFixedColumn(this->table, NUM_OF_TUPLES, TS_COL, ValueFactory::getIntegerValue(i)));
        VOLT_DEBUG("Table at TS %d: %s", i, window_table->debug().c_str());
    }
    voltdb::TableIterator iterator = this->table->tableIterator();
    voltdb::TableTuple tuple(table->schema());
    while (iterator.next(tuple)) {
        for (int ctr = 0; ctr < NUM_OF_COLUMNS; ctr++) {
            EXPECT_EQ(COLUMN_TYPES[ctr], this->table->schema()->columnType(ctr));
            EXPECT_EQ(COLUMN_TYPES[ctr], tuple.getType(ctr));
        }
    }
    VOLT_DEBUG("END VALUE TYPES");
}

TEST_F(TimeWindowTest, TupleDelete) {
    //
    // We are just going to delete all of the odd tuples, then make
    // sure they don't exist anymore
    //
    VOLT_DEBUG("***************************************************");
    VOLT_DEBUG("TUPLE DELETE");
    createWindow(1, 1);
    for (int i = 0; i < 10; i++)
    {
        assert(tableutil::addRandomTuplesFixedColumn(this->table, NUM_OF_TUPLES, TS_COL, ValueFactory::getIntegerValue(i)));
        VOLT_DEBUG("Table at TS %d: %s", i, window_table->debug().c_str());
    }
    VOLT_DEBUG("WINDOW BEFORE DELETE: %s", table->debug().c_str());
    voltdb::TableIterator iterator = this->table->tableIterator();
    voltdb::TableTuple tuple(table->schema());
    int i = 0;
    while (iterator.next(tuple)) {
        if (ValuePeeker::peekAsBigInt(tuple.getNValue(1)) != 0) {
            EXPECT_EQ(true, window_table->deleteTuple(tuple, true));
        }
        VOLT_DEBUG("DELETE #%d: %s", i, window_table->printChain().c_str());
        i++;
    }

    iterator = this->table->tableIterator();
    while (iterator.next(tuple)) {
        EXPECT_EQ(false, ValuePeeker::peekAsBigInt(tuple.getNValue(1)) != 0);
    }

    VOLT_DEBUG("WINDOW AFTER DELETE: %s", table->debug().c_str());
}


int main() {
	try
    {
        return TestSuite::globalInstance()->runAll();
    }
    catch (std::exception const &exc)
    {
        std::cerr << "Exception caught " << exc.what() << "\n";
    }
}

