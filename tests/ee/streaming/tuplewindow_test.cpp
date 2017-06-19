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
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/persistenttable.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "storage/tableutil.h"
#include "streaming/TupleWindow.h"
#include "execution/VoltDBEngine.h"

using std::string;
using std::vector;
using namespace voltdb;

#define NUM_OF_COLUMNS 8
#define NUM_OF_TUPLES 20
#define WINDOW_SIZE 4
#define SLIDE_SIZE 2

//The TEST_ID_COL is only intended to aid in verification of tuples flow through Window.
#define TEST_ID_COL 0

#define WS_COL 1
#define WE_COL 2
#define USEC 0.000001

voltdb::ValueType COLUMN_TYPES[NUM_OF_COLUMNS] = { voltdb::VALUE_TYPE_INTEGER,
		voltdb::VALUE_TYPE_INTEGER, voltdb::VALUE_TYPE_INTEGER,
		voltdb::VALUE_TYPE_BIGINT, voltdb::VALUE_TYPE_TINYINT,
		voltdb::VALUE_TYPE_SMALLINT, voltdb::VALUE_TYPE_INTEGER,
		voltdb::VALUE_TYPE_BIGINT };
int32_t COLUMN_SIZES[NUM_OF_COLUMNS] = { NValue::getTupleStorageSize(
		voltdb::VALUE_TYPE_INTEGER), NValue::getTupleStorageSize(
		voltdb::VALUE_TYPE_INTEGER), NValue::getTupleStorageSize(
		voltdb::VALUE_TYPE_INTEGER), NValue::getTupleStorageSize(
		voltdb::VALUE_TYPE_BIGINT), NValue::getTupleStorageSize(
		voltdb::VALUE_TYPE_TINYINT), NValue::getTupleStorageSize(
		voltdb::VALUE_TYPE_SMALLINT), NValue::getTupleStorageSize(
		voltdb::VALUE_TYPE_INTEGER), NValue::getTupleStorageSize(
		voltdb::VALUE_TYPE_BIGINT) };
bool COLUMN_ALLOW_NULLS[NUM_OF_COLUMNS] = { true, true, true, true, true, true,
		true };

class TupleWindowTest: public Test {
public:
	TupleWindowTest() :
			table(NULL), window_table(NULL) {
		m_engine = new voltdb::VoltDBEngine();
		m_engine->initialize(1, 1, 0, 0, "");

		srand(0);
	}
	~TupleWindowTest() {
		delete table;
		delete m_engine;
	}

protected:

	voltdb::Table* table;
	voltdb::TupleWindow* window_table;
	voltdb::VoltDBEngine *m_engine;

	void createWindow(int32_t size, int32_t slide) {
		createWindow(size, slide, GROUP_BY_NONE);
	}

	void createWindow(int32_t size, int32_t slide, int32_t groupByIndex) {
		VOLT_DEBUG("CREATE WINDOW");

		voltdb::CatalogId database_id = 1000;
		char buffer[32];
		std::string *columnNames = new std::string[NUM_OF_COLUMNS];
		std::vector<voltdb::ValueType> columnTypes;
		std::vector<int32_t> columnLengths;
		std::vector<bool> columnAllowNull;
		for (int ctr = 0; ctr < NUM_OF_COLUMNS; ctr++) {
			switch (ctr) {
			case TEST_ID_COL:
				VOLT_DEBUG("Set ID column");
				snprintf(buffer, 32, "ID");
				break;
			case WS_COL:
				VOLT_DEBUG("Set WSTART");
				snprintf(buffer, 32, "WSTART");
				break;
			case WE_COL:
				VOLT_DEBUG("Set WEND");
				snprintf(buffer, 32, "WEND");
				break;
			default:
				VOLT_DEBUG("Set other column");
				snprintf(buffer, 32, "column%02d", ctr);
				break;
			}
			columnNames[ctr] = buffer;
			columnTypes.push_back(COLUMN_TYPES[ctr]);
			columnLengths.push_back(COLUMN_SIZES[ctr]);
			columnAllowNull.push_back(COLUMN_ALLOW_NULLS[ctr]);
		}
		voltdb::TupleSchema *schema = voltdb::TupleSchema::createTupleSchema(
				columnTypes, columnLengths, columnAllowNull, true);

		table = voltdb::TableFactory::getWindowTable(database_id,
				m_engine->getExecutorContext(), "test_table", schema,
				columnNames, -1, false, false, size, slide, TUPLE_WINDOW,
				groupByIndex);
		window_table = dynamic_cast<TupleWindow*>(table);

		VOLT_DEBUG("END CREATE WINDOW");
	}

};

/** Print time value to cout with endl */
inline void printTime(const char* s, struct timeval start,
		struct timeval stop) {
	double time = (double) stop.tv_sec + (double) stop.tv_usec * USEC
			- (double) start.tv_sec - (double) start.tv_usec * USEC;
	double t10000000 = time * 1000000.0;
	cout << s << t10000000 << endl;
}

/**
 * Verify that the tuple contains the same data type specify in the schema.
 */
TEST_F(TupleWindowTest, ValueTypesGroupByCol7) {
	//
	// Make sure that our table has the right types and that when
	// we pull out values from a tuple that it has the right type too
	//
	VOLT_DEBUG("VALUE TYPES");
	createWindow(2, 1, 6);
	for (int i = 0; i < 20; i++) {
		assert(
				tableutil::addRandomTuplesFixedColumn(this->table, 1, TEST_ID_COL, ValueFactory::getIntegerValue(i), 6, ValueFactory::getIntegerValue(i % 3)));
		VOLT_DEBUG("Table at TS %d: %s", i, window_table->debug().c_str());
	}
	int64_t tuple_counts = this->table->activeTupleCount();
	VOLT_DEBUG("Actual Tuple Count: %ld", tuple_counts);
	EXPECT_GT(tuple_counts, 0);
	voltdb::TableIterator iterator = this->table->tableIterator();
	voltdb::TableTuple tuple(table->schema());
	while (iterator.next(tuple)) {
		for (int ctr = 0; ctr < NUM_OF_COLUMNS; ctr++) {
			EXPECT_EQ(COLUMN_TYPES[ctr],
					this->table->schema()->columnType(ctr));
			EXPECT_EQ(COLUMN_TYPES[ctr], tuple.getType(ctr));
		}
	}
	VOLT_DEBUG("END VALUE TYPES");
}

/**
 * Verify that the tuple contains the same data type specify in the schema.
 */

TEST_F(TupleWindowTest, ValueTypes) {
	//
	// Make sure that our table has the right types and that when
	// we pull out values from a tuple that it has the right type too
	//
	VOLT_DEBUG("VALUE TYPES");
	createWindow(5, 2);
	for (int i = 0; i < 10; i++) {
		assert(
				tableutil::addRandomTuplesFixedColumn(this->table, 1, TEST_ID_COL, ValueFactory::getIntegerValue(i)));
		VOLT_DEBUG("Table at TS %d: %s", i, window_table->debug().c_str());
	}
	int64_t tuple_counts = this->table->activeTupleCount();
	VOLT_DEBUG("Actual Tuple Count: %ld", tuple_counts);
	EXPECT_GT(tuple_counts, 0);
	voltdb::TableIterator iterator = this->table->tableIterator();
	voltdb::TableTuple tuple(table->schema());
	while (iterator.next(tuple)) {
		for (int ctr = 0; ctr < NUM_OF_COLUMNS; ctr++) {
			EXPECT_EQ(COLUMN_TYPES[ctr],
					this->table->schema()->columnType(ctr));
			EXPECT_EQ(COLUMN_TYPES[ctr], tuple.getType(ctr));
		}
	}
	VOLT_DEBUG("END VALUE TYPES");
}

/**
 * Test the window active and stage tuple counts as tuples are being inserted
 */

TEST_F(TupleWindowTest, InsertTupleCount) {
	VOLT_DEBUG("INSERT TUPLE");
	int64_t stageTupleCount = 0;
	int maxWinSize = 15;
	int maxWinSlide = 15;
	int tupleCount = maxWinSize;
	for (int wSlide = 1; wSlide < maxWinSlide; wSlide += 5) {
		for (int wSize = 1; wSize < maxWinSize; wSize += 5) {
			//nested loops to produce test of various window size and slide combination.
			createWindow(wSize, wSlide);
			for (int i = 1; i <= tupleCount; i++) {
				assert(
						tableutil::addRandomTuplesFixedColumn(this->table, 1, TEST_ID_COL, ValueFactory::getIntegerValue(i)));
				VOLT_DEBUG("Table at tuple %d: %s", i,
						window_table->debug().c_str());
				// Assert the active and staged tuple counts
				if (i < wSize) {
					// The very first window is still not available
					ASSERT_EQ(0, this->window_table->activeTupleCount());
					ASSERT_EQ(i,
							this->window_table->getStageActiveTupleCount());
				} else {
					ASSERT_EQ(wSize, this->window_table->activeTupleCount());

					if (wSlide > wSize) {
						if (wSize == 1 || ((i - 1) % wSlide + 1) >= wSize) {
							stageTupleCount = 0;
						} else {
							stageTupleCount = ((i - 1) % wSlide) + 1;
						}
					} else {
						stageTupleCount = (i - wSize) % wSlide;
					}
					VOLT_DEBUG(
							"Expected Staged Tuple Count is %ld, actual count %ld",
							stageTupleCount,
							this->window_table->getStageActiveTupleCount());
					ASSERT_EQ(stageTupleCount,
							this->window_table->getStageActiveTupleCount());
				}
			}
			table->deleteAllTuples(true);
		}
	}
	VOLT_DEBUG("END INSERT TUPLE");
}

TEST_F(TupleWindowTest, InsertPerformance) {
	VOLT_DEBUG("INSERT PERFORMANCE");
	struct timeval start, stop;

	int maxWinSize = 100;
	int maxWinSlide = 100;
	int winStepFactor = 10;
	int winSlideFactor = 10;

	for (int wSize = 10; wSize <= maxWinSize; wSize *= winStepFactor) {
		for (int wSlide = 1; wSlide <= maxWinSlide; wSlide *= winSlideFactor) {
			for (int tCount = 1; tCount <= winStepFactor; tCount *= 10) {
				createWindow(wSize, wSlide);
				gettimeofday(&start, NULL);
				for (int i = 0; i < wSize; i++) {
					assert(
							tableutil::addRandomTuplesFixedColumn(this->table, tCount, TEST_ID_COL, ValueFactory::getIntegerValue(i)));
				}
				gettimeofday(&stop, NULL);
				std::stringstream s;
				s << wSize << "," << wSlide << "," << tCount << ",";
				printTime(s.str().c_str(), start, stop);

				table->deleteAllTuples(true);
			}
		}
	}

	VOLT_DEBUG("END INSERT PERFORMANCE");
}

int main() {
	return TestSuite::globalInstance()->runAll();
}
