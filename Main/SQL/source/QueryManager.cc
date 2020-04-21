
#ifndef QUERY_MGER_CC
#define QUERY_MGER_CC

#include "QueryManager.h"
#include "MyDB_TableReaderWriter.h"
#include "MyDB_Schema.h"
#include "ParserTypes.h"
#include "MyDB_Catalog.h"
#include <chrono>

using namespace std;

QueryManager :: QueryManager (SQLStatement *_statement, MyDB_BufferManagerPtr _bufMgrPtr, MyDB_CatalogPtr _catalog, map <string, MyDB_TableReaderWriterPtr> _allTableReaderWriters) {
    this->statement = _statement;
    this->bufMgrPtr = _bufMgrPtr;
    this->catalog = _catalog;
    this->allTableReaderWriters = _allTableReaderWriters;
}

MyDB_TableReaderWriterPtr QueryManager :: joinOptimization(
    vector<pair<string, string>> tableToProcess, vector<ExprTree> allDisjunctions, map <string, string> tableAliases) {

    set<MyDB_TableReaderWriterPtr> all_tables; 

    /* Find the smallest table first */
    MyDB_TableReaderWriterPtr cur_table = this->allTableReaderWriters[tableToProcess.front().first];
    all_tables.insert(cur_table);

    for (int i = 1; i < tableToProcess.size(); i++) {
        MyDB_TableReaderWriterPtr next_table = this->allTableReaderWriters[tableToProcess[i]];
        all_tables.insert(next_table);

        size_t cur_size = cur_table->getTable()->getTupleCount();
        size_t next_size = next_table->getTable()->getTupleCount();
        if (next_size < cur_size) 
            cur_table = next_table;
    }

    
    /* ! Separate disjunctions to only have equality */
    for (auto d: allDisjunctions) {
        // d->calculateCost(this->allTableReaderWriters, tableAliases);
    }
    
    /* Greedy */
    all_tables.erase(cur_table); 
    while (all_tables.size() > 1) {
        // for all tables
            // if cur table and new table is in disjunctions: calculate cost
            // else calculate cost cost of cross join

            // ! need to think about combined tables, how to calcualte cost after that
    }

    // Greedy join: 
        //while(tables still exist in vector)
            // find the table with the smallest result when joined with cur table
            // join cur table with that table
    
    // return cur_table

    return nullptr;
}

void QueryManager :: runExpression () {
    
    auto start = chrono::steady_clock::now();

    cout << "run expression starting" << endl;
    /* Data Structures Needed */
    MyDB_TableReaderWriterPtr inputTablePtr = nullptr;
    MyDB_TableReaderWriterPtr outputTablePtr = nullptr;
    MyDB_SchemaPtr mySchemaOutAgain  = make_shared <MyDB_Schema> ();
 	vector <string> groupings;
    string selectionPredicate;
    vector <string> projections; // For RegularSelection
    vector <pair <MyDB_AggType, string>> aggsToCompute; // For Aggregates
    bool hasAggregation = false;

    SFWQuery query = statement->getSFW();
    /* Create the MyDB_TableReaderWriterPtr inputin for RegularSelection by joining all the tables from SFWQuery */
    vector<pair<string, string>> tableToProcess = query.getTables();    
    map <string, string> tableAliases;
    for (auto p : tableToProcess) {
        tableAliases[p.second] = p.first;
    }

    vector<pair<string, MyDB_AttTypePtr>> groupSchema;
    vector<pair<string, MyDB_AttTypePtr>> aggSchema;

	/* Parse valuesToSelect */
    for(auto v : query.getValues()){
        projections.push_back(v->toString());
        ExpType expType = v->getExpType();
        if (expType == ExpType:: SumExp) {
            hasAggregation = true;
            string push = v->toString().substr(4);
            int len = push.size();
            push = push.substr(0, len -1);
            aggsToCompute.push_back(make_pair(MyDB_AggType::Sum, push));
            aggSchema.push_back(make_pair(v->getName(), v->getAttTypePtr(catalog, tableAliases)));
        } 
        else if (expType == ExpType:: AvgExp) {
            hasAggregation = true;
            string push = v->toString().substr(4);
            int len = push.size();
            push = push.substr(0, len -1);
            aggsToCompute.push_back(make_pair(MyDB_AggType::Avg, push));
            aggSchema.push_back(make_pair(v->getName(), v->getAttTypePtr(catalog, tableAliases)));
        } 
        else { // NonAggType
            groupings.push_back(v->toString());
            groupSchema.push_back(make_pair(v->getName(), v->getAttTypePtr(catalog, tableAliases)));
            cout << "Grouping:" << v->toString() << endl;
        }
    }

    for (auto g : groupSchema) {
        mySchemaOutAgain->appendAtt(g);
    }

    for (auto ag: aggSchema) {
        mySchemaOutAgain->appendAtt(ag);
    }

    /* Parse allDisjunctions */
    vector<string> allPredicates;
    for(auto d : query.getDisjunctions()){
        allPredicates.push_back(d->toString());
    }

    //Todo: make this a function
    /* Combine all predicates into one string */
    selectionPredicate = allPredicates.front();
    if (allPredicates.size() > 1) {
        for (int i = 1; i < allPredicates.size(); i++) {
            selectionPredicate =  "&& (" + selectionPredicate + ", " + allPredicates[i] + ")";
        }
    }

    //Might need to join other stuff
    if (tableToProcess.size() == 1) {
        inputTablePtr = allTableReaderWriters[tableToProcess.front().first];
    } else {
        /* Project tables first for optimization */
        // if disjunction belongs to one table, do selection on that table.
        // Group disjunctions into same tables and do one selection on that table.

        // construct SQLstatement , allTableREadersWriters (just one table)
        
        //todo: maybe add something to ExprTree to get num of tables

        /* Replace allTableReaderWriter with new pointers */

        inputTablePtr = this->joinOptimization(tableToProcess, query.getDisjunctions(), tableAliases);
    }
    
    /* Use the schema we created to get a outputTablePtr */ 
    MyDB_TablePtr outTable = make_shared<MyDB_Table>("TableOut", "TableOut.bin", mySchemaOutAgain);
    outputTablePtr = make_shared<MyDB_TableReaderWriter>(outTable, this->bufMgrPtr);

    /* Distingush between aggregation and regular selection */
    if (hasAggregation) {
        cout << "doing aggregation\n";
        Aggregate *aggregation = new Aggregate(inputTablePtr, outputTablePtr, aggsToCompute, groupings, selectionPredicate);
        aggregation->run();
    }
    else{
        cout << "Doing selection\n";
        RegularSelection *selection = new RegularSelection(inputTablePtr, outputTablePtr, selectionPredicate, projections);
        selection->run();
    }

    cout << "Finished selection\n";

    /* Print out the first 30 records */
    // TODO: need to calculate and print query time
    MyDB_RecordPtr rec = outputTablePtr->getEmptyRecord();
    MyDB_RecordIteratorAltPtr iter = outputTablePtr->getIteratorAlt();

    cout << "About to output\n";


    int num_records = 0;
    while(iter->advance()){
        iter->getCurrent(rec);
        if(num_records < 30)
            cout << "Record " << num_records << ": " <<  rec << endl;
        num_records++;
    }
    cout << "Total number of records is: " << num_records << endl;

    auto end = chrono::steady_clock::now();
    cout << "Total time taken is: " 
		<< chrono::duration_cast<chrono::seconds>(end - start).count()
		<< " sec " << endl;
}

#endif




// Function:
// runExpression()
// Parse valuesToSelect, allDisjunctions, groupingClauses from our input SFWQuery
// Create the MyDB_TableReaderWriterPtr inputin for RegularSelection by joining all the tables from SFWQuery
// Make schema for MyDB_TableReaderWriterPtr outputin for RegularSelection, create this scema by looping through the valuesToSelect
// For RegularSelection:
// Get selectionPredicateIn from disjunctions 
// Get projectionsIn from values to select (do it in the for loop of values to select)
// For Aggregate:
// Get aggsToComputeIn from valuesToSelect
// Get groupings from groupingClaues (from SFWQuery)
// Get selectionPredicateIn from disjunctions
// If just selection, create a RegularSelection class using the values we just created. Call .run() on it. 
// Get an iterator from outputin and then iterate 30 times to grab the records. 

// Note: When we need to do join, plug the output of join as the input for aggregate

