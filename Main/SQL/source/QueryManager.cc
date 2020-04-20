
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

    //Might need to join other stuff
    inputTablePtr = allTableReaderWriters[tableToProcess.front().first];

    vector<pair<string, MyDB_AttTypePtr>> groupSchema;
    vector<pair<string, MyDB_AttTypePtr>> aggSchema;

	/* Parse valuesToSelect */
    for(auto v : query.getValues()){
        projections.push_back(v->toString());
        ExpType expType = v->getExpType();
        // cout << "ExpType: " << expType << endl;
        // mySchemaOutAgain->appendAtt (make_pair(v->getName(), v->getAttTypePtr(catalog, tableAliases)));
        // need to fill this in
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

    /* Combine all predicates into one string */
    selectionPredicate = allPredicates.front();
    if (allPredicates.size() > 1) {
        for (int i = 1; i < allPredicates.size(); i++) {
            selectionPredicate =  "&& (" + selectionPredicate + ", " + allPredicates[i] + ")";
        }
    }
    
    // /* Parse groupingClauses */
    // for (auto g : query.getGroupings()) {
    //     groupings.push_back(g->toString());
    // }

    cout << "Grouping size: " << groupings.size() << endl;

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

