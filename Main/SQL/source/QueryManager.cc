
#ifndef QUERY_MGER_CC
#define QUERY_MGER_CC

#include "QueryManager.h"
#include "MyDB_TableReaderWriter.h"
#include "MyDB_Schema.h"
#include "ParserTypes.h"
#include "MyDB_Catalog.h"

using namespace std;

QueryManager :: QueryManager (SQLStatement *_statement, MyDB_BufferManagerPtr _bufMgrPtr, MyDB_CatalogPtr _catalog, map <string, MyDB_TableReaderWriterPtr> _allTableReaderWriters) {
    this->statement = _statement;
    this->bufMgrPtr = _bufMgrPtr;
    this->catalog = _catalog;
    this->allTableReaderWriters = _allTableReaderWriters;


    // allTables = allTableReaderWriters;
    // allTables = MyDB_Table :: getAllTables (catalog);
	
	// for (auto &a : allTables) {
	// 	if (a.second->getFileType () == "heap") {
	// 		allTableReaderWriters[a.first] =  make_shared <MyDB_TableReaderWriter> (a.second, bufMgrPtr);
	// 	} else if (a.second->getFileType () == "bplustree") {
	// 		allBPlusReaderWriters[a.first] = make_shared <MyDB_BPlusTreeReaderWriter> (a.second->getSortAtt (), a.second, bufMgrPtr);
	// 		allTableReaderWriters[a.first] = allBPlusReaderWriters[a.first];	
	// 	}
    //     string tableFile = a.first+".tbl";
    //     cout << "Loading " + tableFile << endl;
    //     allTableReaderWriters[a.first]->loadFromTextFile(tableFile);
        
	// }
    // // ! might switch to blus one later idks
    // this->tableMap = allTableReaderWriters;
}

void QueryManager :: runExpression () {
    cout << "run expression starting" << endl;
    /* Data Structures Needed */
    MyDB_TableReaderWriterPtr inputTablePtr = nullptr;
    MyDB_TableReaderWriterPtr outputTablePtr = nullptr;
    MyDB_SchemaPtr outputSchema = make_shared<MyDB_Schema>();
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


    MyDB_SchemaPtr mySchemaOutAgain  = make_shared <MyDB_Schema> ();
		

	/* Parse valuesToSelect */
    for(auto v : query.getValues()){
        projections.push_back(v->toString());
        ExpType expType = v->getExpType();
        mySchemaOutAgain->appendAtt (make_pair(v->getName(), v->getAttTypePtr(catalog, tableAliases)));
        // need to fill this in
        if (expType == ExpType:: SumExp) {
            hasAggregation = true;
            // Todo: push back onto aggsToCompute
        } 
        else if (expType == ExpType:: AvgExp) {
            hasAggregation = true;
            // Todo: push back onto aggsToCompute
        } 
        else { // NonAggType
            // ! not sure what to do here
        }
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
    
    /* Parse groupingClauses */
    for (auto g : query.getGroupings()) {
        //! push g->toString() onto groupings?
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

