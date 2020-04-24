
#ifndef QUERY_MGER_CC
#define QUERY_MGER_CC

#include "QueryManager.h"
#include "MyDB_TableReaderWriter.h"
#include "MyDB_Schema.h"
#include "SortMergeJoin.h"
#include "ScanJoin.h"
#include "ParserTypes.h"
#include "MyDB_Catalog.h"
#include "ExprTree.h"
#include <chrono>
#include <algorithm>

using namespace std;

// I think only bufMgr and catalog are needed
QueryManager :: QueryManager (SQLStatement *_statement, MyDB_BufferManagerPtr _bufMgrPtr, MyDB_CatalogPtr _catalog, map <string, MyDB_TableReaderWriterPtr> _allTableReaderWriters) {
    this->statement = _statement;
    this->bufMgrPtr = _bufMgrPtr;
    this->catalog = _catalog;
    this->allTableReaderWriters = _allTableReaderWriters;
}

// ! change it so the the pass in vector of tables to process with only the shorthand strings becaue with full names it wont work for query 9 and 10 because they use the same table twice 
// ! make it so the map is from the shorthand string to tablePtr
MyDB_TableReaderWriterPtr QueryManager :: joinOptimization(
    vector<string> tableToProcess, vector<ExprTreePtr> allDisjunctions, map <string, MyDB_TableReaderWriterPtr> tableMap, MyDB_TableReaderWriterPtr cur_table) {
    
    vector<string> tableToProcessCopy = tableToProcess;
    cout << "cur table ptr: " << cur_table << endl;
    cout << tableToProcess.front() << " ptr: " << tableMap[tableToProcess.front()] << endl;
    cout << tableToProcessCopy.size() << " amount of tables left to process\n";
    if (tableToProcessCopy.size() == 0) {
        return cur_table;
    }

    pair<pair<string, string>, pair<string, string>> table_att;
    pair<string, string> equality_check;// join param
    MyDB_TableReaderWriterPtr tableToJoin;// join param
    string tableToRemove;
    ExprTreePtr disjunct;// join param
    bool found_disjunct = false;
    for (auto d : allDisjunctions) {
        
        table_att = d->getTable();
        string leftTable = table_att.first.first;
        string rightTable = table_att.second.first;
        // If the disjunct had two tables
        if (leftTable != "" && rightTable != "") {
            cout << "Disjunct: " << d->toString() << endl;
            auto leftIt = find(tableToProcessCopy.begin(), tableToProcessCopy.end(), leftTable);
            auto rightIt = find(tableToProcessCopy.begin(), tableToProcessCopy.end(), rightTable);

            // Both tables belong to tables to process
            if (leftIt != tableToProcessCopy.end() && rightIt != tableToProcessCopy.end()) {
                continue;
            }

            cout << "left table: " << leftTable << " right table: " << rightTable << endl;
            found_disjunct = true;
            disjunct = d;
            // Means the left table is the one part of cur_table
            if (leftIt == tableToProcessCopy.end()) {
                equality_check.first = "[" + table_att.first.second + "]";
                equality_check.second = "[" + table_att.second.second + "]";
                tableToJoin = tableMap[rightTable];
                tableToRemove = rightTable;
            } else {
                equality_check.first = "[" + table_att.second.second + "]";
                equality_check.second = "[" + table_att.first.second + "]";
                tableToJoin = tableMap[leftTable];
                tableToRemove = leftTable;
            }
            break;
        } 

    }

    string finalPredicate;
    
    
    if (found_disjunct) {
        // Erase disjunct and table to process
        cout << "Removing " << tableToRemove << " from tables to process\n";
        tableToProcessCopy.erase(remove(tableToProcessCopy.begin(), tableToProcessCopy.end(), tableToRemove), tableToProcessCopy.end());
        allDisjunctions.erase(remove(allDisjunctions.begin(), allDisjunctions.end(), disjunct), allDisjunctions.end());
        finalPredicate = disjunct->toString();

    } else {
        cout << "cartesian hit\n";
        finalPredicate = "bool[true]";
        equality_check.first = "int[1]";
        equality_check.second = "int[1]";

        string randomTable = tableToProcessCopy.front();        
        tableToJoin = tableMap[randomTable];
        tableToProcessCopy.erase(remove(tableToProcessCopy.begin(), tableToProcessCopy.end(), randomTable), tableToProcessCopy.end());
    }

    cout << "equality pair: " << equality_check.first << "," << equality_check.second << endl;
    cout << "final pred: " << finalPredicate << endl;

    MyDB_SchemaPtr mySchemaOutAgain  = make_shared <MyDB_Schema> ();

    // Getting projections but lazily since I get everything.
    vector<pair<string, MyDB_AttTypePtr>> leftTableAtts = cur_table->getTable()->getSchema()->getAtts();
    vector<pair<string, MyDB_AttTypePtr>> rightTableAtts = tableToJoin->getTable()->getSchema()->getAtts();

    vector<string> projections;
    for (auto att : leftTableAtts) {
        if (att.first.substr(0, 1) == "[") {
            projections.push_back(att.first);
        } else {
            projections.push_back("[" + att.first + "]");
        }
        mySchemaOutAgain->appendAtt(att);
    }
    cout << endl;

    for (auto att : rightTableAtts) {
        if (att.first.substr(0, 1) == "[") {
            projections.push_back(att.first);
        } else {
            projections.push_back("[" + att.first + "]");
        }
        mySchemaOutAgain->appendAtt(att);
    }
    cout << endl;


    MyDB_TablePtr outTable = make_shared<MyDB_Table>("temp_" + to_string(tempTable), "temp_" + to_string(tempTable) + ".bin", mySchemaOutAgain);
    tempTable++;
    MyDB_TableReaderWriterPtr outputTablePtr = make_shared<MyDB_TableReaderWriter>(outTable, this->bufMgrPtr);
    cout << "About to do join\n";
    SortMergeJoin myOp (cur_table, tableToJoin, outputTablePtr, finalPredicate, projections, equality_check, "bool[true]", "bool[true]");
    myOp.run();
    
    return joinOptimization(tableToProcessCopy, allDisjunctions, tableMap, outputTablePtr);
}

void QueryManager :: runExpression () {
    
    auto start = chrono::steady_clock::now();


    MyDB_TableReaderWriterPtr inputTablePtr;

    SFWQuery query = statement->getSFW();
    vector <ExprTreePtr> allDisjunctions = query.getDisjunctions();
    vector <ExprTreePtr> valuesToSelect = query.getValues();
    vector <pair <string, string>> tablesToProcess = query.getTables();

    vector<string> joinTables;
    map <string, string> tableAliases;
    map<string, MyDB_TableReaderWriterPtr> tableMap;

    for (auto table: tablesToProcess) {
        joinTables.push_back(table.second);
        tableAliases[table.second] = table.first;
        tableMap[table.second] = allTableReaderWriters[table.first];
    }

    

    if (joinTables.size() != 1) {

        /* -------------------- Push Selection Down ------------------ */
        map <string, vector<string>> tableToPredicateMap; //<tableName, predicates>
        map <string, string> tableToSelectionMap; //<tableName, final selection>
        map <string, vector<string>> tableToProjectionMap;
        vector<ExprTreePtr> disjunctToRemove;
        /* Map table name to vector of one table disjunctions */
        for(auto d : allDisjunctions){
            pair<pair<string, string>, pair<string, string>> table = d->getTable();
            string leftTableName = table.first.first;
            string rightTableName = table.second.first;

            if (rightTableName != "") 
                continue;
            
            cout << "push selection " <<  d->toString() << endl;
            disjunctToRemove.push_back(d);
            tableToPredicateMap[leftTableName].push_back(d->toString());
        }

        for (auto d : disjunctToRemove) {
            allDisjunctions.erase(remove(allDisjunctions.begin(), allDisjunctions.end(), d), allDisjunctions.end());
        }
        bool isZero = false;
        /* Loop through every table with a disjunction */
        for (auto t: tableToPredicateMap) {
            /* Concat the predicates into a final selection predicate */
            string tableName = t.first;
            tableToSelectionMap[tableName] = CombineSelectionPredicate(t.second); // Grab final selection
            cout << tableName << " selection " << tableToSelectionMap[tableName] << endl;

            /* Retain all attributes in the original table */
            MyDB_SchemaPtr mySchema = make_shared <MyDB_Schema> ();
            vector<pair<string, MyDB_AttTypePtr>> tableAtts = tableMap[tableName]->getTable()->getSchema()->getAtts();
            for (auto att : tableAtts) {
                tableToProjectionMap[tableName].push_back("[" + att.first + "]");
                mySchema->appendAtt(att);
            }

            MyDB_TablePtr tempTablePtr = make_shared<MyDB_Table>("tempTable" + to_string(tempTable), "temp" + to_string(tempTable) + ".bin", mySchema);
            tempTable++;
            MyDB_TableReaderWriterPtr tempOutTablePtr = make_shared<MyDB_TableReaderWriter>(tempTablePtr, this->bufMgrPtr);

            /* Run Selection on this final predicate */
            RegularSelection *selection = new RegularSelection(tableMap[tableName], tempOutTablePtr, tableToSelectionMap[tableName], tableToProjectionMap[tableName]);
            selection->run();

            MyDB_RecordIteratorAltPtr ttIter = tempOutTablePtr->getIteratorAlt();
            MyDB_RecordPtr rectt = tempOutTablePtr->getEmptyRecord();
            if (ttIter->advance())  {
                ttIter->getCurrent(rectt);
                cout << "A rec from input table " << rectt << endl;

            } else {
                isZero = true;
                break;
            }


            /* Replace table pointer with new table pointer */
            tableMap[tableName] = tempOutTablePtr; 
        }
        /* -------------------- Push Selection Down ------------------ */


        /* Find the smallest table first */
        MyDB_TableReaderWriterPtr cur_table = tableMap[joinTables.front()];
        cout << "Cur table is " << joinTables.front() << endl;

        // for (int i = 1; i < joinTables.size(); i++) {
        //     MyDB_TableReaderWriterPtr next_table = tableMap[joinTables[i]];

        //     size_t cur_size = cur_table->getTable()->getTupleCount();
        //     size_t next_size = next_table->getTable()->getTupleCount();
        //     if (next_size < cur_size) 
        //         cur_table = next_table;
        // }

        joinTables.erase(remove(joinTables.begin(), joinTables.end(), joinTables.front()), joinTables.end());
        

        if (!isZero) {
            inputTablePtr = joinOptimization(joinTables, allDisjunctions, tableMap, cur_table);
        } else {
            cout << "Total number of records is: " << 0 << endl;

            auto end = chrono::steady_clock::now();
            cout << "Total time taken is: " 
                << chrono::duration_cast<chrono::seconds>(end - start).count()
                << " sec " << endl;
            return;
        }
    } else {
        cout << "Only table: " << tablesToProcess.front().second << endl;
        inputTablePtr = tableMap[tablesToProcess.front().second];
        cout << "input table ptr" << inputTablePtr << endl;
    }

    

    vector<pair<string, MyDB_AttTypePtr>> groupSchema;
    vector<pair<string, MyDB_AttTypePtr>> aggSchema;
    vector <pair <MyDB_AggType, string>> aggsToCompute; // For Aggregates
    vector <string> groupings;
    vector <string> projections; // For RegularSelection
    bool hasAggregation = false;

	/* Parse valuesToSelect */
    for(auto v : valuesToSelect){
        cout << "Values to select:" << v->toString() << endl;
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


    MyDB_SchemaPtr mySchemaOutAgain  = make_shared <MyDB_Schema> ();

    for (auto g : groupSchema) {
        mySchemaOutAgain->appendAtt(g);
    }

    for (auto ag: aggSchema) {
        mySchemaOutAgain->appendAtt(ag);
    }

    /* Parse allDisjunctions */
    vector<string> allPredicates;
    for(auto d : allDisjunctions){
        allPredicates.push_back(d->toString());
    }

    //Todo: make this a function
    /* Combine all predicates into one string */
    string selectionPredicate = allPredicates.front();
    if (allPredicates.size() > 1) {
        for (int i = 1; i < allPredicates.size(); i++) {
            selectionPredicate =  "&& (" + selectionPredicate + ", " + allPredicates[i] + ")";
        }
    }

    MyDB_RecordIteratorAltPtr ttIter = inputTablePtr->getIteratorAlt();
    MyDB_RecordPtr rectt = inputTablePtr->getEmptyRecord();
    ttIter->advance();
    ttIter->getCurrent(rectt);
    cout << "A rec from input table " << rectt << endl;
    
    
    /* Use the schema we created to get a outputTablePtr */ 
    MyDB_TablePtr outTable = make_shared<MyDB_Table>("TableOut", "TableOut.bin", mySchemaOutAgain);
    MyDB_TableReaderWriterPtr outputTablePtr = make_shared<MyDB_TableReaderWriter>(outTable, this->bufMgrPtr);

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

    /* Print out the first 30 records */
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

string QueryManager :: CombineSelectionPredicate(vector<string> allPredicates) {
    string selectionPredicate = allPredicates.front();
    if (allPredicates.size() > 1) {
        for (int i = 1; i < allPredicates.size(); i++) {
            selectionPredicate =  "&& (" + selectionPredicate + ", " + allPredicates[i] + ")";
        }
    }
    return selectionPredicate;
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

