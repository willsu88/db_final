#ifndef QUERY_MGER_H
#define QUERY_MGER_H

#include "RegularSelection.h"
#include "ParserTypes.h"
#include "MyDB_BPlusTreeReaderWriter.h"
#include "MyDB_TableReaderWriter.h"
#include "Aggregate.h"
#include "ExprTree.h"
#include "MyDB_Catalog.h"

class QueryManager{



    public: 

        QueryManager(SQLStatement *statement, MyDB_BufferManagerPtr bufMgrPtr, MyDB_CatalogPtr catalog, map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters);

        void runExpression();
        
        MyDB_TableReaderWriterPtr joinOptimization(vector<string> tableToProcess, vector<ExprTreePtr> allDisjunctions, map <string, MyDB_TableReaderWriterPtr> tableMap, MyDB_TableReaderWriterPtr cur_table);

        string CombineSelectionPredicate(vector<string> allPredicates);
        size_t getCost(string leftTable, string leftAtt,string rightTable, string rightAtt, map <string, MyDB_TableReaderWriterPtr> tableMap);
    private:
        SQLStatement *statement;
        MyDB_BufferManagerPtr bufMgrPtr;
        map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters;
        MyDB_CatalogPtr catalog;
        set<string> allAtts;
        int tempTable = 0;


};

#endif
