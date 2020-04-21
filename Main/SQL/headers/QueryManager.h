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
        
        MyDB_TableReaderWriterPtr joinOptimization(vector<pair<string, string>> tableToProcess, vector<ExprTree> allDisjunctions, map <string, string> tableAliases);
    private:
        SQLStatement *statement;
        MyDB_BufferManagerPtr bufMgrPtr;
        map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters;
        MyDB_CatalogPtr catalog;


};

#endif
