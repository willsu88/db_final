#ifndef QUERY_MGER_H
#define QUERY_MGER_H

#include "RegularSelection.h"
#include "ParserTypes.h"
#include "MyDB_TableReaderWriter.h"
#include "Aggregate.h"
#include "ExprTree.h"
#include "MyDB_Catalog.h"

class QueryManager{



    public: 

        QueryManager(SFWQuery query, MyDB_BufferManagerPtr bufMgrPtr, MyDB_CatalogPtr catalog, map <string, MyDB_TableReaderWriterPtr> tableMap);

        void runExpression();

    private:
        SFWQuery query;
        MyDB_BufferManagerPtr bufMgrPtr;
        map <string, MyDB_TableReaderWriterPtr> tableMap;
        MyDB_CatalogPtr catalog;

};

#endif
