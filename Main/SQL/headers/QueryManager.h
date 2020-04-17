#ifndef QUERY_MGER_H
#define QUERY_MGER_H

#include "RegularSelection.h"
#include "ParserTypes.h"
#include "MyDB_TableReaderWriter.h"
#include "Aggregate.h"
#include "ExprTree.h"

class QueryManager{



    public: 

        QueryManager(SFWQuery query, MyDB_BufferManagerPtr bufMgrPtr);

        void runExpression();

    private:
        SFWQuery query;
        MyDB_BufferManagerPtr bufMgrPtr;
        map <string, MyDB_TableReaderWriterPtr> tableMap;

};

#endif
