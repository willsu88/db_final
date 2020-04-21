
#ifndef SQL_EXPRESSIONS
#define SQL_EXPRESSIONS

#include "MyDB_AttType.h"
#include <string>
#include <vector>
#include <map>
#include "MyDB_Catalog.h"

// create a smart pointer for database tables
using namespace std;
class ExprTree;
typedef shared_ptr <ExprTree> ExprTreePtr;

enum ExpType {SumExp, AvgExp, IdenExp, NonAgg};
enum AttType {BoolType, NumType, StringType};

// this class encapsules a parsed SQL expression (such as "this.that > 34.5 AND 4 = 5")

// class ExprTree is a pure virtual class... the various classes that implement it are below
class ExprTree {

public:
	virtual string toString () = 0;
	virtual ~ExprTree () {}

	virtual ExpType getExpType() = 0;
	virtual AttType getAttType() = 0;
	virtual MyDB_AttTypePtr getAttTypePtr(MyDB_CatalogPtr catalog, map <string, string> tableAliases) = 0;
	virtual string getName() = 0;
	virtual size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) = 0;
	
};

class BoolLiteral : public ExprTree {

private:
	bool myVal;
public:
	
	BoolLiteral (bool fromMe) {
		myVal = fromMe;
	}

	string toString () {
		if (myVal) {
			return "bool[true]";
		} else {
			return "bool[false]";
		}
	}

	AttType getAttType() {
		return BoolType;
	}

	ExpType getExpType() {
		return ExpType :: NonAgg;
	}

	MyDB_AttTypePtr getAttTypePtr(MyDB_CatalogPtr catalog, map <string, string> tableAliases) {
		return make_shared <MyDB_BoolAttType> ();
	}

	string getName() {
		return "Bool";
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;
	}
	
};

class DoubleLiteral : public ExprTree {

private:
	double myVal;
public:

	DoubleLiteral (double fromMe) {
		myVal = fromMe;
	}

	string toString () {
		return "double[" + to_string (myVal) + "]";
	}	

	~DoubleLiteral () {}

	AttType getAttType() {
		return NumType;
	}

	ExpType getExpType() {
		return ExpType :: NonAgg;
	}

	MyDB_AttTypePtr getAttTypePtr(MyDB_CatalogPtr catalog, map <string, string> tableAliases) {
		return make_shared <MyDB_DoubleAttType> ();
	}

	string getName() {
		return "Number";
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;
	}

};

// this implement class ExprTree
class IntLiteral : public ExprTree {

private:
	int myVal;
public:

	IntLiteral (int fromMe) {
		myVal = fromMe;
	}

	string toString () {
		return "int[" + to_string (myVal) + "]";
	}

	~IntLiteral () {}

	AttType getAttType() {
		return NumType;
	}

	ExpType getExpType() {
		return ExpType :: NonAgg;
	}

	MyDB_AttTypePtr getAttTypePtr(MyDB_CatalogPtr catalog, map <string, string> tableAliases) {
		return make_shared <MyDB_IntAttType> ();
	}

	string getName() {
		return "Number";
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;
	}

};

class StringLiteral : public ExprTree {

private:
	string myVal;
public:

	StringLiteral (char *fromMe) {
		fromMe[strlen (fromMe) - 1] = 0;
		myVal = string (fromMe + 1);
	}

	string toString () {
		return "string[" + myVal + "]";
	}

	~StringLiteral () {}

	
	AttType getAttType() {
		return StringType;
	}

	ExpType getExpType() {
		return ExpType :: NonAgg;
	}

	MyDB_AttTypePtr getAttTypePtr(MyDB_CatalogPtr catalog, map <string, string> tableAliases) {
		return make_shared <MyDB_StringAttType> ();
	}

	string getName() {
		return "Number";
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;
	}
};

class Identifier : public ExprTree {

private:
	string tableName;
	string attName;
	string attributeType;
	MyDB_AttTypePtr attTypePtr;
public:

	Identifier (char *tableNameIn, char *attNameIn) {
		tableName = string (tableNameIn);
		attName = string (attNameIn);
		attributeType = "";
		attTypePtr = nullptr;
	}

	string toString () {
		return "[" + attName + "]";
	}	

	ExpType getExpType() {
		return ExpType :: IdenExp;
	}

	~Identifier () {}

	AttType getAttType() {
		if (this->attributeType == "bool") {
			attTypePtr = make_shared<MyDB_BoolAttType>();
			return BoolType;
		}

		if (this->attributeType == "double") {
			attTypePtr = make_shared<MyDB_DoubleAttType>();
			return NumType;
		}

		if (this->attributeType == "int") {
			attTypePtr = make_shared<MyDB_IntAttType>();
			return NumType;
		}
		
		if (this->attributeType == "string") {
			attTypePtr = make_shared<MyDB_StringAttType>();
			return StringType;
		}

		attTypePtr = make_shared<MyDB_BoolAttType>();
		return BoolType;
	} 

	MyDB_AttTypePtr getAttTypePtr(MyDB_CatalogPtr catalog, map <string, string> tableAliases) {
		string tableFileName = tableAliases[tableName];
		catalog->getString(tableFileName + "." + attName + ".type", this->attributeType);
		this->getAttType();
		return attTypePtr;
	}

	string getName() {
		return  tableName + "_" + attName;
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;
	}
};

class MinusOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	MinusOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "- (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	ExpType getExpType() {
		return ExpType :: NonAgg;
	}

	~MinusOp () {}

	AttType getAttType() {
		return NumType;
	}

	MyDB_AttTypePtr getAttTypePtr(MyDB_CatalogPtr catalog, map <string, string> tableAliases) {
		return make_shared <MyDB_DoubleAttType> ();
	}

	string getName() {
		return "Number";
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;
	}
};

class PlusOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	PlusOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "+ (" + lhs->toString () + ", " + rhs->toString () + ")";
	}

	ExpType getExpType() {
		return ExpType :: NonAgg;
	}	

	~PlusOp () {}

	AttType getAttType() {
		return lhs->getAttType();
	}

	MyDB_AttTypePtr getAttTypePtr(MyDB_CatalogPtr catalog, map <string, string> tableAliases) {
		if (StringType == getAttType()) {
			return make_shared <MyDB_StringAttType> ();
		} else {
			return make_shared <MyDB_DoubleAttType> ();
		}
	}

	string getName() {
		return "Number";
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;
	}


};

class TimesOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	TimesOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "* (" + lhs->toString () + ", " + rhs->toString () + ")";
	}

	ExpType getExpType() {
		return ExpType :: NonAgg;
	}	

	~TimesOp () {}

	AttType getAttType() {
		return NumType;
	}

	MyDB_AttTypePtr getAttTypePtr(MyDB_CatalogPtr catalog, map <string, string> tableAliases) {
		return make_shared <MyDB_DoubleAttType> ();
	}

	string getName() {
		return "Number";
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;
	}
};

class DivideOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	DivideOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "/ (" + lhs->toString () + ", " + rhs->toString () + ")";
	}

	ExpType getExpType() {
		return ExpType :: NonAgg;
	}	

	~DivideOp () {}
	AttType getAttType() {
		return NumType;
	}

	MyDB_AttTypePtr getAttTypePtr(MyDB_CatalogPtr catalog, map <string, string> tableAliases) {
		return make_shared <MyDB_DoubleAttType> ();
	}

	string getName() {
		return "Number";
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;
	}
};

class GtOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	GtOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	ExpType getExpType() {
		return ExpType :: NonAgg;
	}

	string toString () {
		return "> (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	~GtOp () {}

	AttType getAttType() {
		return BoolType;
	}

	MyDB_AttTypePtr getAttTypePtr(MyDB_CatalogPtr catalog, map <string, string> tableAliases) {
		return make_shared <MyDB_BoolAttType>();
	}

	string getName() {
		return "Number";
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;
	}
};

class LtOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	LtOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "< (" + lhs->toString () + ", " + rhs->toString () + ")";
	}

	ExpType getExpType() {
		return ExpType :: NonAgg;
	}	

	~LtOp () {}

	AttType getAttType() {
		return BoolType;
	}

	MyDB_AttTypePtr getAttTypePtr(MyDB_CatalogPtr catalog, map <string, string> tableAliases) {
		return make_shared <MyDB_BoolAttType>();
	}

	string getName() {
		return "Number";
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;
	}
};

class NeqOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	NeqOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "!= (" + lhs->toString () + ", " + rhs->toString () + ")";
	}

	ExpType getExpType() {
		return ExpType :: NonAgg;
	}	

	~NeqOp () {}

	AttType getAttType() {
		return BoolType;
	}

	MyDB_AttTypePtr getAttTypePtr(MyDB_CatalogPtr catalog, map <string, string> tableAliases) {
		return make_shared <MyDB_BoolAttType>();
	}

	string getName() {
		return "Number";
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;  //! not sure
	}
};

class OrOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	OrOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "|| (" + lhs->toString () + ", " + rhs->toString () + ")";
	}

	ExpType getExpType() {
		return ExpType :: NonAgg;
	}	

	~OrOp () {}

	AttType getAttType() {
		return BoolType;
	}

	MyDB_AttTypePtr getAttTypePtr(MyDB_CatalogPtr catalog, map <string, string> tableAliases) {
		return make_shared <MyDB_BoolAttType>();
	}

	string getName() {
		return "Number";
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;
	}
};

class EqOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	EqOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "== (" + lhs->toString () + ", " + rhs->toString () + ")";
	}

	ExpType getExpType() {
		return ExpType :: NonAgg;
	}	

	~EqOp () {}

	AttType getAttType() {
		return BoolType;
	}

	MyDB_AttTypePtr getAttTypePtr(MyDB_CatalogPtr catalog, map <string, string> tableAliases) {
		return make_shared <MyDB_BoolAttType>();
	}

	string getName() {
		return "Number";
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {

		if (lhs->getExpType() == ExpType :: IdenExp && rhs->getExpType() == ExpType :: IdenExp) {
			string leftName = lhs->getName();
			string rightName = rhs->getName();
			string leftTableName = leftName.substr(0, leftName.find('_'));
			string rightTableName = rightName.substr(0, rightName.find('_'));
			string leftAttName = leftName.substr(leftName.find('_') + 1);
			string rightAttName = rightName.substr(rightName.find('_') + 1);

			MyDB_TableReaderWriterPtr leftTablePtr = allTableReaderWriters[tableAliases[leftTableName]];
			MyDB_TableReaderWriterPtr rightTablePtr = allTableReaderWriters[tableAliases[rightTableName]];

			size_t leftT = leftTablePtr->getTable()->getTupleCount();
			size_t rightT = rightTablePtr->getTable()->getTupleCount();

			size_t leftV = leftTablePtr->getTable()->getDistinctValues(leftAttName);
			size_t rightV = rightTablePtr->getTable()->getDistinctValues(rightAttName);

			size_t minV = min(leftV, rightV);

			return (leftT * rightT * minV) / (leftV * rightV);

		} else {
			return 0;
		}

	}
};

class NotOp : public ExprTree {

private:

	ExprTreePtr child;
	
public:

	NotOp (ExprTreePtr childIn) {
		child = childIn;
	}

	ExpType getExpType() {
		return ExpType :: NonAgg;
	}

	string toString () {
		return "!(" + child->toString () + ")";
	}	

	~NotOp () {}

	AttType getAttType() {
		return BoolType;
	}

	MyDB_AttTypePtr getAttTypePtr(MyDB_CatalogPtr catalog, map <string, string> tableAliases) {
		return make_shared <MyDB_BoolAttType>();
	}

	string getName() {
		return "Number";
	}
	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;
	}
};

class SumOp : public ExprTree {

private:

	ExprTreePtr child;
	
public:

	SumOp (ExprTreePtr childIn) {
		child = childIn;
	}

	ExpType getExpType() {
		return ExpType :: SumExp;
	}

	string toString () {
		return "sum(" + child->toString () + ")";
	}	

	~SumOp () {}

	AttType getAttType() {
		return NumType;
	}

	MyDB_AttTypePtr getAttTypePtr(MyDB_CatalogPtr catalog, map <string, string> tableAliases) {
		return make_shared <MyDB_DoubleAttType>();
	}

	string getName() {
		return "Sum";
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;
	}

};

class AvgOp : public ExprTree {

private:

	ExprTreePtr child;
	
public:

	AvgOp (ExprTreePtr childIn) {
		child = childIn;
	}

	string toString () {
		return "avg(" + child->toString () + ")";
	}	

	ExpType getExpType() {
		return ExpType :: AvgExp;
	}

	~AvgOp () {}

	AttType getAttType() {
		return NumType;
	}

	MyDB_AttTypePtr getAttTypePtr(MyDB_CatalogPtr catalog, map <string, string> tableAliases) {
		return make_shared <MyDB_DoubleAttType>();
	}

	string getName() {
		return "Avg";
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;
	}
};

#endif