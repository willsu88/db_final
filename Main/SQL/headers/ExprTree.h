
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

enum ExpType {SumExp, AvgExp, NonAgg};
enum AttType {BoolType, NumType, StringType};

// this class encapsules a parsed SQL expression (such as "this.that > 34.5 AND 4 = 5")

// class ExprTree is a pure virtual class... the various classes that implement it are below
class ExprTree {

public:
	virtual string toString () = 0;
	virtual ~ExprTree () {}

	ExpType getExpType() {
		return ExpType :: NonAgg;
	}
	virtual AttType getAttType() = 0;
	virtual MyDB_AttTypePtr getAttTypePtr(MyDB_CatalogPtr catalog, map <string, string> tableAliases) = 0;
	virtual string getName() = 0;
	
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

	MyDB_AttTypePtr getAttTypePtr(MyDB_CatalogPtr catalog, map <string, string> tableAliases) {
		return make_shared <MyDB_BoolAttType> ();
	}

	string getName() {
		return "Bool";
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

	MyDB_AttTypePtr getAttTypePtr(MyDB_CatalogPtr catalog, map <string, string> tableAliases) {
		return make_shared <MyDB_DoubleAttType> ();
	}

	string getName() {
		return "Number";
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

	MyDB_AttTypePtr getAttTypePtr(MyDB_CatalogPtr catalog, map <string, string> tableAliases) {
		return make_shared <MyDB_IntAttType> ();
	}

	string getName() {
		return "Number";
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

	MyDB_AttTypePtr getAttTypePtr(MyDB_CatalogPtr catalog, map <string, string> tableAliases) {
		return make_shared <MyDB_StringAttType> ();
	}

	string getName() {
		return "Number";
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
		return "[" + tableName + "_" + attName + "]";
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
		cout << "Table aliases size: " << tableAliases.size() << endl;
		cout << "table name: " << tableName << " table aliases " << tableAliases.begin()->first << endl;
		string tableFileName = tableAliases[tableName];
		cout << tableFileName << endl;
		cout << "att name: " << attName << endl;
		catalog->getString(tableFileName + "." + attName + ".type", this->attributeType);
		cout << this->attributeType << endl;
		this->getAttType();
		return attTypePtr;
	}

	string getName() {
		return attName;
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
};

class NotOp : public ExprTree {

private:

	ExprTreePtr child;
	
public:

	NotOp (ExprTreePtr childIn) {
		child = childIn;
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
};

class SumOp : public ExprTree {

private:

	ExprTreePtr child;
	
public:

	SumOp (ExprTreePtr childIn) {
		child = childIn;
	}

	string toString () {
		return "sum(" + child->toString () + ")";
	}	

	~SumOp () {}

	ExpType getExpType() {
		return ExpType :: SumExp;
	}

	AttType getAttType() {
		return NumType;
	}

	MyDB_AttTypePtr getAttTypePtr(MyDB_CatalogPtr catalog, map <string, string> tableAliases) {
		return make_shared <MyDB_DoubleAttType>();
	}

	string getName() {
		return "Sum";
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

	~AvgOp () {}

	ExpType getExpType() {
		return ExpType :: AvgExp;
	}

	AttType getAttType() {
		return NumType;
	}

	MyDB_AttTypePtr getAttTypePtr(MyDB_CatalogPtr catalog, map <string, string> tableAliases) {
		return make_shared <MyDB_DoubleAttType>();
	}

	string getName() {
		return "Avg";
	}
};

#endif
