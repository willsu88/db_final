
#ifndef SQL_EXPRESSIONS
#define SQL_EXPRESSIONS

#include "MyDB_AttType.h"
#include "MyDB_TableReaderWriter.h"
#include <string>
#include <vector>
#include <algorithm>
#include <map>
#include <set>
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
	virtual MyDB_AttTypePtr getAttTypePtr(MyDB_SchemaPtr schema) = 0;
	virtual string getName() = 0;
	virtual size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) = 0;
	// <tablename1, attname1> <tablename2, attname2> 
	virtual pair<pair<string, string>, pair<string, string>> getTable() = 0;
	virtual set<string> getAtts() = 0;
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

	MyDB_AttTypePtr getAttTypePtr(MyDB_SchemaPtr schema) {
		return make_shared <MyDB_BoolAttType> ();
	}

	string getName() {
		return "Bool";
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;
	}

	pair<pair<string, string>, pair<string, string>> getTable() {
		return make_pair(make_pair("",""), make_pair("",""));
	}
	set<string> getAtts() {
		set<string> empty;
		return empty;
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

	MyDB_AttTypePtr getAttTypePtr(MyDB_SchemaPtr schema) {
		return make_shared <MyDB_DoubleAttType> ();
	}

	string getName() {
		return "Number";
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;
	}

	pair<pair<string, string>, pair<string, string>> getTable() {
		return make_pair(make_pair("",""), make_pair("",""));

	}

	set<string> getAtts() {
		set<string> empty;
		return empty;
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

	MyDB_AttTypePtr getAttTypePtr(MyDB_SchemaPtr schema) {
		return make_shared <MyDB_IntAttType> ();
	}

	string getName() {
		return "Number";
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;
	}

	pair<pair<string, string>, pair<string, string>> getTable() {
		return make_pair(make_pair("",""), make_pair("",""));
	}

	set<string> getAtts() {
		set<string> empty;
		return empty;
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

	MyDB_AttTypePtr getAttTypePtr(MyDB_SchemaPtr schema) {
		return make_shared <MyDB_StringAttType> ();
	}

	string getName() {
		return "Number";
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;
	}

	pair<pair<string, string>, pair<string, string>> getTable() {
		return make_pair(make_pair("",""), make_pair("",""));
	}

	set<string> getAtts() {
		set<string> empty;
		return empty;
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

	MyDB_AttTypePtr getAttTypePtr(MyDB_SchemaPtr schema) {
		// string tableFileName = tableAliases[tableName];
		// catalog->getString(tableFileName + "." + tableName + "_" + attName + ".type", this->attributeType);
		// this->getAttType();

		return schema->getAttByName(tableName + "_" + attName).second;
		
	}

	string getName() {
		return  tableName + "_" + attName;
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;
	}

	pair<pair<string, string>, pair<string, string>> getTable() {
		return make_pair(make_pair(tableName, tableName + "_" + attName), make_pair("",""));
	}

	set<string> getAtts() {
		set<string> empty;
		empty.insert(tableName + "_" + attName);
		return empty;
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

	MyDB_AttTypePtr getAttTypePtr(MyDB_SchemaPtr schema) {
		return make_shared <MyDB_DoubleAttType> ();
	}

	string getName() {
		return "Number";
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;
	}

	pair<pair<string, string>, pair<string, string>> getTable() {
		return make_pair(make_pair("", ""), make_pair("", ""));
	}

	set<string> getAtts() {
		set<string> left = lhs->getAtts();
		set<string> right = rhs->getAtts();
		set<string> un;
		merge(left.begin(), left.end(),
			right.begin(), right.end(),
			inserter(un, un.begin()));
		return un;
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

	MyDB_AttTypePtr getAttTypePtr(MyDB_SchemaPtr schema) {
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

	pair<pair<string, string>, pair<string, string>> getTable() {
		return make_pair(make_pair("", ""), make_pair("", ""));
	}

	set<string> getAtts() {
		set<string> left = lhs->getAtts();
		set<string> right = rhs->getAtts();
		set<string> un;
		merge(left.begin(), left.end(),
			right.begin(), right.end(),
			inserter(un, un.begin()));
		return un;
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

	MyDB_AttTypePtr getAttTypePtr(MyDB_SchemaPtr schema) {
		return make_shared <MyDB_DoubleAttType> ();
	}

	string getName() {
		return "Number";
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;
	}

	pair<pair<string, string>, pair<string, string>> getTable() {
		return make_pair(make_pair("", ""), make_pair("", ""));
	}

	set<string> getAtts() {
		set<string> left = lhs->getAtts();
		set<string> right = rhs->getAtts();
		set<string> un;
		merge(left.begin(), left.end(),
			right.begin(), right.end(),
			inserter(un, un.begin()));
		return un;
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

	MyDB_AttTypePtr getAttTypePtr(MyDB_SchemaPtr schema) {
		return make_shared <MyDB_DoubleAttType> ();
	}

	string getName() {
		return "Number";
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;
	}

	pair<pair<string, string>, pair<string, string>> getTable() {
		return make_pair(make_pair("", ""), make_pair("", ""));
	}

	set<string> getAtts() {
		set<string> left = lhs->getAtts();
		set<string> right = rhs->getAtts();
		set<string> un;
		merge(left.begin(), left.end(),
			right.begin(), right.end(),
			inserter(un, un.begin()));
		return un;
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

	MyDB_AttTypePtr getAttTypePtr(MyDB_SchemaPtr schema) {
		return make_shared <MyDB_BoolAttType>();
	}

	string getName() {
		return "Number";
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;
	}

	pair<pair<string, string>, pair<string, string>> getTable() {
		pair<string, string> leftName = lhs->getTable().first;
		pair<string, string> rightName = rhs->getTable().first;
		string leftTableName = leftName.first;
		string rightTableName = rightName.first;
		string leftAttName = leftName.second;
		string rightAttName = rightName.second;

		if (lhs->getExpType() == ExpType :: IdenExp && rhs->getExpType() == ExpType :: IdenExp) {		
			
			if (leftName == rightName) {
				return make_pair(leftName, make_pair("", ""));
			}

			if (leftTableName == rightTableName) {
				return make_pair(leftName, make_pair("", ""));
			}
			
			return make_pair(leftName, rightName); // Case for two tables
		} 

		else if (lhs->getExpType() == ExpType :: IdenExp) {
			return make_pair(leftName, make_pair("", ""));
		} 
		
		else if (rhs->getExpType() == ExpType :: IdenExp) {
			return make_pair(rightName, make_pair("", ""));
		}

		cout << "Equality getTable no identifier\n";
		
		return make_pair(make_pair("", ""), make_pair("", ""));
		
	}

	set<string> getAtts() {
		set<string> left = lhs->getAtts();
		set<string> right = rhs->getAtts();
		set<string> un;
		merge(left.begin(), left.end(),
			right.begin(), right.end(),
			inserter(un, un.begin()));
		return un;
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

	MyDB_AttTypePtr getAttTypePtr(MyDB_SchemaPtr schema) {
		return make_shared <MyDB_BoolAttType>();
	}

	string getName() {
		return "Number";
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;
	}

	pair<pair<string, string>, pair<string, string>> getTable() {
		pair<string, string> leftName = lhs->getTable().first;
		pair<string, string> rightName = rhs->getTable().first;
		string leftTableName = leftName.first;
		string rightTableName = rightName.first;
		string leftAttName = leftName.second;
		string rightAttName = rightName.second;

		if (lhs->getExpType() == ExpType :: IdenExp && rhs->getExpType() == ExpType :: IdenExp) {		
			
			if (leftName == rightName) {
				return make_pair(leftName, make_pair("", ""));
			}

			if (leftTableName == rightTableName) {
				return make_pair(leftName, make_pair("", ""));
			}
			
			return make_pair(leftName, rightName); // Case for two tables
		} 

		else if (lhs->getExpType() == ExpType :: IdenExp) {
			return make_pair(leftName, make_pair("", ""));
		} 
		
		else if (rhs->getExpType() == ExpType :: IdenExp) {
			return make_pair(rightName, make_pair("", ""));
		}

		cout << "Equality getTable no identifier\n";
		
		return make_pair(make_pair("", ""), make_pair("", ""));
		
	}

	set<string> getAtts() {
		set<string> left = lhs->getAtts();
		set<string> right = rhs->getAtts();
		set<string> un;
		merge(left.begin(), left.end(),
			right.begin(), right.end(),
			inserter(un, un.begin()));
		return un;
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

	MyDB_AttTypePtr getAttTypePtr(MyDB_SchemaPtr schema) {
		return make_shared <MyDB_BoolAttType>();
	}

	string getName() {
		return "Number";
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;  //! not sure
	}

	pair<pair<string, string>, pair<string, string>> getTable() {
		pair<string, string> leftName = lhs->getTable().first;
		pair<string, string> rightName = rhs->getTable().first;
		string leftTableName = leftName.first;
		string rightTableName = rightName.first;
		string leftAttName = leftName.second;
		string rightAttName = rightName.second;

		if (lhs->getExpType() == ExpType :: IdenExp && rhs->getExpType() == ExpType :: IdenExp) {		
			
			if (leftName == rightName) {
				return make_pair(leftName, make_pair("", ""));
			}

			if (leftTableName == rightTableName) {
				return make_pair(leftName, make_pair("", ""));
			}
			
			return make_pair(leftName, rightName); // Case for two tables
		} 

		else if (lhs->getExpType() == ExpType :: IdenExp) {
			return make_pair(leftName, make_pair("", ""));
		} 
		
		else if (rhs->getExpType() == ExpType :: IdenExp) {
			return make_pair(rightName, make_pair("", ""));
		}

		cout << "Equality getTable no identifier\n";
		
		return make_pair(make_pair("", ""), make_pair("", ""));
		
	}

	set<string> getAtts() {
		set<string> left = lhs->getAtts();
		set<string> right = rhs->getAtts();
		set<string> un;
		merge(left.begin(), left.end(),
			right.begin(), right.end(),
			inserter(un, un.begin()));
		return un;
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

	MyDB_AttTypePtr getAttTypePtr(MyDB_SchemaPtr schema) {
		return make_shared <MyDB_BoolAttType>();
	}

	string getName() {
		return "Number";
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;
	}
	
	pair<pair<string, string>, pair<string, string>> getTable() {
		pair<string, string> leftName = lhs->getTable().first;
		pair<string, string> rightName = rhs->getTable().first;
		string leftTableName = leftName.first;
		string rightTableName = rightName.first;
		string leftAttName = leftName.second;
		string rightAttName = rightName.second;
		if (leftTableName != "" && rightTableName != "") {	
			if (leftTableName == rightTableName) {
				return make_pair(leftName, make_pair("", ""));
			}
			return make_pair(leftName, rightName);
		} 
		
		else if (leftTableName != "") {
			return make_pair(leftName, make_pair("", ""));

		} 
		else if (rightTableName != "") {
			return make_pair(rightName, make_pair("", ""));
		}

		cout << "Equality getTable no identifier\n";
		return make_pair(make_pair("", ""), make_pair("", ""));
	}

	set<string> getAtts() {
		set<string> left = lhs->getAtts();
		set<string> right = rhs->getAtts();
		set<string> un;
		merge(left.begin(), left.end(),
			right.begin(), right.end(),
			inserter(un, un.begin()));
		return un;
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

	MyDB_AttTypePtr getAttTypePtr(MyDB_SchemaPtr schema) {
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

	pair<pair<string, string>, pair<string, string>> getTable() {
		pair<string, string> leftName = lhs->getTable().first;
		pair<string, string> rightName = rhs->getTable().first;
		string leftTableName = leftName.first;
		string rightTableName = rightName.first;
		string leftAttName = leftName.second;
		string rightAttName = rightName.second;

		if (lhs->getExpType() == ExpType :: IdenExp && rhs->getExpType() == ExpType :: IdenExp) {		
			
			if (leftName == rightName) {
				return make_pair(leftName, make_pair("", ""));
			}

			if (leftTableName == rightTableName) {
				return make_pair(leftName, make_pair("", ""));
			}
			
			return make_pair(leftName, rightName); // Case for two tables
		} 

		else if (lhs->getExpType() == ExpType :: IdenExp) {
			return make_pair(leftName, make_pair("", ""));
		} 
		
		else if (rhs->getExpType() == ExpType :: IdenExp) {
			return make_pair(rightName, make_pair("", ""));
		}

		cout << "Equality getTable no identifier\n";
		
		return make_pair(make_pair("", ""), make_pair("", ""));
		
	}

	set<string> getAtts() {
		set<string> left = lhs->getAtts();
		set<string> right = rhs->getAtts();
		set<string> un;
		merge(left.begin(), left.end(),
			right.begin(), right.end(),
			inserter(un, un.begin()));
		return un;
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

	MyDB_AttTypePtr getAttTypePtr(MyDB_SchemaPtr schema) {
		return make_shared <MyDB_BoolAttType>();
	}

	string getName() {
		return "Number";
	}
	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;
	}

	pair<pair<string, string>, pair<string, string>> getTable() {
		return child->getTable();
	}

	set<string> getAtts() {
		return child->getAtts();
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

	MyDB_AttTypePtr getAttTypePtr(MyDB_SchemaPtr schema) {
		return make_shared <MyDB_DoubleAttType>();
	}

	string getName() {
		return "Sum";
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;
	}

	pair<pair<string, string>, pair<string, string>> getTable() {
		return make_pair(make_pair("",""), make_pair("",""));
	}

	set<string> getAtts() {
		return child->getAtts();
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

	MyDB_AttTypePtr getAttTypePtr(MyDB_SchemaPtr schema) {
		return make_shared <MyDB_DoubleAttType>();
	}

	string getName() {
		return "Avg";
	}

	size_t calculateCost(map <string, MyDB_TableReaderWriterPtr> allTableReaderWriters, map <string, string> tableAliases) {
		return 0;
	}

	pair<pair<string, string>, pair<string, string>> getTable() {
		return make_pair(make_pair("",""), make_pair("",""));
	}

	set<string> getAtts() {
		return child->getAtts();
	}
};

#endif