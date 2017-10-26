#include "rm.h"
#include <iostream>

RelationManager* RelationManager::instance()
{
	static RelationManager _rm;
	return &_rm;
}

RelationManager::RelationManager()
{
   _rbfm = RecordBasedFileManager::instance();
   FILE* exists = fopen("tables.stat", "r");
   if(exists == NULL) {
        stats = fopen("tables.stat", "wrb+");
   } else {
       fclose(exists);
       stats = fopen("tables.stat", "rb+");
   }
}

RelationManager::~RelationManager()
{
   _rbfm = NULL;
   fflush(stats);
   fclose(stats);
}
vector<Attribute> RelationManager::vector_table() {
   vector<Attribute> record;
   getTableAttr(record);

   return record;
}

vector<Attribute> RelationManager::vector_col() {
   vector<Attribute> record;
   getColumnAttr(record);

   return record;
}

RC RelationManager::insert_table(FileHandle &fh, const int tableId, const string name, const string fileName) {

   RID rid;
   vector<Attribute> recordDescriptor = vector_table();
   int offset = 0;
   int null_size = 1;

   int record_size = 0;
   size_t size = 1 + sizeof(int) + sizeof(int) + name.length() + sizeof(int) + fileName.length() + sizeof(int);

   void *record = malloc(size);
   unsigned char *nullsIndicator = (unsigned char *) malloc(null_size);
   memset(nullsIndicator, 0, null_size);

   memcpy((char *)record + offset, nullsIndicator, null_size);
   offset += null_size;

   memcpy((char *)record + offset, &tableId, sizeof(int));
   offset += sizeof(int);

//   memcpy((char *)record + offset, &name.length(), sizeof(int));
//   offset += sizeof(int);

   memcpy((char *)record + offset, name.c_str(), name.length());
   offset += name.length();

//   memcpy((char *)record + offset, fileName.length(), sizeof(int));
//   offset += sizeof(int);

   memcpy((char *)record + offset, fileName.c_str(), fileName.length());
   offset += fileName.length();

   record_size = offset;
   free(nullsIndicator);

   _rbfm->insertRecord(fh, recordDescriptor, record, rid);

   void *returnedData = malloc(size);
   _rbfm->readRecord(fh, recordDescriptor, rid, returnedData);

   free(returnedData);
   free(record);

   return 0;
}
RC RelationManager::insert_col(FileHandle &fh, const int tableId, const string name, const int colType, const int colLength, const int colPos) {

   RID rid;
   vector<Attribute> record_vector = vector_col();
   int offset = 0;

   int record_size = 0;
   size_t size = 1 + sizeof(int) + sizeof(int) + name.length() + sizeof(int) + sizeof(int) + sizeof(int);

   void *record = malloc(size);

   //prepare column vector
   int null_size = 1;
   unsigned char *nullsIndicator = (unsigned char *) malloc(null_size);
   memset(nullsIndicator, 0, null_size);
   memcpy((char *)record + offset, nullsIndicator, null_size);
   offset += null_size;
   memcpy((char *)record + offset, &tableId, sizeof(int));
   offset += sizeof(int);
   //memcpy((char *)record + offset, name.length(), sizeof(int));
   //offset += sizeof(int);
   memcpy((char *)record + offset, name.c_str(), name.length());
   offset += name.length();
   memcpy((char *)record + offset, &colType, sizeof(int));
   offset += sizeof(int);
   memcpy((char *)record + offset, &colLength, sizeof(int));
   offset += sizeof(int);
   memcpy((char *)record + offset, &colPos, sizeof(int));
   offset += sizeof(int);

   record_size = offset;
   free(nullsIndicator);

   _rbfm->insertRecord(fh, record_vector, record, rid);

   void *returnedData = malloc(size);
   _rbfm->readRecord(fh, record_vector, rid, returnedData);

   free(returnedData);
   free(record);

   return 0;
}

RC RelationManager::createCatalog()
{
    int trc = _rbfm->createFile("Tables");
    int crc = _rbfm->createFile("Columns");
    if(trc != 0 && crc != 0) return -1;

    if (trc){
    	FileHandle fh;

    	if(_rbfm->openFile("Tables", fh) != 0) return -1;

    	int max_id;
    	if(fseek(stats, 0, SEEK_SET) != 0) return -1;
    	if(fread(&max_id, sizeof(int), 1, stats) == 0) max_id = 0;

    	max_id ++;

    	insert_table(fh, max_id, "tables", "Tables");

    	max_id += 1;
    	insert_table(fh, max_id, "columns", "Columns");

    	if(_rbfm->closeFile(fh) != 0) return -1;

    }
    if (crc){
    	FileHandle fh;

    	if(_rbfm->openFile("Columns", fh) != 0) return -1;

    	insert_col(fh, 1, "table-id", TypeInt, 4, 1);
    	insert_col(fh, 1, "table-name", TypeVarChar, 50, 2);
    	insert_col(fh, 1, "file-name", TypeVarChar, 50, 3);
    	insert_col(fh, 2, "table-id", TypeInt, 4, 1);
    	insert_col(fh, 2, "column-name", TypeVarChar, 50, 2);
    	insert_col(fh, 2, "column-type", TypeInt, 4, 3);
    	insert_col(fh, 2, "column-length", TypeInt, 4, 3);
    	insert_col(fh, 2, "column-position", TypeInt, 4, 4);

    	if(_rbfm->closeFile(fh) != 0) return -1;
    }
    return 0;
}

RC RelationManager::deleteCatalog()
{
    int trc = _rbfm->destroyFile("Tables");
    int crc = _rbfm->destroyFile("Columns");
    if(trc != 0 &&  crc != 0) return -1;

    if(remove("tables.stat") != 0) return -1;

    return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	_rbfm->createFile(tableName);

	if(_rbfm->createFile(tableName) != 0) return -1;

   FileHandle fh;
   if(_rbfm->openFile("Tables", fh) != 0) return -1;


   int max_id;
   if(fseek(stats, 0, SEEK_SET) != 0) return -1;
   if(fread(&max_id, sizeof(int), 1, stats) == 0) max_id = 0;

   max_id ++;

   insert_table(fh, max_id, tableName, tableName);
   //close file



   return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    if (tableName == "Tables") {
    	getTableAttr(attrs);
    	return 0;
    }
    if (tableName == "Columns") {
    	getColumnAttr(attrs);
    	return 0;
    }

    RM_ScanIterator columnRsi;
    RID rid;
    void *data = malloc(PAGE_SIZE + 1);
    data[PAGE_SIZE] = '\0';
    vector<string> returnAttr;
    Attribute attr;
    int tid;
    returnAttr.push_back("column-name");
    returnAttr.push_back("column-type");
    returnAttr.push_back("column-length");

    if(getTidByTname(tableName, tid) != 0) return -1;
    if(scan("Columns", "table-id", EQ_OP, &tid, returnAttr, columnRsi) != 0) return -1;
    while(columnRsi.getNextTuple(rid, data) != RM_EOF) {
    	int len;
    	int offset = 0;

    	memcpy(&len, data, sizeof(int));
    	offset += sizeof(int);
    	char* charAttrName = (char*)malloc(len + 1);
    	charAttrName[len] = '\0';
    	memcpy(charAttrName, data + offset, len);
    	attr.name = charAttrName;
    	offset += len;

    	memcpy(&attr.type, data + offset, sizeof(int));
    	offset += sizeof(int);

    	memcpy(&attr.length, data + offset, sizeof(int));

    	attrs.push_back(attr);
    }
    columnRsi.close();
	return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	vector<Attribute> attrs;
	if (getAttributes(tableName, attrs) != 0) return -1;

	FileHandle fh;
	if(_rbfm->openFile(tableName, fh) != 0) return -1;

	if(_rbfm->insertRecord(fh, attrs, data, rid) != 0) return -1;
	if(_rbfm->closeFile(fh) != 0) return -1;
  
    return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	vector<Attribute> attrs;
	if (getAttributes(tableName, attrs) != 0) return -1;

	FileHandle fh;
	if(_rbfm->openFile(tableName, fh) != 0) return -1;

	if(_rbfm->deleteRecord(fh, attrs, rid) != 0) return -1;
	if(_rbfm->closeFile(fh) != 0) return -1;
    return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	vector<Attribute> attrs;
	if (getAttributes(tableName, attrs) != 0) return -1;

	FileHandle fh;
	if(_rbfm->openFile(tableName, fh) != 0) return -1;

	if(_rbfm->updateRecord(fh, attrs, data, rid) != 0) return -1;
	if(_rbfm->closeFile(fh) != 0) return -1;
	return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	vector<Attribute> attrs;
	if (getAttributes(tableName, attrs) != 0) return -1;

	FileHandle fh;
	if(_rbfm->openFile(tableName, fh) != 0) return -1;

	if(_rbfm->readRecord(fh, attrs, rid, data) != 0) return -1;
	if(_rbfm->closeFile(fh) != 0) return -1;
	return 0;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    return -1;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,
      const void *value,
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
	vector<Attribute> attrs;
	FileHandle fh;
	RBFM_ScanIterator rbsi;

	if(_rbfm->openFile(tableName, fh) != 0) return -1;

	switch(tableName) {
		case "Tables": {
			getTableAttr(attrs);
			break;
		}
		case "Columns": {
			getColumnAttr(attrs);
			break;
		}
		default: {
			if (getAttributes(tableName, attrs) != 0) return -1;
			break;
		}
	}
	if(_rbfm->scan(fh, attrs, conditionAttribute, compOp, value, attributeNames, rbsi) != 0) return -1;
	rm_ScanIterator.set_rbsi(&rbsi);
	rbsi.close();
	if(_rbfm->closeFile(fh) != 0) return -1;
	return 0;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}

void RelationManager::getTableAttr(vector<Attribute> &attrs) {
	Attribute attribute;

	attribute.name = "table-id";
	attribute.type = TypeInt;
	attribute.length = (AttrLength)4;
	attrs.push_back(attribute);

	attribute.name = "table-name";
	attribute.type = TypeVarChar;
	attribute.length = (AttrLength)50;
	attrs.push_back(attribute);

	attribute.name = "file-name";
	attribute.type = TypeVarChar;
	attribute.length = (AttrLength)50;
	attrs.push_back(attribute);
}

void RelationManager::getColumnAttr(vector<Attribute> &attrs) {
	Attribute attribute;

	attribute.name = "table-id";
	attribute.type = TypeInt;
	attribute.length = (AttrLength)4;
	attrs.push_back(attribute);

	attribute.name = "column-name";
	attribute.type = TypeVarChar;
	attribute.length = (AttrLength)50;
	attrs.push_back(attribute);

	attribute.name = "column-type";
	attribute.type = TypeInt;
	attribute.length = (AttrLength)4;
	attrs.push_back(attribute);

	attribute.name = "column-length";
	attribute.type = TypeInt;
	attribute.length = (AttrLength)4;
	attrs.push_back(attribute);

	attribute.name = "column-position";
	attribute.type = TypeInt;
	attribute.length = (AttrLength)4;
	attrs.push_back(attribute);
}

RC RelationManager::getTidByTname(const string &tableName, int & tid) {
	RM_ScanIterator rsi;
	RID rid;
	void *data = malloc(PAGE_SIZE + 1);
	data[PAGE_SIZE] = '\0';
	vector<string> tidAttr;
	Attribute attr;
	tidAttr.push_back("table-id");

	if(scan("Tables", "table-name", EQ_OP, &tableName, tidAttr, rsi) != 0) return -1;
	if(rsi.getNextTuple(rid, data) != RM_EOF) {
		memcpy(&tid, data, sizeof(int));
	}
	rsi.close();

	return 0;
}
