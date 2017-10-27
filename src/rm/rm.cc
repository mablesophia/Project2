#include "rm.h"

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
   _rbfm = RecordBasedFileManager::instance();
   FILE* exists = fopen("tables.stat", "r");
   if(exists != NULL) {
     fclose(exists);
     stats = fopen("tables.stat", "rb+");
   }
   else stats = fopen("tables.stat", "wrb+");
}

RelationManager::~RelationManager()
{
   _rbfm = NULL;
   fflush(stats);
   fclose(stats);
}
RC RelationManager::createCatalog()
{
  int table_rc, col_rc, maxID;
  FileHandle fh1; FileHandle fh2;

  _rbfm->createFile("Tables.txt");
  _rbfm->createFile("Columns.txt");

    // insert table records
    _rbfm->openFile("Tables.txt", fh1);
    //get table id
    fseek(stats, 0, SEEK_SET);
    fread(&maxID, sizeof(int), 1, stats);
    maxID = maxID + 1;
    //insert record
    insertTableRecord(fh1, maxID,   "Tables",  "Tables.txt");
    insertTableRecord(fh1, maxID+1, "Columns", "Columns.txt");

    _rbfm->closeFile(fh1);
    // set table id
    fseek(stats, 0, SEEK_SET);
    fwrite(&maxID, sizeof(int), 1, stats);
    fflush(stats);
    table_rc = 0;

    // insert column records
    _rbfm->openFile("Columns.txt", fh2);

    insertColumnRecord(fh2, 1, "table-id",   TypeInt,      4, 1);
    insertColumnRecord(fh2, 1, "table-name", TypeVarChar, 50, 2);
    insertColumnRecord(fh2, 1, "file-name",  TypeVarChar, 50, 3);
//    insertColumnRecord(fh2, 1, "privileged", TypeInt,      4, 4);

    insertColumnRecord(fh2, 2, "table-id",        TypeInt,      4, 1);
    insertColumnRecord(fh2, 2, "column-name",     TypeVarChar, 50, 2);
    insertColumnRecord(fh2, 2, "column-type",     TypeInt,      4, 3);
    insertColumnRecord(fh2, 2, "column-length",   TypeInt,      4, 3);
    insertColumnRecord(fh2, 2, "column-position", TypeInt,      4, 4);

    _rbfm->closeFile(fh2);
    col_rc = 0;

    if(table_rc != 0 && col_rc != 0) return -1;
    return 0;
}
RC RelationManager::deleteCatalog()
{
    _rbfm->destroyFile("Tables.txt");
    _rbfm->destroyFile("Columns.txt");
    remove("tables.stat");

    return 0;
}
RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
  FileHandle fh;
  int maxId, i=0;

   _rbfm->createFile(tableName + ".txt");
   _rbfm->openFile("Tables.txt", fh);
   // get table id
   fseek(stats, 0, SEEK_SET);
   fread(&maxId, sizeof(int), 1, stats);
   maxId = maxId+1;

   // insert table record, system attribute = 1
   insertTableRecord(fh, maxId, tableName, tableName + ".txt");
   // set table id
   fseek(stats, 0, SEEK_SET);
   fwrite(&maxId, sizeof(int), 1, stats);
   fflush(stats);
   _rbfm->closeFile(fh);
   _rbfm->openFile("Columns.txt", fh);

   for(; i < attrs.size(); i++) {
      Attribute attr = attrs.at(i);
      insertColumnRecord(fh, maxId, attr.name, attr.type, attr.length, i+1);
   }

   _rbfm->closeFile(fh);
   return 0;
}

//helper function
RC RelationManager::insertTableRecord(FileHandle &fh, const int tableId, const string name, const string fileName) {

   RID rid;
   int recordSize = 0, offset = 0, nullSize = 1;
   void *record =      malloc(1 + sizeof(int) + sizeof(int) + name.length() + sizeof(int) + fileName.length() + sizeof(int));
   void *finalRecord = malloc(1 + sizeof(int) + sizeof(int) + name.length() + sizeof(int) + fileName.length() + sizeof(int));
   vector<Attribute> recordDescriptor = vec_table();

   unsigned char *null = (unsigned char*) malloc(nullSize);
   memset(null, 0, nullSize);
   memcpy((char *)record + offset, null, nullSize);
   offset += nullSize;
   memcpy((char *)record + offset, &tableId, sizeof(int));
   offset += sizeof(int);
   auto nameLen = name.length();
   memcpy((char *)record + offset, &nameLen, sizeof(int));
   offset += sizeof(int);
   memcpy((char *)record + offset, name.c_str(), name.length());
   offset += name.length();
   auto fileLen = fileName.length();
   memcpy((char *)record + offset, &fileLen, sizeof(int));
   offset += sizeof(int);
   memcpy((char *)record + offset, fileName.c_str(), fileName.length());
   offset += fileName.length();
   recordSize = offset;

   _rbfm->insertRecord(fh, recordDescriptor, record, rid);
   _rbfm->readRecord(fh, recordDescriptor, rid, finalRecord);

   free(null);
   free(finalRecord);
   free(record);
   return 0;
}

//helper function
vector<Attribute> RelationManager::vec_table() {
   vector<Attribute> recordDescriptor;
   Attribute attribute;

   attribute.name = "table-id";
   attribute.type = TypeInt;
   attribute.length = (AttrLength)4;
   recordDescriptor.push_back(attribute);

   attribute.name = "table-name";
   attribute.type = TypeVarChar;
   attribute.length = (AttrLength)50;
   recordDescriptor.push_back(attribute);

   attribute.name = "file-name";
   attribute.type = TypeVarChar;
   attribute.length = (AttrLength)50;
   recordDescriptor.push_back(attribute);

   return recordDescriptor;
}
//helper function
RC RelationManager::insertColumnRecord(FileHandle &fh, const int tableId, const string name, const int colType, const int colLength, const int colPos) {

   RID rid;
   int recordSize = 0;int offset = 0; int nullSize = 1;
   unsigned char *nullsIndicator = (unsigned char *) malloc(nullSize);
   void *record =      malloc(1 + sizeof(int) + sizeof(int) + name.length() + sizeof(int) + sizeof(int) + sizeof(int));
   void *finalRecord = malloc(1 + sizeof(int) + sizeof(int) + name.length() + sizeof(int) + sizeof(int) + sizeof(int));

   vector<Attribute> recordDescriptor = vec_column();

   memset(nullsIndicator, 0, nullSize);
   memcpy((char *)record + offset, nullsIndicator, nullSize);
   offset += nullSize;
   memcpy((char *)record + offset, &tableId, sizeof(int));
   offset += sizeof(int);
   auto nameLen = name.length();
   memcpy((char *)record + offset, &nameLen, sizeof(int));
   offset += sizeof(int);
   memcpy((char *)record + offset, name.c_str(), nameLen);
   offset += nameLen;
   memcpy((char *)record + offset, &colType, sizeof(int));
   offset += sizeof(int);
   memcpy((char *)record + offset, &colLength, sizeof(int));
   offset += sizeof(int);
   memcpy((char *)record + offset, &colPos, sizeof(int));
   offset += sizeof(int);
   recordSize = offset;

   _rbfm->insertRecord(fh, recordDescriptor, record, rid);
   _rbfm->readRecord(fh, recordDescriptor, rid, finalRecord);

   free(nullsIndicator);
   free(finalRecord);
   free(record);

   return 0;
}

//helper function
vector<Attribute> RelationManager::vec_column() {
   vector<Attribute> recordDescriptor;
   Attribute attribute;

   attribute.name = "table-id";
   attribute.type = TypeInt;
   attribute.length = (AttrLength)4;
   recordDescriptor.push_back(attribute);

   attribute.name = "column-name";
   attribute.type = TypeVarChar;
   attribute.length = (AttrLength)50;
   recordDescriptor.push_back(attribute);

   attribute.name = "column-type";
   attribute.type = TypeInt;
   attribute.length = (AttrLength)4;
   recordDescriptor.push_back(attribute);

   attribute.name = "column-length";
   attribute.type = TypeInt;
   attribute.length = (AttrLength)4;
   recordDescriptor.push_back(attribute);

   attribute.name = "column-position";
   attribute.type = TypeInt;
   attribute.length = (AttrLength)4;
   recordDescriptor.push_back(attribute);

   return recordDescriptor;
}