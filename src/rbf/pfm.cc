#include "pfm.h"
#include<iostream>
#include<sys/stat.h>



PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
    FILE * file;
	// check if file exists
    struct stat buf;
	if(stat(fileName.c_str(), &buf) == 0) return -1;
	//create file
	file = fopen(fileName.c_str(), "wb");
	fclose(file);
	return 0;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
	if (remove(fileName.c_str()) != 0) return -1;
	else return 0;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    FILE * file;
	file = fopen(fileName.c_str(),"rb+");
	//check existence
	if(!file) return -1;
	//check if fileHandle used
	if (fileHandle.getFile() != NULL) return -1;
	fileHandle.setFile(file);

	// for further implementation
//		if(fileHandle.getNumberOfPages() == 0) {
//		void * data = malloc(sizeof(unsigned) * 3);
//		int offset = 0;
//		int * temp;
//		*temp = fileHandle.readPageCounter;
//		memcpy(data + offset, temp, sizeof(unsigned));
//		*temp = fileHandle.writePageCounter;
//		offset += sizeof(unsigned);
//		memcpy(data + offset, temp, sizeof(unsigned));
//		offset += sizeof(unsigned);
//		*temp = fileHandle.appendPageCounter;
//		memcpy(data + offset, temp, sizeof(unsigned));
//		fileHandle.appendPage(data);
//		fileHandle.appendPageCounter--;
//	} else {
//		void * data = malloc(sizeof(unsigned) * 3);
//		fread(data, sizeof(unsigned), 3, file);
//		int offset = 0;
//		int * temp;
//		*temp = fileHandle.readPageCounter;
//		memcpy(temp, data + offset, sizeof(unsigned));
//		*temp = fileHandle.writePageCounter;
//		offset += sizeof(unsigned);
//		memcpy(temp, data + offset, sizeof(unsigned));
//		offset += sizeof(unsigned);
//		*temp = fileHandle.appendPageCounter;
//		memcpy(temp, data + offset, sizeof(unsigned));
//	}

	return 0;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	FILE * fn = fileHandle.getFile();
	if (fn == NULL) return -1;
//	char * data = (char*)malloc(sizeof(unsigned) * 3);
//	int offset = 0;
//	unsigned int * temp;
//	temp = &fileHandle.readPageCounter;
//	memcpy(data + offset, temp, sizeof(unsigned));
//	temp = &fileHandle.writePageCounter;
//	offset += sizeof(unsigned);
//	memcpy(data + offset, temp, sizeof(unsigned));
//	offset += sizeof(unsigned);
//	temp = &fileHandle.appendPageCounter;
//	memcpy(data + offset, temp, sizeof(unsigned));
//	fileHandle.writePage(-1, data);
	fclose(fn);
	fileHandle.setFile(NULL);
    return 0;
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    fileName = NULL;
}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
	// check if page(0 based) exists
	if (pageNum >= getNumberOfPages()) return -1;
	//read file at the page pos
	fseek(fileName, PAGE_SIZE * pageNum, SEEK_SET);
	fread(data, sizeof(char), PAGE_SIZE, fileName);
	if (ferror(fileName)) return -1;
	readPageCounter++;
    return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	// check if page(0 based) exists
	if (pageNum >= getNumberOfPages()) return -1;
	//write
	fseek(fileName, PAGE_SIZE * pageNum, SEEK_SET);
	fwrite(data, sizeof(char), PAGE_SIZE, fileName);
	if (ferror(fileName)) return -1;
	writePageCounter++;
    return 0;
}


RC FileHandle::appendPage(const void *data)
{
	fseek(fileName, 0, SEEK_END);
	fwrite(data, sizeof(char), PAGE_SIZE, fileName);
	if (ferror(fileName)) return -1;
	appendPageCounter++;
    return 0;
}


unsigned FileHandle::getNumberOfPages()
{
	int size = 0;
	fseek(fileName, 0, SEEK_END);
	size = ftell(fileName);
	fseek(fileName, 0, SEEK_SET);
    return size / PAGE_SIZE;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount = readPageCounter;
	writePageCount = writePageCounter;
	appendPageCount = appendPageCounter;
    return 0;
}

FILE * FileHandle::getFile() {
	return fileName;
}

void FileHandle::setFile(FILE * fn) {
	fileName = fn;
}
