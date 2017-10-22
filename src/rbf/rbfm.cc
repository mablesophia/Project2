#include "rbfm.h"
#include<cmath>
#include<iostream>
RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;
PagedFileManager * RecordBasedFileManager::pfm = PagedFileManager::instance();

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)

    	_rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    if(pfm->createFile(fileName) != 0) return -1;
	return 0;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	if(pfm->destroyFile(fileName) != 0) return -1;
	return 0;
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
	if(pfm->openFile(fileName, fileHandle) != 0) return -1;
	if (fileHandle.getNumberOfPages() == 0) {
		Page page;

		fileHandle.appendPage(page.buf);

	}
	return 0;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    if(pfm->closeFile(fileHandle) != 0) return -1;
	return 0;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
	Record record;
	record.initRecord(recordDescriptor, data);
	int res = findLoc(record.size, fileHandle);
	if (res != -1) {
		rid.pageNum = res;
		char* temp = (char*)malloc(PAGE_SIZE + 1);
		temp[PAGE_SIZE] = '\0';
		fileHandle.readPage(res - 1, temp);
		int sp, slot_num, headPtr;
		Slot slot;
		slot.len = record.size;
		int offset = PAGE_SIZE-sizeof(int);
		memcpy(&sp, temp + offset, sizeof(int));
		sp += record.size;
		sp += sizeof(slot);
		memcpy(temp + offset, &sp, sizeof(int));
		offset -= sizeof(int);
		memcpy(&slot_num, temp + offset, sizeof(int));
		rid.slotNum = ++slot_num;
		memcpy(temp + offset, &slot_num, sizeof(int));
		offset -= sizeof(int);
		memcpy(&headPtr, temp + offset, sizeof(int));
		memcpy(temp + headPtr, record.buffer, slot.len);
		headPtr += slot.len;
		memcpy(temp + offset, &headPtr, sizeof(int));
		offset -= sizeof(slot) * slot_num;
		slot.offset = headPtr - slot.len;
		memcpy(temp + offset, &slot.offset, sizeof(int));
		offset += sizeof(int);
		memcpy(temp + offset, &slot.len, sizeof(int));
		fileHandle.writePage(res - 1, temp);
		free(temp);
	} else {
		Page pg;
		Slot slot;
		rid.pageNum = fileHandle.getNumberOfPages() + 1;
		rid.slotNum = 1;
		int offset = PAGE_SIZE - sizeof(int);
		int sp, slot_num, headPtr;
		memcpy(&sp, pg.buf + offset, sizeof(int));
		sp += record.size;
		sp += sizeof(slot);
		memcpy(pg.buf + offset, &sp, sizeof(int));
		offset -= sizeof(int);
		slot_num = rid.slotNum;
		memcpy(pg.buf + offset, &slot_num, sizeof(int));
		offset -= sizeof(int);
		memcpy(&headPtr, pg.buf + offset, sizeof(int));
		slot.len = record.size;
		memcpy(pg.buf + headPtr, record.buffer, slot.len);
		slot.offset = headPtr;
		headPtr += slot.len;
		memcpy(pg.buf + offset, &headPtr, sizeof(int));
		offset -= sizeof(slot);
		memcpy(pg.buf + offset, &slot.offset, sizeof(int));
		offset += sizeof(int);
		memcpy(pg.buf + offset, &slot.len, sizeof(int));
		fileHandle.appendPage(pg.buf);
	}
	return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	Page pg;
    fileHandle.readPage(rid.pageNum - 1, pg.buf);
    int offset = PAGE_SIZE-sizeof(int);
    memcpy(&pg.free_sp, pg.buf + offset, sizeof(int));
    offset -= sizeof(int);
    memcpy(&pg.snum, pg.buf + offset, sizeof(int));
    offset -= sizeof(int);
    memcpy(&pg.headPtr, pg.buf + offset, sizeof(int));
    offset -= rid.slotNum * sizeof(Slot);
    int rlen, roffset;
    memcpy(&roffset, pg.buf + offset, sizeof(int));
    offset += sizeof(int);
    memcpy(&rlen, pg.buf + offset, sizeof(int));
    memcpy(data, pg.buf + roffset, rlen);
    memset(data + rlen, '\0', 1);
	return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    int offset = 0;
    int null_byte = ceil(1.0 * recordDescriptor.size() / 8);
    unsigned char* null_indicator = (unsigned char*)malloc(null_byte);
    memcpy(null_indicator,data,null_byte);
    offset = null_byte;
    int null_byte_num;
    int null_bit_num;
    bool flag = false;
    for (int i = 0; i < recordDescriptor.size(); i++) {
    	null_byte_num = i / 8;
    	null_bit_num = i % 8;
    	flag = null_indicator[null_byte_num] & (1 << (7-null_bit_num));
    	flag = null_indicator[null_byte_num] & (1 << (7-null_bit_num));
    	if (flag) {
    		cout << recordDescriptor[i].name << ":" << "NULL" << endl;
    	} else {
    		switch(recordDescriptor[i].type) {
    		case TypeInt:
    			{
    				int value;
    				memcpy(&value,(char*)data + offset,recordDescriptor[i].length);
    				offset += recordDescriptor[i].length;
    				cout << recordDescriptor[i].name << ":" << value << endl;
    				break;
    			}
    		case TypeReal:
    			{
    				float value;
    				memcpy(&value,(char*)data + offset,recordDescriptor[i].length);
    				offset += recordDescriptor[i].length;
    				cout << recordDescriptor[i].name << ":" << value << endl;
    				break;
    			}
    		case TypeVarChar:
    			{
    				int len;
    				memcpy(&len,(char*)data + offset, 4 * sizeof(char));
    				offset += 4 * sizeof(char);
    				char * vc = (char*)malloc(len + 1);
    				vc[len] = '\0';
    				memcpy(vc, (char*)data + offset, len);
    				offset += len;
    				cout << recordDescriptor[i].name << ":" << vc << endl;
    				free(vc);
    				break;
    			}
    		}
    	}
    }
    free(null_indicator);
	return 0;
}

RC RecordBasedFileManager::findLoc(int size, FileHandle & fh) {
	int curr = fh.getNumberOfPages();
	int sp;
	char* temp = (char*)malloc(PAGE_SIZE + 1);
	temp[PAGE_SIZE] = '\0';
	if(fh.readPage(curr - 1, temp) == 0) {
		memcpy(&sp, temp + PAGE_SIZE - sizeof(int), sizeof(int));
		if(PAGE_SIZE - sp >= size) {
			free(temp);
			return curr;
		}
		else {
			for(int i = 1; i < curr; i++) {
				if(fh.readPage(i - 1, temp) == 0) {
					memcpy(&sp, temp + PAGE_SIZE - sizeof(int), sizeof(int));
					if(PAGE_SIZE - sp >= size) {
						free(temp);
						return i;
					}
				}
			}
		}
	}
	free(temp);
	return -1;
}

Page::Page() {
	headPtr = 0;
	snum = 0;
	free_sp = 3 * sizeof(int);
	buf = (char*) malloc(PAGE_SIZE + 1);
	buf[PAGE_SIZE] = '\0';
	int offset = PAGE_SIZE - sizeof(int);
	memcpy(buf + offset, &free_sp, sizeof(int));
	offset -= sizeof(int);
	memcpy(buf + offset, &snum, sizeof(int));
	offset -= sizeof(int);
	memcpy(buf + offset, &headPtr, sizeof(int));
	memset(buf, 0, offset);
	}
Page::~Page() {
	free(buf);
	}

Record::Record() {
	size = 0;
}
Record::~Record() {
	free(buffer);
}

void Record::initRecord(const vector<Attribute> &recordDescriptor, const void *data) {
	char* buf = (char*)malloc(PAGE_SIZE + 1);
	buf[PAGE_SIZE] = '\0';
	record_descriptor = recordDescriptor;
	null_byte = ceil(1.0 * record_descriptor.size() / 8);
	null_indicator = (unsigned char *)malloc(null_byte);
	memcpy(null_indicator, data, null_byte);
	int null_byte_num = 0;
	int null_bit_num = 0;
	bool flag = false;
	int offset = 0;
	memcpy((char *)buf + offset, null_indicator, null_byte);
	offset += null_byte;
	for (int i = 0; i < record_descriptor.size(); i++) {
		null_byte_num = i / 8;
		null_bit_num = i % 8;
		flag = null_indicator[null_byte_num] & (1 << (7-null_bit_num));
		if(!flag) {
			switch(record_descriptor[i].type) {
			case TypeInt:
			{
				memcpy(buf + offset,(char*)data+offset,recordDescriptor[i].length);
				offset += recordDescriptor[i].length;
				break;
			}
			case TypeReal:
			{
				memcpy(buf + offset,(char*)data+offset,recordDescriptor[i].length);
				offset += recordDescriptor[i].length;
				break;
			}
			case TypeVarChar:
			{
				int temp;
				memcpy(buf + offset,(char*)data+offset,sizeof(int));
				memcpy(&temp,(char*)data+offset,sizeof(int));
				offset += sizeof(int);
				memcpy(buf + offset, (char*)data + offset, temp);
				offset += temp;
				break;
			}
			}
		}
	}
	size = null_byte + offset + 4;
	buffer = (char*)malloc(size + 1);
	buffer[size] = '\0';
	memcpy(buffer, buf, size);
	free(null_indicator);
	free(buf);
}
