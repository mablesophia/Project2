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
    int localSlotNum, localOffset;
    localOffset = roffset;
    while(true) {
		if(localOffset >= PAGE_SIZE || localOffset < 0) {
			int num = localOffset / PAGE_SIZE;
			fileHandle.readPage(rid.pageNum - 1 + num, pg.buf);
			localSlotNum = localOffset % PAGE_SIZE;
			if(localSlotNum < 0) localSlotNum += PAGE_SIZE;
			memcpy(&localOffset, pg.buf + PAGE_SIZE - 3 * sizeof(int) - localSlotNum * sizeof(Slot), sizeof(int));
		} else {
			break;
		}
    }
    memcpy(data, pg.buf + localOffset, rlen);
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
RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid) {
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
	int invalidNum = -1;
	memcpy(&roffset, pg.buf + offset, sizeof(int));
	memcpy(pg.buf + offset, &invalidNum, sizeof(int));
	offset += sizeof(int);
	memcpy(&rlen, pg.buf + offset, sizeof(int));
	if(roffset < 0 || roffset > PAGE_SIZE) {
		rlen = 0;
		memcpy(pg.buf + offset, &rlen, sizeof(int));
		fileHandle.writePage(rid.pageNum - 1, pg.buf);
		RID newRid;
		newRid.slotNum = roffset % PAGE_SIZE;
		newRid.pageNum = roffset / PAGE_SIZE + rid.pageNum;
		deleteRecord(fileHandle, recordDescriptor, newRid);
		return 0;
	}
	pg.free_sp -= rlen;
	memcpy(pg.buf + PAGE_SIZE - sizeof(int), &pg.free_sp, sizeof(int));

	char* tmp = (char*)malloc(pg.headPtr - roffset - rlen + 1);
	tmp[pg.headPtr - roffset - rlen] = '\0';
	memcpy(tmp, pg.buf + roffset + rlen, pg.headPtr - roffset - rlen);
	memcpy(pg.buf + roffset, tmp, pg.headPtr - roffset - rlen);
//	memmove(pg.buf + roffset, pg.buf + roffset + rlen, pg.headPtr - roffset - rlen);
	memset(pg.buf + pg.headPtr - rlen, 0, rlen);
	pg.headPtr -= rlen;
	memcpy(pg.buf + PAGE_SIZE - 3 * sizeof(int), &pg.headPtr, sizeof(int));
	int temp = 0;
	memcpy(pg.buf + offset, &temp, sizeof(int));
	offset = PAGE_SIZE - 3 * sizeof(int);
	for(int i = 0; i < pg.snum; i++) {
		offset -= sizeof(Slot);
		memcpy(&temp, pg.buf + offset, sizeof(int));
		if(temp > roffset && temp > 0 && temp < PAGE_SIZE) temp -= rlen;
		memcpy(pg.buf + offset, &temp, sizeof(int));
	}

	fileHandle.writePage(rid.pageNum - 1, pg.buf);
	free(tmp);
	return 0;
}

RC RecordBasedFileManager:: updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid) {
	Page pg;
	int rlen, roffset, offset;
	Record r;
	r.initRecord(recordDescriptor, data);
	getPageAndSlotDetails(fileHandle, pg, rid, rlen, roffset, offset);
	if(r.size <= rlen) {
		int num = 0;
		RID newRid;
//		bool flag = true;
		int tempSN = rid.slotNum;
		int tempPN = rid.pageNum;
		newRid.pageNum = tempPN;
		newRid.slotNum = tempSN;
		while(true) {
			if (roffset >= PAGE_SIZE || roffset < 0) {
				memcpy(pg.buf + PAGE_SIZE - 12 - sizeof(Slot) * tempSN + sizeof(int), &r.size, sizeof(int));
				fileHandle.writePage(tempPN - 1, pg.buf);
				num += roffset / PAGE_SIZE;
				newRid.slotNum = roffset % PAGE_SIZE;
				newRid.pageNum = roffset / PAGE_SIZE + tempPN;
				tempSN = newRid.slotNum;
				tempPN = newRid.pageNum;
				fileHandle.readPage(newRid.pageNum - 1, pg.buf);
//				getPageInfo(fileHandle, pg, rid.pageNum - 1 + num, offset);
//				memcpy(pg.buf + offset - roffset % PAGE_SIZE * sizeof(Slot) + sizeof(int), &r.size, sizeof(int));
				memcpy(&roffset, pg.buf + PAGE_SIZE - 3 * sizeof(int) - roffset % PAGE_SIZE * sizeof(Slot), sizeof(int));
			} else break;
		}
		getPageInfo(fileHandle, pg, rid.pageNum - 1 + num, offset);
		memcpy(pg.buf + 4084 - newRid.slotNum * sizeof(Slot) + sizeof(int), &r.size, sizeof(int));

		int diff = rlen - r.size;
		pg.free_sp -= diff;
		memcpy(pg.buf + PAGE_SIZE - sizeof(int), &pg.free_sp, sizeof(int));

		memcpy(pg.buf + roffset, r.buffer, r.size);
		memmove(pg.buf + roffset + r.size, pg.buf + roffset + rlen, pg.headPtr - roffset - rlen);
		memset(pg.buf + pg.headPtr - diff, 0, diff);

		pg.headPtr -= diff;
		memcpy(pg.buf + PAGE_SIZE - 3 * sizeof(int), &pg.headPtr, sizeof(int));


		int temp = 0;
		offset = PAGE_SIZE - 3 * sizeof(int);
		for(int i = 0; i < pg.snum; i++) {
			offset -= sizeof(Slot);
			memcpy(&temp, pg.buf + offset, sizeof(int));
			if(temp > roffset && temp > 0 && temp < PAGE_SIZE) temp -= diff;
			memcpy(pg.buf + offset, &temp, sizeof(int));
		}
		fileHandle.writePage(rid.pageNum - 1 + num, pg.buf);
	} else {
		int num = 0;
		RID newRid;
//		bool flag = true;
		int tempSN = rid.slotNum;
		int tempPN = rid.pageNum;
		newRid.pageNum = tempPN;
		newRid.slotNum = tempSN;
		while(true) {
			if (roffset >= PAGE_SIZE || roffset < 0) {
				memcpy(pg.buf + PAGE_SIZE - 12 - sizeof(Slot) * tempSN + sizeof(int), &r.size, sizeof(int));
				fileHandle.writePage(tempPN - 1, pg.buf);
				num += roffset / PAGE_SIZE;
				newRid.slotNum = roffset % PAGE_SIZE;
				newRid.pageNum = roffset / PAGE_SIZE + tempPN;
				fileHandle.readPage(newRid.pageNum - 1, pg.buf);
				memcpy(&roffset, pg.buf + PAGE_SIZE - 3 * sizeof(int) - roffset % PAGE_SIZE * sizeof(Slot), sizeof(int));
				tempSN = newRid.slotNum;
				tempPN = newRid.pageNum;
			} else break;
		}
		deleteRecord(fileHandle, recordDescriptor, newRid);
		memcpy(pg.buf + PAGE_SIZE - 12 - sizeof(Slot) * tempSN + sizeof(int), &r.size, sizeof(int));
		getPageInfo(fileHandle, pg, rid.pageNum - 1 + num, offset);


//		memcpy(pg.buf + PAGE_SIZE - 12 - sizeof(Slot) * rid.slotNum, &roffset, sizeof(int));
//		fileHandle.writePage(rid.pageNum - 1, pg.buf);

//		getPageAndSlotDetails(fileHandle, pg, rid, rlen, roffset, offset);
		if (PAGE_SIZE - pg.free_sp >= r.size) {
			memcpy(pg.buf + pg.headPtr, r.buffer, r.size);
			rlen = r.size;
			pg.free_sp += rlen;
			roffset = pg.headPtr;
			pg.headPtr += rlen;
			memcpy(pg.buf + 4084 - newRid.slotNum * sizeof(Slot) + sizeof(int), &rlen, sizeof(int));
//			offset -= sizeof(int);
			memcpy(pg.buf + 4084 - newRid.slotNum * sizeof(Slot), &roffset, sizeof(int));
			memcpy(pg.buf + PAGE_SIZE - 3 * sizeof(int), &pg.headPtr, sizeof(int));
			memcpy(pg.buf + PAGE_SIZE - sizeof(int), &pg.free_sp, sizeof(int));
			fileHandle.writePage(rid.pageNum - 1 + num, pg.buf);
		} else {
			RID newRid2;
			insertRecord(fileHandle, recordDescriptor, r.buffer, newRid2);
//			char* tmp = (char*)malloc(PAGE_SIZE + 1);
//			tmp[PAGE_SIZE] = '\0';
//			fileHandle.readPage(newRid.pageNum - 1, tmp);
			int localSlot;
//			memcpy(&localSlot, tmp + PAGE_SIZE - 3 * sizeof(int) - newRid.slotNum * sizeof(Slot), sizeof(int));
			localSlot = newRid2.slotNum;
			roffset = (newRid2.pageNum - rid.pageNum) * PAGE_SIZE + localSlot;
			rlen = r.size;
			memcpy(pg.buf + 4084 - newRid.slotNum * sizeof(Slot) + sizeof(int), &rlen, sizeof(int));
			offset -= sizeof(int);
			memcpy(pg.buf + 4084 - newRid.slotNum * sizeof(Slot), &roffset, sizeof(int));
			fileHandle.writePage(rid.pageNum - 1 + num, pg.buf);
//			free(tmp);
		}
	}

	return 0;
}

RC RecordBasedFileManager::findLoc(int size, FileHandle & fh) {
	int curr = fh.getNumberOfPages();
	int sp;
	char* temp = (char*)malloc(PAGE_SIZE + 1);
	temp[PAGE_SIZE] = '\0';
	if(fh.readPage(curr - 1, temp) == 0) {
		memcpy(&sp, temp + PAGE_SIZE - sizeof(int), sizeof(int));
		if(PAGE_SIZE - sp >= size + sizeof(Slot)) {
			free(temp);
			return curr;
		}
		else {
			for(int i = 1; i < curr; i++) {
				if(fh.readPage(i - 1, temp) == 0) {
					memcpy(&sp, temp + PAGE_SIZE - sizeof(int), sizeof(int));
					if(PAGE_SIZE - sp >= size + sizeof(Slot)) {
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


void RecordBasedFileManager::getPageInfo(FileHandle &fileHandle, Page &pg, PageNum num, int &offset) {
	fileHandle.readPage(num, pg.buf);
	offset = PAGE_SIZE-sizeof(int);
	memcpy(&pg.free_sp, pg.buf + offset, sizeof(int));
	offset -= sizeof(int);
	memcpy(&pg.snum, pg.buf + offset, sizeof(int));
	offset -= sizeof(int);
	memcpy(&pg.headPtr, pg.buf + offset, sizeof(int));
	if (pg.headPtr != pg.free_sp - pg.snum * 8 - 12) pg.headPtr = pg.free_sp - pg.snum * 8 - 12;
}
//void RecordBasedFileManager::getAnotherPageAndRecordDetails(FileHandle &fileHandle, Page &pg, PageNum num, int &rlen, int &roffset, int &offset) {
//	getPageInfo(fileHandle, pg, num, offset);
//
//}

void RecordBasedFileManager::getPageAndSlotDetails(FileHandle &fileHandle, Page &pg, RID rid, int &rlen, int &roffset, int &offset) {
//	fileHandle.readPage(rid.pageNum - 1, pg.buf);
	int num = rid.pageNum - 1;
	getPageInfo(fileHandle, pg, num, offset);
	offset -= rid.slotNum * sizeof(Slot);
	memcpy(&roffset, pg.buf + offset, sizeof(int));
	offset += sizeof(int);
	memcpy(&rlen, pg.buf + offset, sizeof(int));
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
	size = null_byte + offset;
	buffer = (char*)malloc(size + 1);
	buffer[size] = '\0';
	memcpy(buffer, buf, size);
	free(null_indicator);
	free(buf);
}
