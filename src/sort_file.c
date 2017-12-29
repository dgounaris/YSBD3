#include "sort_file.h"
#include "bf.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#define CALL_OR_RETURN(call)  \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      return SR_ERROR;        \
    }                         \
  }                           \

///////////////////////////////////////// Utility Functions /////////////////////////////////////////

int checkAttributes(int fieldNo, int bufferSize){
  if (bufferSize < 3 || bufferSize > BF_BUFFER_SIZE || fieldNo < 0 || fieldNo > 3)
    return 0;
  return 1;
}

BF_ErrorCode BF_Block_Init_Allocate_GetData(BF_Block** pBlock, int fileDesc, char** data){
  BF_Block_Init(pBlock);
  CALL_OR_RETURN(BF_AllocateBlock(fileDesc, *pBlock));
  *data = BF_Block_GetData(*pBlock);
  return BF_OK;
}

BF_ErrorCode BF_Block_Init_GetBlock_GetData(BF_Block** pBlock, int fileDesc, int blockNum, char** data){
  BF_Block_Init(pBlock);
  CALL_OR_RETURN(BF_GetBlock(fileDesc, blockNum, *pBlock));
  *data = BF_Block_GetData(*pBlock);
  return BF_OK;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////

SR_ErrorCode SR_Init() {
  // Your code goes here
  return SR_OK;
}

SR_ErrorCode SR_CreateFile(const char *fileName) {
  // Your code goes here
  int fileDesc;
  char* identifier = "HP_File";
  char* bData;
  
  BF_Block* firstBlock;
  
  /* Creation of file */
  CALL_OR_RETURN(BF_CreateFile(fileName));
  CALL_OR_RETURN(BF_OpenFile(fileName, &fileDesc));
  
  /* Creation of metadata block */
  CALL_OR_RETURN(BF_Block_Init_Allocate_GetData(&firstBlock, fileDesc, &bData)); // Custom function for initializing->allocating->gettingData
  memcpy(bData, identifier, strlen(identifier)+1);
  BF_Block_SetDirty(firstBlock);
  CALL_OR_RETURN(BF_UnpinBlock(firstBlock));
  BF_Block_Destroy(&firstBlock);
  
  CALL_OR_RETURN(BF_CloseFile(fileDesc));

  return SR_OK;
}

SR_ErrorCode SR_OpenFile(const char *fileName, int *fileDesc) {
  // Your code goes here
  char* identifier = "HP_File";
  char* checkData;
  
  BF_Block* checkBlock;
  
  printf("Openfile\n");
  fflush(stdout);
  CALL_OR_RETURN(BF_OpenFile(fileName, fileDesc));
  printf("Openfile end\n");
  printf("Custom\n");
  fflush(stdout);
  CALL_OR_RETURN(BF_Block_Init_GetBlock_GetData(&checkBlock, *fileDesc, 0, &checkData)); // Custom function for initializing->gettingBlock->gettingData
printf("Custom end\n");
  fflush(stdout);

  if (strcmp(identifier, checkData) != 0) {
    CALL_OR_RETURN(BF_UnpinBlock(checkBlock));
    return SR_ERROR;
  }

  return SR_OK;
}

SR_ErrorCode SR_CloseFile(int fileDesc) {
  // Your code goes here
  CALL_OR_RETURN(BF_CloseFile(fileDesc));
  
  return SR_OK;
}

SR_ErrorCode SR_InsertEntry(int fileDesc,	Record record) {
  // Your code goes here
  // records are written to have same length each time, optimizing blocks
  BF_ErrorCode code;
  BF_Block* myBlock;
  BF_Block_Init(&myBlock);
  int bCount;
  char* bData;
  int writtenRecords;
  CALL_OR_RETURN(BF_GetBlockCounter(fileDesc, &bCount));
  CALL_OR_RETURN(BF_GetBlock(fileDesc, bCount-1, myBlock));
  bData = BF_Block_GetData(myBlock);
  memcpy(&writtenRecords, bData, sizeof(int));
  if (bCount<2 || writtenRecords>=(BF_BLOCK_SIZE-sizeof(int))/sizeof(Record)) {
    //either we are on file block 0, or no space in current data block
    //unpins previous block since we are allocating a new one
    CALL_OR_RETURN(BF_UnpinBlock(myBlock));
   CALL_OR_RETURN(BF_AllocateBlock(fileDesc, myBlock));
    bData = BF_Block_GetData(myBlock);
    writtenRecords = 0;
    memcpy(bData, &writtenRecords, sizeof(int));
  }
  memcpy(&writtenRecords, bData, sizeof(int));
  if (writtenRecords<(BF_BLOCK_SIZE-sizeof(int))/sizeof(Record)) { //record fits in block
    memcpy(bData + sizeof(int) + writtenRecords*sizeof(Record), &record, sizeof(Record));
    writtenRecords++;
    memcpy(bData, &writtenRecords, sizeof(int));
  }
  BF_Block_SetDirty(myBlock);
  CALL_OR_RETURN(BF_UnpinBlock(myBlock));
  BF_Block_Destroy(&myBlock);
  
  return SR_OK;
}

SR_ErrorCode SR_SortedFile(
  const char* input_filename,
  const char* output_filename,
  int fieldNo,
  int bufferSize
) {
  // Your code goes here
  if (!checkAttributes(fieldNo, bufferSize)) {
    return SR_ERROR;
  }

  int input_fileDesc, temp_fileDesc;
  char* temp_filename = "tempFile";
  CALL_OR_RETURN(BF_CreateFile(temp_filename));
  CALL_OR_RETURN(BF_OpenFile(temp_filename, &temp_fileDesc));
  BF_Block* tempFileBlock;

  //DG code
  BF_Block** mBlocks = malloc(bufferSize*sizeof(BF_Block*));
  int i;
  for (i=0;i<bufferSize;i++) {
    BF_Block_Init(&mBlocks[i]);
  }
  //chunkTable is an array of pointers to the block data
  char** chunkTable = malloc(bufferSize*sizeof(char*));
  SR_OpenFile(input_filename, &input_fileDesc);
  //count the total blocks
  int totalBlocks;
  CALL_OR_RETURN(BF_GetBlockCounter(input_fileDesc, &totalBlocks));
  //algorithm for first sorting, inside chunks
  int currentBlock = 1;
  int totalRecords = 0; //counts total records in chunk, used in quicksort
  while (currentBlock < totalBlocks) {
    //read block data from the chunk blocks
    for (i=0;i<bufferSize && currentBlock<totalBlocks;i++) {
      printf("%d of %d\n", currentBlock, totalBlocks);
      CALL_OR_RETURN(BF_GetBlock(input_fileDesc, currentBlock, mBlocks[i]));
      chunkTable[i] = BF_Block_GetData(mBlocks[i]);
      int bRecords;
      memcpy(&bRecords, chunkTable[i], sizeof(int));
      totalRecords += bRecords;
      currentBlock++;
    }
    //keep in mind that indexing for records is [1,total]
    quickSort(chunkTable, 1, totalRecords, fieldNo);
    int k;
    for (k=0;k<i;k++) {
      //to debug by checking a chunk sorted version of unsorted file, uncomment the following line
      //BF_Block_SetDirty(mBlocks[k]);
      CALL_OR_RETURN(BF_AllocateBlock(temp_fileDesc, tempFileBlock));
      char* tempBData = BF_Block_GetData(tempFileBlock);
      memcpy(tempBData, chunkTable[k], BF_BLOCK_SIZE);
      CALL_OR_RETURN(BF_UnpinBlock(tempFileBlock));
      CALL_OR_RETURN(BF_UnpinBlock(mBlocks[k]));
    }
    totalRecords = 0;
  }
  //DG code end

  SR_CloseFile(temp_fileDesc);
  SR_CloseFile(input_fileDesc);
  return SR_OK;
}

//utility function to get records from multiple block arrays, used in quicksort
//we suppose correct use, there is no check for false input
Record* getSparseRecord(char** chunkTable, int index) {
  //reminder, int in start of each blockdata shows how many records are in it
  int passedRecords=0, i=0, bRecords;
  while(1) { //loop through block data to find the block containing the index
    memcpy(&bRecords, chunkTable[i], sizeof(int));
    if (passedRecords+bRecords>=index) { //index is in this block
      break;
    }
    //if not in this block, change variables accordingly
    passedRecords += bRecords;
    i++;
  }
  Record* mRecord = (Record*)(chunkTable[i]+sizeof(int)+(index-passedRecords-1)*sizeof(Record));
  return mRecord;
}

int compareRecords(Record* r1, Record* r2, int fieldNo) {
  if (fieldNo==0) {
    if (r1->id <= r2->id) {
      return 1;
    }
  }
  if (fieldNo==1) {
    if (strcmp(r1->name, r2->name)<=0) {
      return 1;
    }
  }
  if (fieldNo==2) {
    if (strcmp(r1->surname, r2->surname)<=0) {
      return 1;
    }
  }
  if (fieldNo==3) {
    if (strcmp(r1->city, r2->city)<=0) {
      return 1;
    }
  }
  return 0;
}

//takes last element as pivot and splits for quicksort
int partition(char** chunkTable, int low, int high, int fieldNo) {
  Record* pivot = getSparseRecord(chunkTable, high);
  Record* tempRec = malloc(sizeof(Record));
  Record* lowRec;
  int i=low;
  int j;
  for (j=low;j<=high-1;j++) {
    Record* currRecord = getSparseRecord(chunkTable, j);
    if (compareRecords(currRecord, pivot, fieldNo)) {
      lowRec = getSparseRecord(chunkTable, i);
      memcpy(tempRec, currRecord, sizeof(Record));
      memcpy(currRecord, lowRec, sizeof(Record));
      memcpy(lowRec, tempRec, sizeof(Record));
      i++;
    }
  }
  lowRec = getSparseRecord(chunkTable, i);
  memcpy(tempRec, pivot, sizeof(Record));
  memcpy(pivot, lowRec, sizeof(Record));
  memcpy(lowRec, tempRec, sizeof(Record));
  return i;
}

void quickSort(char** chunkTable, int low, int high, int fieldNo) {
  if (low < high) {
    int pIndex = partition(chunkTable, low, high, fieldNo);
    quickSort(chunkTable, low, pIndex-1, fieldNo);
    quickSort(chunkTable, pIndex+1, high, fieldNo);
  }
}

SR_ErrorCode SR_PrintAllEntries(int fileDesc) {
  // Your code goes here
  int totalBlocks;
  int blockRecs;
  int i, j;
  Record rec;

  BF_Block* myBlock;
  BF_Block_Init(&myBlock);
  
  CALL_OR_RETURN(BF_GetBlockCounter(fileDesc, &totalBlocks));
  
  for (i = 1; i < totalBlocks; i++) {
    CALL_OR_RETURN(BF_GetBlock(fileDesc, i, myBlock));
    char* bData = BF_Block_GetData(myBlock);
    
    memcpy(&blockRecs, bData, sizeof(int));

    for (j = 0; j < blockRecs; j++) {
      Record rec;
      memcpy(&rec, bData + sizeof(int) + j*sizeof(Record), sizeof(Record));
      printf("%d, %s, %s, %s\n",
        rec.id,
        rec.name,
        rec.surname,
        rec.city
      );
    }
    
    CALL_OR_RETURN(BF_UnpinBlock(myBlock));
  }
  BF_Block_Destroy(&myBlock);
  
  return SR_OK;
}
