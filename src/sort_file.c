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

/* Utility function to check attributes of sortedFile function */
int checkAttributes(int fieldNo, int bufferSize) {
  if (bufferSize < 3 || bufferSize > BF_BUFFER_SIZE || fieldNo < 0 || fieldNo > 3)
    return 0;
  return 1;
}

/* Custom BF function for initializing->allocating->gettingData */
BF_ErrorCode BF_Block_Init_Allocate_GetData(BF_Block** pBlock, int fileDesc, char** data) {
  BF_Block_Init(pBlock);
  CALL_OR_RETURN(BF_AllocateBlock(fileDesc, *pBlock));
  *data = BF_Block_GetData(*pBlock);
  return BF_OK;
}

/* Custom BF function for allocating->gettingData */
BF_ErrorCode BF_Block_Allocate_GetData(BF_Block** pBlock, int fileDesc, char** data) {
  CALL_OR_RETURN(BF_AllocateBlock(fileDesc, *pBlock));
  *data = BF_Block_GetData(*pBlock);
  return BF_OK;
}

/* Custom BF function for initializing->gettingBlock->gettingData */
BF_ErrorCode BF_Block_Init_GetBlock_GetData(BF_Block** pBlock, int fileDesc, int blockNum, char** data) {
  BF_Block_Init(pBlock);
  CALL_OR_RETURN(BF_GetBlock(fileDesc, blockNum, *pBlock));
  *data = BF_Block_GetData(*pBlock);
  return BF_OK;
}

int getRecordCount(char* data) {
  int numberRecords;
  memcpy(&numberRecords, data, sizeof(int));
  return numberRecords;
}

void setRecordCount(char* data, int numberRecords) {
  memcpy(data, &numberRecords, sizeof(int));
}

void increaseRecordCount(char* data) {
  int numberRecords;
  memcpy(&numberRecords, data, sizeof(int));
  numberRecords++;
  memcpy(data, &numberRecords, sizeof(int));
}

/* 
 Utility function to get records from multiple block arrays, used in quicksort
 we suppose correct use, there is no check for false input 
*/
Record* getSparseRecord(char** chunkTable, int index) {
  int passedRecords=0, i=0, bRecords;           //reminder, int in start of each blockdata shows how many records are in it
  while(1) {                                    //loop through block data to find the block containing the index
    memcpy(&bRecords, chunkTable[i], sizeof(int));
    if (passedRecords+bRecords>=index) {        //index is in this block
      break;
    }
    passedRecords += bRecords;                  //if not in this block, change variables accordingly
    i++;
  }
  Record* mRecord = (Record*)(chunkTable[i]+sizeof(int)+(index-passedRecords-1)*sizeof(Record));
  return mRecord;
}

/* Compares 2 records based on the given fieldNo */
int compareRecords(Record* r1, Record* r2, int fieldNo) {
  if (fieldNo==0) {
    if (r1->id <= r2->id)
      return 1;
  }
  if (fieldNo==1) {
    if (strcmp(r1->name, r2->name)<=0)
      return 1;
  }
  if (fieldNo==2) {
    if (strcmp(r1->surname, r2->surname)<=0)
      return 1;
  }
  if (fieldNo==3) {
    if (strcmp(r1->city, r2->city)<=0)
      return 1;
  }
  return 0;
}

/* Takes last element as pivot and splits for quicksort */
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

BF_ErrorCode internalSort(int input_fileDesc, int temp_fileDesc, int fieldNo, int bufferSize){
  int i, k;
  int bRecords;
  int totalBlocks, currentBlock, totalRecords;
  char** chunkTable;
  BF_Block* tempFileBlock;
  BF_Block_Init(&tempFileBlock);  

  BF_Block** mBlocks = (BF_Block**)malloc(bufferSize*sizeof(BF_Block*));
  for (i = 0; i < bufferSize; i++) {
    BF_Block_Init(&mBlocks[i]);
  }

  chunkTable = (char**)malloc(bufferSize*sizeof(char*));     //chunkTable is an array of pointers to the block data

  CALL_OR_RETURN(BF_GetBlockCounter(input_fileDesc, &totalBlocks));     // Count the total blocks
  
  currentBlock = 1;     // Algorithm for first sorting, inside chunks
  totalRecords = 0;     // counts total records in chunk, used in quicksort
  while (currentBlock < totalBlocks) {
    for(i = 0; (i < bufferSize) && (currentBlock < totalBlocks); i++) {        // Read block data from the chunk blocks
      printf("%d of %d\n", currentBlock, totalBlocks-1);
      CALL_OR_RETURN(BF_GetBlock(input_fileDesc, currentBlock, mBlocks[i]));
      chunkTable[i] = BF_Block_GetData(mBlocks[i]);
      memcpy(&bRecords, chunkTable[i], sizeof(int));
      totalRecords += bRecords;
      currentBlock++;
    }
    quickSort(chunkTable, 1, totalRecords, fieldNo);        // Keep in mind that indexing for records is [1,total]
    for(k = 0; k < i; k++) {
      //to debug by checking a chunk sorted version of unsorted file, uncomment the following line
      //BF_Block_SetDirty(mBlocks[k]);
      CALL_OR_RETURN(BF_AllocateBlock(temp_fileDesc, tempFileBlock));
      char* tempBData = BF_Block_GetData(tempFileBlock);
      memcpy(tempBData, chunkTable[k], BF_BLOCK_SIZE);
      BF_Block_SetDirty(tempFileBlock);
      CALL_OR_RETURN(BF_UnpinBlock(tempFileBlock));
      CALL_OR_RETURN(BF_UnpinBlock(mBlocks[k]));
    }
    totalRecords = 0;
  }   

  BF_Block_Destroy(&tempFileBlock);
  for (i = 0; i < bufferSize; i++) {
    free(mBlocks[i]);
  }
  free(mBlocks);
  free(chunkTable);

  return SR_OK;
}

int countChunks(int totalBlocks, int chunkSize){
    return (totalBlocks/chunkSize) + ((totalBlocks%chunkSize != 0) ? 1:0);
}

BF_ErrorCode recursiveMergeSort(int output_fileDesc, int temp_fileDesc, char** chunkTable, int numChunks, int chunkSize, int fieldNo, int firstBlockIndex, int secondBlockIndex){

    //DC code Start
    //Merging buffersize blocks...
    int tempMerge_fileDesc;
    //Creating a temporary file that will hold the merged blocks.
    char* tempMerge_filename = "tempMergeFile";
    CALL_OR_RETURN(SR_CreateFile(tempMerge_filename));
    CALL_OR_RETURN(SR_OpenFile(tempMerge_filename, &tempMerge_fileDesc));

    BF_Block *firstChunkBlock, *secondChunkBlock;                                                         //Pointers to the first Block of the first and second chunk respectively.
    char **firstChunkBlockData, **secondChunkBlockData;                                                   //Pointers to the data of the first Block of the first and second chunk respectively.

    CALL_OR_RETURN(BF_Block_Init_GetBlock_GetData(&firstChunkBlock, *temp_fileDesc, 0, &firstChunkBlockData)); // Initializing -> gettingBlock from temp file -> gettingData from the block
    CALL_OR_RETURN(BF_Block_Init_GetBlock_GetData(&firstChunkBlock, *temp_fileDesc, chunkSize, &secondChunkBlockData)); // Initializing -> gettingBlock from temp file -> gettingData from the block

    Record minRecord;
    memcpy(&minRecord, firstChunkBlockData + sizeof(int), sizeof(Record));                //First Record is by default the minimum.

    for(int i=0; i< chunkSize; i++){                                                      //For each block in the chunk, repeat...
        Record tempRecordFirstBlock, tempRecordSecondBlock;
        int recordCountFirstBlock, recordCountSecondBlock;
        memcpy(&recordCount, firstChunkBlockData, sizeof(int));                           //Get how many records there are in the blocks.
        memcpy(&recordCount, secondChunkBlockData, sizeof(int));
        for(int j = 0; j < recordCount; j++){
            memcpy(&tempRecordFirstBlock, firstChunkBlockData + sizeof(int) + j* sizeof(Record), sizeof(Record));       //Writing record data of first block in a racord.
            memcpy(&tempRecordSecondBlock, secondChunkBlockData + sizeof(int) + j* sizeof(Record), sizeof(Record));     //Writing record data of second block in a record.
            //Checking if a record in either chunk is smaller than the current min record
            //At the end, insert the minimum record in the new block and repeat for a new record
            if(compareRecords(tempRecordFirstBlock, minRecord, fieldNo)){
                minRecord = tempRecordFirstBlock;
                //NEED TO DELETE RECORD FROM BLOCK HERE
            }
            if(compareRecords(tempRecordSecondBlock, minRecord, fieldNo)){
                minRecord = tempRecordSecondBlock;
                //NEED TO DELETE RECORD FROM BLOCK HERE
            }
        }
        SR_InsertEntry(tempMerge_fileDesc, minRecord);

    }
    if(firstBlockIndex + chunkSize <= numChunks || secondBlockIndex + chunkSize <= numChunks)                           //If there are more chunks which fit in the buffer, call recursively.
        recursiveMergeSort(output_fileDesc, temp_fileDesc, chunkTable, numChunks, chunkSize, fieldNo, firstBlockIndex + chunkSize, secondBlockIndex + chunkSize);
    else                                                                                                                //Else return ok and handle any excess blocks.
        return BF_OK;
    //DC Code End

}

BF_ErrorCode externalSort(int output_fileDesc, int temp_fileDesc, int fieldNo, int bufferSize){
  int i;
  int totalBlocks, totalChunks;
  char* identifier = "SR_File";
  char* mdata;
  char** chunkTable;
  BF_Block* myBlock;
  BF_Block_Init(&myBlock);


  /* Creating metadata block of output file */
  CALL_OR_RETURN(BF_Block_Init_Allocate_GetData(&myBlock, output_fileDesc, &mdata)); // Initializing->allocating->gettingData
  memcpy(mdata, identifier, strlen(identifier)+1);
  BF_Block_SetDirty(myBlock);
  CALL_OR_RETURN(BF_UnpinBlock(myBlock));

  BF_Block** pBlocks = (BF_Block**)malloc(bufferSize*sizeof(BF_Block*));
  for (i = 0; i < bufferSize; i++) {
    BF_Block_Init(&pBlocks[i]);
  }
  chunkTable = (char**)malloc(bufferSize*sizeof(char*));     //chunkTable is an array of pointers to the block data

  CALL_OR_RETURN(BF_GetBlockCounter(temp_fileDesc, &totalBlocks));     // Count the total blocks
  totalChunks = countChunks(totalBlocks-1, bufferSize);
  
  CALL_OR_RETURN(recursiveMergeSort(output_fileDesc, temp_fileDesc, chunkTable, totalChunks, bufferSize, fieldNo, 0, totalChunks));
  

  BF_Block_Destroy(&myBlock);
  for (i = 0; i < bufferSize; i++) {
    free(pBlocks[i]);
  }
  free(pBlocks);
  free(chunkTable);

  return SR_OK;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////

SR_ErrorCode SR_Init() {
  // Your code goes here
  return SR_OK;
}

SR_ErrorCode SR_CreateFile(const char *fileName) {
  // Your code goes here
  int fileDesc;
  char* identifier = "SR_File";
  char* bData;
  
  BF_Block* firstBlock;
  
  /* Creation of file */
  CALL_OR_RETURN(BF_CreateFile(fileName));
  CALL_OR_RETURN(BF_OpenFile(fileName, &fileDesc));
  
  /* Creation of metadata block */
  CALL_OR_RETURN(BF_Block_Init_Allocate_GetData(&firstBlock, fileDesc, &bData)); // Initializing->allocating->gettingData
  memcpy(bData, identifier, strlen(identifier)+1);
  BF_Block_SetDirty(firstBlock);
  CALL_OR_RETURN(BF_UnpinBlock(firstBlock));
  BF_Block_Destroy(&firstBlock);
  
  CALL_OR_RETURN(BF_CloseFile(fileDesc));

  return SR_OK;
}

SR_ErrorCode SR_OpenFile(const char *fileName, int *fileDesc) {
  // Your code goes here
  char* identifier = "SR_File";
  char* checkData;
  
  BF_Block* checkBlock;

  CALL_OR_RETURN(BF_OpenFile(fileName, fileDesc));
  CALL_OR_RETURN(BF_Block_Init_GetBlock_GetData(&checkBlock, *fileDesc, 0, &checkData)); // Initializing->gettingBlock->gettingData

  if (strcmp(identifier, checkData) != 0) {
    CALL_OR_RETURN(BF_UnpinBlock(checkBlock));
    return SR_ERROR;
  }
  CALL_OR_RETURN(BF_UnpinBlock(checkBlock));
  BF_Block_Destroy(&checkBlock);
  
  return SR_OK;
}

SR_ErrorCode SR_CloseFile(int fileDesc) {
  // Your code goes here
  CALL_OR_RETURN(BF_CloseFile(fileDesc));
  
  return SR_OK;
}

SR_ErrorCode SR_InsertEntry(int fileDesc, Record record) {
  // Your code goes here
  int bCount;
  int writtenRecords;
  char* bData;
  BF_Block* myBlock;                                                                 // Records are written to have same length each time, optimizing blocks
  
  CALL_OR_RETURN(BF_GetBlockCounter(fileDesc, &bCount));
  CALL_OR_RETURN(BF_Block_Init_GetBlock_GetData(&myBlock, fileDesc, bCount-1, &bData));

  memcpy(&writtenRecords, bData, sizeof(int));
  if (bCount < 2 || writtenRecords >= (BF_BLOCK_SIZE-sizeof(int))/sizeof(Record)) {  // Either we are on file block 0, or no space in current data block
    CALL_OR_RETURN(BF_UnpinBlock(myBlock));                                          // unpins previous block since we are allocating a new one
    CALL_OR_RETURN(BF_Block_Allocate_GetData(&myBlock, fileDesc, &bData));
    setRecordCount(bData, 0);
  }

  memcpy(&writtenRecords, bData, sizeof(int));
  if ( writtenRecords < (BF_BLOCK_SIZE-sizeof(int))/sizeof(Record) ) {               // Record fits in block
    memcpy(bData + sizeof(int) + writtenRecords*sizeof(Record), &record, sizeof(Record));
    increaseRecordCount(bData);
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
  int input_fileDesc, temp_fileDesc, output_fileDesc;
  char* temp_filename = "tempFile";

  if (!checkAttributes(fieldNo, bufferSize)) {
    return SR_ERROR;
  }

  //DG code 
  CALL_OR_RETURN(SR_CreateFile(temp_filename));     // Temporary file to hold sorted chunks
  CALL_OR_RETURN(SR_OpenFile(temp_filename, &temp_fileDesc));
  CALL_OR_RETURN(SR_OpenFile(input_filename, &input_fileDesc));

  CALL_OR_RETURN(internalSort(input_fileDesc, temp_fileDesc, fieldNo, bufferSize));      // Internal sorting of chunks using quicksort

  CALL_OR_RETURN(SR_CloseFile(input_fileDesc));     // Input file not needed anymore, no changes have been made
  //DG code end

  CALL_OR_RETURN(SR_CreateFile(output_filename));     // Output file
  CALL_OR_RETURN(SR_OpenFile(output_filename, &output_fileDesc));

  CALL_OR_RETURN(externalSort(output_fileDesc, temp_fileDesc, fieldNo, bufferSize));
  
  SR_CloseFile(temp_fileDesc);
  SR_CloseFile(output_fileDesc);

  return SR_OK;
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