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

//////////////////////////////////////////// Utility Functions /////////////////////////////////////////////

/* 
 * Doc: Checks attributes of SR_sortedFile function.
 */
int checkAttributes(int fieldNo, int bufferSize) {
  if (bufferSize < 3 || bufferSize > BF_BUFFER_SIZE || fieldNo < 0 || fieldNo > 3)
    return 0;
  return 1;
}

/* 
 * Doc: Custom function for initializing->allocating->gettingData.
 */
BF_ErrorCode BF_Block_Init_Allocate_GetData(BF_Block** pBlock, int fileDesc, char** data) {
  BF_Block_Init(pBlock);
  CALL_OR_RETURN(BF_AllocateBlock(fileDesc, *pBlock));
  *data = BF_Block_GetData(*pBlock);
  return BF_OK;
}

/* 
 * Doc: Custom function for allocating->gettingData.
 */
BF_ErrorCode BF_Block_Allocate_GetData(BF_Block** pBlock, int fileDesc, char** data) {
  CALL_OR_RETURN(BF_AllocateBlock(fileDesc, *pBlock));
  *data = BF_Block_GetData(*pBlock);
  return BF_OK;
}

/* 
 * Doc: Custom function for initializing->gettingBlock->gettingData.
 */
BF_ErrorCode BF_Block_Init_GetBlock_GetData(BF_Block** pBlock, int fileDesc, int blockNum, char** data) {
  BF_Block_Init(pBlock);
  CALL_OR_RETURN(BF_GetBlock(fileDesc, blockNum, *pBlock));
  *data = BF_Block_GetData(*pBlock);
  return BF_OK;
}

/* 
 * Doc: Custom function for GettingBlock->GettingData.
 */
BF_ErrorCode BF_Block_GetBlock_GetData(BF_Block **pBlock, int fileDesc, int blockNum, char** data){
    CALL_OR_RETURN(BF_GetBlock(fileDesc, blockNum, *pBlock));
    *data = BF_Block_GetData(*pBlock);
    return BF_OK;
}

/* 
 * Doc: Returns the record count of the given data.
 */
int getRecordCount(char* data) {
  int numberRecords;
  memcpy(&numberRecords, data, sizeof(int));
  return numberRecords;
}

/* 
 * Doc: Sets the record count to numberRecords.
 */
void setRecordCount(char* data, int numberRecords) {
  memcpy(data, &numberRecords, sizeof(int));
}

/* 
 * Doc: Increase the record count by 1.
 */
void increaseRecordCount(char* data) {
  int numberRecords;
  memcpy(&numberRecords, data, sizeof(int));
  numberRecords++;
  memcpy(data, &numberRecords, sizeof(int));
}

//////////////////////////////////////////// Internal Sorting Functions /////////////////////////////////////////////

/* 
 * Doc: Utility function to get records from multiple block arrays, used in quicksort 
 *      we suppose correct use, there is no check for false input.
 */
Record* getSparseRecord(char** dataTable, int index) {
  int passedRecords=0, i=0, bRecords;           //reminder, int in start of each blockdata shows how many records are in it
  while(1) {                                    //loop through block data to find the block containing the index
    memcpy(&bRecords, dataTable[i], sizeof(int));
    if (passedRecords+bRecords>=index) {        //index is in this block
      break;
    }
    passedRecords += bRecords;                  //if not in this block, change variables accordingly
    i++;
  }
  Record* mRecord = (Record*)(dataTable[i]+sizeof(int)+(index-passedRecords-1)*sizeof(Record));
  return mRecord;
}

/* 
 * Doc: Compares 2 records based on the given fieldNo.
 */
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

/* 
 * Doc: Takes last element as pivot and splits for quicksort.
 */
int partition(char** dataTable, int low, int high, int fieldNo) {
  Record* pivot = getSparseRecord(dataTable, high);
  Record* tempRec = malloc(sizeof(Record));
  Record* lowRec;
  int i=low;
  int j;
  for (j = low;j <= (high - 1); j++) {
    Record* currRecord = getSparseRecord(dataTable, j);
    if (compareRecords(currRecord, pivot, fieldNo)) {
      lowRec = getSparseRecord(dataTable, i);
      memcpy(tempRec, currRecord, sizeof(Record));
      memcpy(currRecord, lowRec, sizeof(Record));
      memcpy(lowRec, tempRec, sizeof(Record));
      i++;
    }
  }
  lowRec = getSparseRecord(dataTable, i);
  memcpy(tempRec, pivot, sizeof(Record));
  memcpy(pivot, lowRec, sizeof(Record));
  memcpy(lowRec, tempRec, sizeof(Record));
  return i;
}

void quickSort(char** dataTable, int low, int high, int fieldNo) {
  if (low < high) {
    int pIndex = partition(dataTable, low, high, fieldNo);
    quickSort(dataTable, low, pIndex-1, fieldNo);
    quickSort(dataTable, pIndex+1, high, fieldNo);
  }
}

/*
 * Doc: Sorts the chunks internally using the quicksort algorithm.
 */
BF_ErrorCode internalSort(int input_fileDesc, int temp_fileDesc, int fieldNo, int bufferSize){
  int i, k;
  int bRecords;
  int totalBlocks, currentBlock, totalRecords;
  char** dataTable;
  BF_Block* tempFileBlock;
  BF_Block_Init(&tempFileBlock);  

  BF_Block** mBlocks = (BF_Block**)malloc(bufferSize*sizeof(BF_Block*));
  for (i = 0; i < bufferSize; i++) {
    BF_Block_Init(&(mBlocks[i]));
  }

  dataTable = (char**)malloc(bufferSize*sizeof(char*));     //dataTable is an array of pointers to the block data

  CALL_OR_RETURN(BF_GetBlockCounter(input_fileDesc, &totalBlocks));     // Count the total blocks
  
  currentBlock = 1;     // Algorithm for first sorting, inside chunks
  totalRecords = 0;     // counts total records in chunk, used in quicksort
  while (currentBlock < totalBlocks) {
    for(i = 0; (i < bufferSize) && (currentBlock < totalBlocks); i++) {        // Read block data from the chunk blocks
      // To debug uncomment following line
      // printf("%d of %d\n", currentBlock, totalBlocks-1);
      CALL_OR_RETURN(BF_GetBlock(input_fileDesc, currentBlock, mBlocks[i]));
      dataTable[i] = BF_Block_GetData(mBlocks[i]);
      memcpy(&bRecords, dataTable[i], sizeof(int));
      totalRecords += bRecords;
      currentBlock++;
    }
    quickSort(dataTable, 1, totalRecords, fieldNo);        // Keep in mind that indexing for records is [1,total]
    for(k = 0; k < i; k++) {
      //to debug by checking a chunk sorted version of unsorted file, uncomment the following line
      //BF_Block_SetDirty(mBlocks[k]);
      CALL_OR_RETURN(BF_AllocateBlock(temp_fileDesc, tempFileBlock));
      char* tempBData = BF_Block_GetData(tempFileBlock);
      memcpy(tempBData, dataTable[k], BF_BLOCK_SIZE);
      BF_Block_SetDirty(tempFileBlock);
      CALL_OR_RETURN(BF_UnpinBlock(tempFileBlock));
      CALL_OR_RETURN(BF_UnpinBlock(mBlocks[k]));
    }
    totalRecords = 0;
  }   

  BF_Block_Destroy(&tempFileBlock);
  for (i = 0; i < bufferSize; i++) {
    BF_Block_Destroy(&(mBlocks[i]));
  }
  free(mBlocks);
  free(dataTable);

  return SR_OK;
}

//////////////////////////////////////////// External Sorting Functions /////////////////////////////////////////////

/*
 * Doc: Counts the number of chunks based on the given chunkSize.
 */
int countChunks(int totalBlocks, int chunkSize){
    int result = totalBlocks / chunkSize;
    if ((totalBlocks % chunkSize) != 0){
        return result + 1;
    }
    return result;
}

/*
 * Doc: Checks if the first index elements of indexArray[1][] is -1. Used later in Merge.
 */
int allChecked(int **indexArray, int index){
    int flag = 1, i;
    for(i = 0; i < index; i++)
        if(indexArray[1][i] != -1)
            flag = 0;
    return flag;
}

/*
 * Doc: Gets the record specified by the block in the chunkTable[index] and by the record index specified in the indexArray[2][index]
 */
void GetCurrentRecord(char **dataTable, int index, int **indexArray, Record *record){
    memcpy(record, dataTable[index] + indexArray[2][index]*sizeof(Record) + sizeof(int), sizeof(Record));
}

/*
 * Doc: Increases the index in the indexArray[2][index] to show that we have successfully stored the current record and
 *      that we move to the next one.
 */
int IncreaseRecordIndex(int **indexArray, int index, char **dataTable){
    int recordsInBlock;

    memcpy(&recordsInBlock, dataTable[index], sizeof(int));
    if(indexArray[2][index] == recordsInBlock-1)  // If current record index is equal to total records, all records have been checked
        return 0;
    indexArray[2][index] = indexArray[2][index]+1;
    return 1;
}

/*
 * Doc: Increases the index in the indexArray[1][index] and gets the next Block in the chunk,
 *      if there is one and if that one is not NULL.
 */
BF_ErrorCode GetNextBlock(int temp_fileDesc, BF_Block **bufferBlocks, char **dataTable, int **indexArray, int index, int *ErrorCode){
    int totalBlocks;
    int chunkSize = indexArray[0][1] - indexArray[0][0];
    
    CALL_OR_RETURN(BF_UnpinBlock(bufferBlocks[index]));
    CALL_OR_RETURN(BF_GetBlockCounter(temp_fileDesc, &totalBlocks));
    if((indexArray[1][index] + 1 == chunkSize + indexArray[0][index]) || (totalBlocks == indexArray[1][index] + 1)){ // Chunk is finished, no new blocks to get
        *ErrorCode = 0;
    }
    else{
        indexArray[1][index]++; // Increase the current block's index
        CALL_OR_RETURN(BF_Block_GetBlock_GetData(&(bufferBlocks[index]), temp_fileDesc, indexArray[1][index], &(dataTable[index])));
        indexArray[2][index] = 0; // Set record index to point to the first record again
        *ErrorCode = 1;
    }
    return BF_OK;
}

/*
 * Doc: Initializes the minimum record as the first available record of the blocks in the buffer.
 */
void InitiliazeMinRecord(Record *minrecord, int *pos, char **dataTable, int **indexArray, int numChunks){
    int i;
    for(i = 0; i < numChunks; i++){
        if(indexArray[1][i] != -1) {
            GetCurrentRecord(dataTable, i, indexArray, minrecord);
            *pos = i;
            return;
        }
    }
}

/*
 * Doc: Inserts the given record in the output block of the buffer. 
 *      If this block is full, we unpin it and allocate a new one 
 *      in the file and insert it there.
 */
BF_ErrorCode insertInOutputBlock(int write_fileDesc, BF_Block **bufferBlocks, char **dataTable, int bufferSize, Record record){
    int recordCount, blockCount;
    
    CALL_OR_RETURN(BF_GetBlockCounter(write_fileDesc, &blockCount));
    if(blockCount < 2){
        CALL_OR_RETURN(BF_Block_Allocate_GetData(&(bufferBlocks[bufferSize-1]), write_fileDesc, &(dataTable[bufferSize-1])));
        setRecordCount(dataTable[bufferSize-1], 0);
    }
    else {
        memcpy(&recordCount, dataTable[bufferSize-1], sizeof(int));
        if(recordCount >= (BF_BLOCK_SIZE-sizeof(int))/sizeof(Record)) {  // No space in current data block
            CALL_OR_RETURN(BF_UnpinBlock(bufferBlocks[bufferSize-1]));   // unpins previous block since we are allocating a new one
            CALL_OR_RETURN(BF_Block_Allocate_GetData(&(bufferBlocks[bufferSize-1]), write_fileDesc, &(dataTable[bufferSize-1])));
            setRecordCount(dataTable[bufferSize-1], 0);
        }
    }

    memcpy(&recordCount, dataTable[bufferSize-1], sizeof(int));
    memcpy(dataTable[bufferSize-1] + sizeof(int) + recordCount*sizeof(Record), &record, sizeof(Record));
    increaseRecordCount(dataTable[bufferSize-1]);
    BF_Block_SetDirty(bufferBlocks[bufferSize-1]);
    
    return BF_OK;
}

/*
 * Doc: Sets the proper values for bufferBlocks, dataTable 
 *      and indexArray, when the chunks are less that the 
 *      size of the buffer, so they can be used from Merge.
 */
BF_ErrorCode setupForMergeLeftoverChunks(int read_fileDesc, BF_Block **bufferBlocks, char **dataTable, int **indexArray, int bufferSize, int chunkSize, int leftoverChunks, int runtimes){
    int j, k;
    for(j = 0; j < 3; j++) {        // setup the indexArray properly for Merge
        for (k = 0; k < leftoverChunks; k++) {
            if (j < 2)
                indexArray[j][k] = runtimes*((bufferSize-1))*chunkSize + 1 + k*chunkSize;
            else
                indexArray[j][k] = 0;
        }
    }
    for(j = 0; j < (bufferSize-1); j++) {   // setup the bufferBlocks and dataTable properly for Merge
        if(j < leftoverChunks) {
            CALL_OR_RETURN(BF_Block_GetBlock_GetData(&(bufferBlocks[j]), read_fileDesc, indexArray[0][j], &dataTable[j]));
        }
        else {
            indexArray[1][j] = -1;  // blocks of the buffer that will not be used
        }
    }
    
    return BF_OK;
}

/*
 * Doc: Sets the proper values for bufferBlocks, dataTable 
 *      and indexArray so they can be used from Merge.
 */
BF_ErrorCode setupForMerge(int read_fileDesc, BF_Block **bufferBlocks, char **dataTable, int **indexArray, int bufferSize, int chunkSize, int runtimes){
    int j, k;
    for(j = 0; j < 3; j++) {        // setup the indexArray properly for Merge
        for (k = 0; k < (bufferSize-1); k++) {
            if (j < 2)
                indexArray[j][k] = k*chunkSize + 1 + runtimes*((bufferSize-1))*chunkSize;
            else
                indexArray[j][k] = 0;
        }
    }
    for(j = 0; j < (bufferSize-1); j++){        // setup the bufferBlocks and dataTable properly for Merge
        CALL_OR_RETURN(BF_Block_GetBlock_GetData(&(bufferBlocks[j]), read_fileDesc, indexArray[0][j], &(dataTable[j])));
    }
    
    return BF_OK;
}

/*
 * Doc: This function is used to merge numChunks chunks. The function gets the minimum record
 *      from the blocks in bufferBlocks and inserts it to an output Block in a write File.
 */
BF_ErrorCode Merge(int read_fileDesc, int write_fileDesc, BF_Block **bufferBlocks, char **dataTable, int **indexArray, int fieldNo, int numChunks, int bufferSize){
    int i, pos, errorcode;
    Record minrecord, temprecord;

    while(!allChecked(indexArray, numChunks)){       // While the chunk array is not null, not all records have been checked
        InitiliazeMinRecord(&minrecord, &pos, dataTable, indexArray, numChunks);    // Find the first available record to set as the minimum
        for(i = 0; i < numChunks; i++){
            if(indexArray[1][i] != -1) {
                GetCurrentRecord(dataTable, i, indexArray, &temprecord);
                if(compareRecords(&temprecord, &minrecord, fieldNo)){
                    minrecord = temprecord;
                    pos = i;
                }
            }
        }
        if(!IncreaseRecordIndex(indexArray, pos, dataTable)) { // Move the pointer to the next record, if all records checked get the next block
            CALL_OR_RETURN(GetNextBlock(read_fileDesc, bufferBlocks, dataTable, indexArray, pos, &errorcode));
            if(errorcode == 0) // no new blocks in chunk
                indexArray[1][pos] = -1;
        }
        insertInOutputBlock(write_fileDesc, bufferBlocks, dataTable, bufferSize, minrecord);
    }
    CALL_OR_RETURN(BF_UnpinBlock(bufferBlocks[bufferSize-1]));   // unpins last output block before exiting

    return BF_OK;
}

/*
 * Doc: This function is called recursively and merges the current chunks by calling Merge for each one.
 *      If there is only one chunk left, the function returns BF_OK. Else, it merges all chunk groups (chunks with buffersize blocks)
 *      with a k-way merge (k = buffersize-1) and then
 *      it merges the leftover chunks by using a k-way merge with k<buffersize.
 */
BF_ErrorCode recursiveMergeSort(BF_Block **bufferBlocks, char **dataTable, int **indexArray, int chunkSize, int bufferSize, int fieldNo){
    int i;
    int numChunkGroups, leftoverChunks, totalBlocks, totalChunks;
    int read_fileDesc, write_fileDesc;

    SR_OpenFile("tempFile", &read_fileDesc);    // File created by internal sort
    
    CALL_OR_RETURN(BF_GetBlockCounter(read_fileDesc, &totalBlocks));        // Count the total blocks
    totalChunks = countChunks(totalBlocks-1, chunkSize);                    // Count the total chunks

    if(totalChunks == 1){           // If there is only 1 chunk left then it is sorted
        SR_CloseFile(read_fileDesc);
        return BF_OK;
    }
    
    SR_CreateFile("writeFile");     // Temporary output file
    SR_OpenFile("writeFile", &write_fileDesc);

    numChunkGroups = totalChunks / (bufferSize-1);
    leftoverChunks = totalChunks % (bufferSize-1);

    for(i = 0; i < numChunkGroups; i++) {   // Merging (numChunkGroups) of (bufferSize-1) chunks
        setupForMerge(read_fileDesc, bufferBlocks, dataTable, indexArray, bufferSize, chunkSize, i);  // setup indexArray,bufferBlocks and dataTable properly for Merge
        CALL_OR_RETURN(Merge(read_fileDesc, write_fileDesc, bufferBlocks, dataTable, indexArray, fieldNo, bufferSize-1, bufferSize));
    }
    
    if(leftoverChunks > 1) {    // if last chunk group has more than 1 chunks, they must be merged as well
        setupForMergeLeftoverChunks(read_fileDesc, bufferBlocks, dataTable, indexArray, bufferSize, chunkSize, leftoverChunks, numChunkGroups); // setup indexArray,bufferBlocks and dataTable properly for Merge
        CALL_OR_RETURN(Merge(read_fileDesc, write_fileDesc, bufferBlocks, dataTable, indexArray, fieldNo, leftoverChunks, bufferSize));
    }
    else if(leftoverChunks == 1) {      // Last group has only 1 chunk so we copy it as it is to the writeFile
        for(i = (numChunkGroups*((bufferSize-1))*chunkSize+1); i < totalBlocks; i++) {
            CALL_OR_RETURN(BF_Block_GetBlock_GetData(&(bufferBlocks[0]), read_fileDesc, i, &dataTable[0]));
            CALL_OR_RETURN(BF_Block_Allocate_GetData(&bufferBlocks[1], write_fileDesc, &dataTable[1]));
            memcpy(dataTable[1], dataTable[0], BF_BLOCK_SIZE);
            BF_Block_SetDirty(bufferBlocks[1]);
            CALL_OR_RETURN(BF_UnpinBlock(bufferBlocks[0]));
            CALL_OR_RETURN(BF_UnpinBlock(bufferBlocks[1]));
        }
    }

    SR_CloseFile(read_fileDesc);
    remove("tempFile");     // remove the file we read blocks
    SR_CloseFile(write_fileDesc);
    rename("writeFile", "tempFile");    // now writeFile is the file we will read from in the next recursiveMergeSort

    CALL_OR_RETURN(recursiveMergeSort(bufferBlocks, dataTable, indexArray, chunkSize*(bufferSize-1), bufferSize, fieldNo));
}

/*
 * Doc: Function used to identify files, initialize Blocks and call the recursive function.
 */
BF_ErrorCode externalSort(int fieldNo, int bufferSize){
    int i;
    BF_Block **bufferBlocks;
    char **dataTable;
    int **indexArray;

    /*    Index array format:
     *    Starting position in chunk
     *    Current position in chunk
     *    Current record being compared
     */
    indexArray = (int**)malloc(3 * sizeof(int*));
    for(i = 0; i < 3; i++)
        indexArray[i] = (int*)malloc((bufferSize-1) * sizeof(int));
    bufferBlocks = (BF_Block**)malloc((bufferSize) * sizeof(BF_Block*)); // bufferBlocks works as the buffer by holding the pinned blocks
    for (i = 0; i < (bufferSize); i++)
        BF_Block_Init(&bufferBlocks[i]);
    dataTable = (char**)malloc((bufferSize) * sizeof(char*));      // dataTable is an array of pointers to the bufferblocks data

    /* Merging groups of bufferSize-1 chunks recursively until all records are sorted */
    CALL_OR_RETURN(recursiveMergeSort(bufferBlocks, dataTable, indexArray, bufferSize, bufferSize, fieldNo));

    for (i = 0; i < bufferSize; i++)
        BF_Block_Destroy(&(bufferBlocks[i]));
    free(bufferBlocks);
    free(dataTable);
    free(indexArray);

    return SR_OK;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

SR_ErrorCode SR_SortedFile(const char* input_filename, const char* output_filename, int fieldNo, int bufferSize) {
    // Your code goes here
    int input_fileDesc, temp_fileDesc;

    if (!checkAttributes(fieldNo, bufferSize)) {
        return SR_ERROR;
    }

    CALL_OR_RETURN(SR_CreateFile("tempFile"));     // Temporary file to hold sorted chunks
    CALL_OR_RETURN(SR_OpenFile("tempFile", &temp_fileDesc));
    CALL_OR_RETURN(SR_OpenFile(input_filename, &input_fileDesc));

    CALL_OR_RETURN(internalSort(input_fileDesc, temp_fileDesc, fieldNo, bufferSize));      // Internal sorting of chunks using quicksort

    CALL_OR_RETURN(SR_CloseFile(input_fileDesc));     // Input file not needed anymore, no changes have been made
    CALL_OR_RETURN(SR_CloseFile(temp_fileDesc));      // Temporary file closed. Will be opened again by externalSort
    
    CALL_OR_RETURN(externalSort(fieldNo, bufferSize));
    rename("tempFile", output_filename);

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