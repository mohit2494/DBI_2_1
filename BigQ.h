#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include <queue>
#include <algorithm>
#include<unordered_map>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Comparison.h"
using namespace std;


// ------------------------------------------------------------------
typedef struct{
    int startPage;
    int currentPage;
    int endPage;
    int runId;
} RunFileObject;

typedef struct{
    int runId;
    Record * record;
} QueueObject;

typedef struct {
    Pipe * in;
    Pipe * out;
    OrderMaker * sortorder;
    int runlen;
} ThreadData;
// ------------------------------------------------------------------

// ------------------------------------------------------------------
class run {
    int runLength;
    OrderMaker * sortorder;
    vector <Page *> pages;
    public:
        run(int runLength);
        run(int runLength,OrderMaker * sortorder);
        void AddPage();
        void AddPage(Page *p);
        int addRecordAtPage(long long int pageCount, Record *rec);
        bool checkRunFull();
        bool clearPages();
        int getRunSize();
        vector<Page*> getPages();
        void getPages(vector<Page*> * pagevector);
        bool customRecordComparator(Record &left, Record &right);
        int writeRunToFile(File *file);
};
// ------------------------------------------------------------------


// ------------------------------------------------------------------
class CustomComparator{
    ComparisonEngine myComparisonEngine;
    OrderMaker * myOrderMaker;
public:
    CustomComparator(OrderMaker * sortorder);
    bool operator()( Record* lhs,   Record* rhs);
    bool operator()(QueueObject lhs,  QueueObject rhs);

};
// ------------------------------------------------------------------

// ------------------------------------------------------------------
class RunManager{
    File file;
    char * f_path;
    unordered_map<int,RunFileObject> runLocation;
public:
    RunManager(int runLength,char * f_path);
    void getPages(vector<Page*> * myPageVector);
    bool getNextPageOfRun(Page * page,int runNo);

};
// ------------------------------------------------------------------

// ------------------------------------------------------------------

class TournamentTree{
    OrderMaker * myOrderMaker;
    RunManager * myRunManager;
    vector<Page*> myPageVector;
    Page OutputBuffer;
    bool isRunManagerAvailable;
    priority_queue<QueueObject,vector<QueueObject>,CustomComparator> * myQueue;
    void Inititate();
    void InititateForRun();
public:
    TournamentTree(run * run,OrderMaker * sortorder);
    TournamentTree(RunManager * manager,OrderMaker * sortorder);
    void RefillOutputBuffer();
    bool GetSortedPage(Page * *p);
    void RefillOutputBufferForRun();
    bool GetSortedPageForRun(Page * *p);
};
// ------------------------------------------------------------------

// ------------------------------------------------------------------
class BigQ {
     ThreadData myThreadData;
     TournamentTree * myTree;
     pthread_t myThread;
     int totalRuns;
     bool isLastRunComplete;
     File myFile;
     char * f_path;
     void Phase1();
     void Phase2();
public:
    void sortCompleteRun(run *run, OrderMaker *sortorder);
    static void* Driver(void*);
    BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
    ~BigQ ();
};
// ------------------------------------------------------------------
#endif