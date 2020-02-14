#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include <queue>
#include <algorithm>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Comparison.h"
using namespace std;


// ------------------------------------------------------------------
typedef struct {
    Pipe * in;
    Pipe * out;
    OrderMaker * sortorder;
    int runlen;
} ThreadData;
// ------------------------------------------------------------------

// ------------------------------------------------------------------
class Run {
    int runLength;
    OrderMaker * sortorder;
    vector <Page *> pages;
    public:
        Run(int runLength);
        Run(int runLength,OrderMaker * sortorder);
        void AddPage(Page *p);
        void sortRunInternalPages();
        bool checkRunFull();
        bool clearPages();
        int getRunSize();
        vector<Page*> getPages();
        void getPages(vector<Page> * pagevector);
        void sortSinglePage(Page *p);
        bool customRecordComparator(Record &left, Record &right);
        bool writeRunToFile(DBFile *file);
};
// ------------------------------------------------------------------


// ------------------------------------------------------------------
class CustomComparator{
    ComparisonEngine myComparisonEngine;
    OrderMaker * myOrderMaker;
public:
    CustomComparator(OrderMaker * sortorder);
    bool operator()( Record* lhs,  Record* rhs);
};
// ------------------------------------------------------------------

// ------------------------------------------------------------------
class RunManager{
    DBFile * myFile;
    int noOfRuns;
    int runLength;
    vector <Page> pages;
    vector <int> fileDescriptors;
public:
    void getPages(vector<Page> * myPageVector);
    bool getNextPageOfRun(Page * page,int runNo,int pageOffset);
};
// ------------------------------------------------------------------

// ------------------------------------------------------------------
class TournamentTree{
    OrderMaker * myOrderMaker;
    RunManager * myRunManager;
    vector<Page> myPageVector;
    Page OutputBuffer;
    bool isRunManagerAvailable;
    priority_queue<Record,vector<Record>,CustomComparator> * myQueue;
    void Inititate();
public:
    TournamentTree(Run * run,OrderMaker * sortorder);
    TournamentTree(RunManager * manager,OrderMaker * sortorder);
    bool GetSortedPage(Page &p);
};
// ------------------------------------------------------------------

// ------------------------------------------------------------------
class BigQ {
     ThreadData myThreadData;
     TournamentTree * myTree;
     pthread_t myThread;
     int totalRuns;
     bool isLastRunComplete;
     DBFile myFile;
     void Phase1();
     void Phase2();
public:
    static void* Driver(void*);
    BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
    ~BigQ ();
};
// ------------------------------------------------------------------

#endif
