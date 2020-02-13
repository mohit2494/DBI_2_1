#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include <vector>
#include <queue>
#include "ComparisonEngine.h"
#include "DBFile.h"
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
class TreeComparator{
    ComparisonEngine myComparisonEngine;
    OrderMaker * myOrderMaker;
public:
    TreeComparator(OrderMaker &sortorder);
    bool operator()(const Record & lhs, const Record &rhs);
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
    vector<Page *> getPagesForSorting();
    Page * getNextPageOfRun(int runNo,int pageOffset);
};
// ------------------------------------------------------------------

// ------------------------------------------------------------------
class TournamentTree{
    OrderMaker * myOrderMaker;
    RunManager * myRunManager;
    vector<Page> myPageVector;
    Page OutputBuffer;
    bool isRunManagerAvailable;
    priority_queue<Record,vector<Record>,TreeComparator> * myQueue;
    void Inititate();
public:
    TournamentTree(OrderMaker &sortorder,RunManager &manager);
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

// ------------------------------------------------------------------
class Run {
	int runLength;
	vector <Page> pages;
	public:
		Run(int runLength);
		void AddPage(Page p);
		void sortRunInternalPages();
		bool checkRunFull();
		bool clearPages();
		int getRunSize();
		vector<Page> getPages();
};
// ------------------------------------------------------------------
#endif
