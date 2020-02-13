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

// Wrapper for threaddata
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
    OrderMaker myOrderMaker;
    RunManager myRunManager;
    Page OutputBuffer;
    priority_queue<Record,vector<Record>,TreeComparator> * myQueue;
public:
    TournamentTree(OrderMaker &sortorder);
    TournamentTree(OrderMaker &sortorder,RunManager &manager);
    void Inititate(vector<Page> pages);
    void EmptyPages();
    Page* GetSortedPage();
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
		void sortRun();
		bool checkRunFull();
		bool clearPages();
		int getRunSize();
		vector<Page> getPages();
};
// ------------------------------------------------------------------

#endif
