#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
using namespace std;

// ------------------------------------------------------------------
class BigQ {
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
