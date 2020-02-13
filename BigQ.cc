#include "BigQ.h"
#include <vector>



// ------------------------------------------

void* BigQ :: Driver(void *p){
  BigQ * ptr = (BigQ*) p;
  ptr->Phase1();
  ptr->Phase2();

}


void BigQ :: Phase1()
{
	Record tRec;
	Page tPage;
	Run tRun(runlen);

	// read data from in pipe sort them into runlen pages
	while(in.Remove(&tRec)) {
		if(!tPage.Append(&tRec)) {
			if (tRun.checkRunFull()) {
				tRun.sortRun();
				// TODO:: write run to file
				tRun.clearPages();
			}
			tRun.AddPage(tPage);
			tPage.EmptyItOut();
			tPage.Append(&tRec);
		}
	}

	if(tPage.getNumRecs()>0) {
		if (tRun.checkRunFull()) {
			tRun.sortRun();
			tRun.clearPages();
		}

		tRun.AddPage(tPage);
		tRun.sortRun();
		// TODO:: write run to file
		tPage.EmptyItOut();
	}
	else if(tRun.getRunSize()!=0) {
		tRun.sortRun();
		// TODO:: write run to file
		tRun.clearPages();
	}


}

void BigQ :: Phase2()
{
  cout <<"Phase2";
}
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	myThreadData.in = &in;
  myThreadData.out = &out;
  myThreadData.sortorder = &sortorder;
  myThreadData.runlen = runlen;
  pthread_create(&myThread, NULL, BigQ::Driver,this);
	out.ShutDown ();
}

BigQ::~BigQ () {
}

// ------------------------------------------





// ------------------------------------------
Run::Run(int runlen) {
	this->runLength = runlen;
}
void Run::AddPage(Page p) {
	this->pages.push_back(p);
}
void Run::sortRun() {
	// sort pages
	// insert pages into tournament
}
vector<Page> Run::getPages() {
	return this->pages;
}
bool Run::checkRunFull() {
	return this->pages.size() == this->runLength;
}
bool Run::clearPages() {
	this->pages.clear();
}
int Run::getRunSize() {
	return this->pages.size();
}
//------------------------------------------
