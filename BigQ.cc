#include "BigQ.h"

using namespace std;

// ------------------------------------------------------------------
void* BigQ :: Driver(void *p){
  BigQ * ptr = (BigQ*) p;
  ptr->Phase1();
  ptr->Phase2();

}
void BigQ :: Phase1()
{
    Record tRec;
    Page tPage;
    Run tRun(this->myThreadData.runlen,this->myThreadData.sortorder);

    // read data from in pipe sort them into runlen pages
    while(this->myThreadData.in->Remove(&tRec)) {
        if(!tPage.Append(&tRec)) {
            if (tRun.checkRunFull()) {
                tRun.sortRunInternalPages();
                myTree = new TournamentTree(&tRun,this->myThreadData.sortorder);
                tRun.clearPages();
            }
            tRun.AddPage(tPage);
            tPage.EmptyItOut();
            tPage.Append(&tRec);
        }
    }

    if(tPage.getNumRecs()>0) {
        if (tRun.checkRunFull()) {
            tRun.sortRunInternalPages();
            tRun.clearPages();
        }

        tRun.AddPage(tPage);
        tRun.sortRunInternalPages();
        // TODO:: write run to file
        tPage.EmptyItOut();
    }
    else if(tRun.getRunSize()!=0) {
        tRun.sortRunInternalPages();
        // TODO:: write run to file
        tRun.clearPages();
    }
}

// sort runs from file using Run Manager
void BigQ :: Phase2()
{
  cout <<"Phase2";
}

// constructor
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    myThreadData.in = &in;
    myThreadData.out = &out;
    myThreadData.sortorder = &sortorder;
    myThreadData.runlen = runlen;
    pthread_create(&myThread, NULL, BigQ::Driver,this);
    out.ShutDown ();
}

// destructor
BigQ::~BigQ () {
}
// ------------------------------------------------------------------


// ------------------------------------------------------------------
struct recordComparator {
  bool operator() (int i,int j) { return (i<j);}
} myobject;

Run::Run(int runlen) {
    this->runLength = runlen;
    this->sortorder = NULL;
}

Run::Run(int runlen,OrderMaker * sortorder) {
    this->runLength = runlen;
    this->sortorder = sortorder;
}
void Run::AddPage(Page p) {
    this->pages.push_back(p);
}
void Run::sortRunInternalPages() {
    for(int i=0; i < pages.size(); i++) {
        this->sortSinglePage(pages.at(i));
    }
}
vector<Page> Run::getPages() {
    return this->pages;
}
void Run::getPages(vector<Page> * myPageVector) {
    myPageVector->swap(this->pages);
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

Page Run::sortSinglePage(Page p) {
    if (sortorder){
        vector<Record> records;
        Record temp;
        while(p.getNumRecs()>0) {
            p.GetFirst(&temp);
            records.push_back(temp);
        }
        sort(records.begin(), records.end(), CustomComparator(this->sortorder));
    }
}

// ------------------------------------------------------------------



// ------------------------------------------------------------------
TournamentTree :: TournamentTree(Run * run,OrderMaker * sortorder){
    myOrderMaker = sortorder;
    myQueue = new priority_queue<Record,vector<Record>,CustomComparator>(CustomComparator(sortorder));
    isRunManagerAvailable = false;
    run->getPages(&myPageVector);
    Inititate();

}

TournamentTree :: TournamentTree(RunManager * manager,OrderMaker * sortorder){
    myOrderMaker = sortorder;
    myRunManager = manager;
    myQueue = new priority_queue<Record,vector<Record>,CustomComparator>(CustomComparator(sortorder));
    isRunManagerAvailable = true;
    myRunManager->getPages(&myPageVector);
    Inititate();
}
void TournamentTree :: Inititate(){
    if (!myPageVector.empty()){
        int flag = 1;
        while(flag) {
            for(vector<Page>::iterator page = myPageVector.begin() ; page!=myPageVector.end() ; ++page){
                Record tempRecord;
                if (!page->GetFirst(&tempRecord) && isRunManagerAvailable){
                    if (myRunManager->getNextPageOfRun(&(*page),0,0)){
                        myQueue->push(tempRecord);
                    }
                }
                else{
                    myQueue->push(tempRecord);
                }
            }
            Record r = myQueue->top();
            myQueue->pop();

            if (OutputBuffer.Append(&r) || !myQueue->empty()){
                flag = 0;
            }

        }
    }

}

bool TournamentTree :: GetSortedPage(Page &page){
    Record temp;
    bool isOutputBufferEmpty = true;
    while(OutputBuffer.GetFirst(&temp)){
        page.Append(&temp);
        isOutputBufferEmpty = false;
    }
    if(isOutputBufferEmpty){
        return 0;
    }
    return 1;
}

// ------------------------------------------------------------------




void  RunManager:: getPages(vector<Page> * myPageVector){

};
bool RunManager :: getNextPageOfRun(Page * page,int runNo,int pageOffset){

};


// ------------------------------------------------------------------


CustomComparator :: CustomComparator(OrderMaker * sortorder){
    this->myOrderMaker = sortorder;
}
bool CustomComparator :: operator ()( Record & lhs,  Record &rhs){
    return myComparisonEngine.Compare(&lhs,&rhs,myOrderMaker);
}
