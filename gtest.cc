#include <string.h>
#include <gtest/gtest.h>
#include "test.h"
#include "Utilities.h"
#include "BigQ.h"

// -------------------------------------------------------------------------------------------------------------
class TestHelper{
    public:
    const std::string dbfile_dir = "dbfiles/"; 
    const std::string tpch_dir ="/home/mk/Documents/uf docs/sem 2/Database Implementation/git/tpch-dbgen/"; 
    const std::string catalog_path = "catalog";
    static OrderMaker sortorder;
    void get_sort_order (OrderMaker &sortorder) {
         cout << "\n specify sort ordering (when done press ctrl-D):\n\t ";
        if (yyparse() != 0) {
            cout << "Can't parse your sort CNF.\n";
            exit (1);
        }
        cout << " \n";
        Record literal;
        CNF sort_pred;
        sort_pred.GrowFromParseTree (final, new Schema ("catalog","nation"), literal);
        OrderMaker dummy;
        sort_pred.GetSortOrders (sortorder, dummy);
        // TestHelper::sortorder=sortorder;
    }
    void deleteFile(string filePath) {
             if(Utilities::checkfileExist(filePath.c_str())) {
                if( remove(filePath.c_str())!= 0) {
                    cerr<< "Error deleting file";   
                }
            }
    }
};
// -------------------------------------------------------------------------------------------------------------
TEST(WriteRunToFile,WriteRunToFile) {
        TestHelper th;
        DBFile fetcher;
        Record tempRecord;
        const string schemaSuffix = "nation.bin";
        const string dbFilePath = th.dbfile_dir + schemaSuffix;
        File file;
        OrderMaker sortorder;
        string tpchpath(tpch_dir);
        Schema nation ("catalog", "nation");
        if(!Utilities::checkfileExist(dbFilePath.c_str())) {
            fetcher.Create(dbFilePath.c_str(),heap,NULL);
        }
        fetcher.Open(dbFilePath.c_str());
        fetcher.Load(nation,dbFilePath.c_str());
         th.get_sort_order(sortorder);
        run trun(1,&sortorder);
        long long int pageCount=0;
        trun.AddPage();
        while(fetcher.GetNext(tempRecord)) {
                if(!trun.addRecordAtPage(pageCount, &tempRecord)) {
                    if(trun.checkRunFull()) {
                    break;
                }
            }
        }
        trun.writeRunToFile(&file);
        file.Close();fetcher.Close();
        // th.deleteFile("temp.xbin");
        th.deleteFile(dbFilePath.c_str());
        string prefloc = th.dbfile_dir+"nation.pref";
        th.deleteFile(prefloc.c_str());    
}
TEST(RunManager, GetPages) {
    RunManager rm(1,"temp.xbin");
    vector<Page *> pages;
    rm.getPages(&pages);
    ASSERT_GT(pages.size(),0);
}
TEST(RunManager, getNextPage) {
    RunManager rm(1,"temp.xbin");
    Page *page;
    ASSERT_FALSE(rm.getNextPageOfRun(page,1));
}

TEST(customRecordComparator, customRecordComparator) {
    TestHelper th;
    RunManager rm(1,"temp.xbin");
    ASSERT_GE(rm.getNoOfRuns()*rm.getRunLength(),rm.getTotalPages());
    th.deleteFile("temp.xbin");
}
// -------------------------------------------------------------------------------------------------------------
int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}