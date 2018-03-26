package tests;


import btree.AddFileEntryException;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import columnar.Columnarfile;
import global.SystemDefs;
import heap.*;
import org.junit.Test;

import java.io.IOException;

public class BitMapTests {


    @Test
    public void ete() throws Exception{
        // // batchinsert DATAFILENAME COLUMNDBNAME COLUMNARFILENAME NUMCOLUMNS
        batchinsert.run(new String[] {"smalltest.txt", "column_db_name","columnar_file_name","4" });

        // index COLUMNDBNAME COLUMNARFILENAME COLUMNNAME INDEXTYPE
        Index.run(new String[]{ "column_db_name", "columnar_file_name", "C", "BITMAP"});
    }

    @Test
    public void test() throws Exception{
        // // batchinsert DATAFILENAME COLUMNDBNAME COLUMNARFILENAME NUMCOLUMNS
        String columnDbName = "column_db_name";
        batchinsert.run(new String[] {"smalltest.txt", columnDbName,"columnar_file_name","4" });

        Columnarfile columnarfile = new Columnarfile(columnDbName);
//        columnarfile.createBitMapIndex()
    }
}