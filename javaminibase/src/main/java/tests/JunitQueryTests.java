package tests;


import btree.AddFileEntryException;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import columnar.Columnarfile;
import global.SystemDefs;
import heap.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

public class JunitQueryTests {

    @Before
    public void setup(){
        // simulate TestPromptRunning
        SystemDefs sysdef = new SystemDefs("db1",100000,100,"Clock");
    }


//    @Test
//    public void ete() throws Exception{
//        // // batchinsert DATAFILENAME COLUMNDBNAME COLUMNARFILENAME NUMCOLUMNS
//        batchinsert.run(new String[] {"smalltest.txt", "column_db_name","columnar_file_name","4" });
//
//        // index COLUMNDBNAME COLUMNARFILENAME COLUMNNAME INDEXTYPE
//        Index.run(new String[]{ "column_db_name", "columnar_file_name", "C", "BITMAP"});
//    }
//
//    @Test
//    public void test() throws Exception{
//        // // batchinsert DATAFILENAME COLUMNDBNAME COLUMNARFILENAME NUMCOLUMNS
//        String columnDbName = "column_db_name";
//        batchinsert.run(new String[] {"smalltest.txt", columnDbName,"columnar_file_name","4" });
//
//        Columnarfile columnarfile = new Columnarfile(columnDbName);
////        columnarfile.createBitMapIndex()
//    }



    @Test
    public void testInsertThenQueryIntBTREESubset() throws Exception{
        String DATA_FILE_NAME = "smalltest.txt";
        String COLUMN_DB_NAME = "column_db_name";
        String COLUMNAR_FILENAME = "columnar_file_name";

        String[] argv = {DATA_FILE_NAME, COLUMN_DB_NAME, COLUMNAR_FILENAME, "4"};
        System.out.println("batchinsert "+ String.join(" ", argv));
        batchinsert.run(argv);

        Index.run(new String[]{ "column_db_name", "columnar_file_name", "C", "BTREE"});

        String params = COLUMN_DB_NAME+" "+COLUMNAR_FILENAME+" "+ "[B C] {C = 8} 50 BTREE";

        String[] args = params.split(" ");
        System.out.println("TEST PARAMS: query "+String.join(" ", args));
        query.run(args);
    }

}