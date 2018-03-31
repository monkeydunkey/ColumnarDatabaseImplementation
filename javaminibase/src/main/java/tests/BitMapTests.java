package tests;


import columnar.Columnarfile;
import org.junit.Test;

public class BitMapTests {


    @Test
    public void ete() throws Exception{
        // // batchinsert DATAFILENAME COLUMNDBNAME COLUMNARFILENAME NUMCOLUMNS
        batchinsert.run(new String[] {"smalltest.txt", "column_db_name","columnar_file_name","4" });

        // index COLUMNDBNAME COLUMNARFILENAME COLUMNNAME INDEXTYPE
        Index.run(new String[]{ "column_db_name", "columnar_file_name", "C", "BITMAP"});
    }

}