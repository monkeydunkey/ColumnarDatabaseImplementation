package tests;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Created by shashankbhushan on 3/25/18.
 */
public class TestPromptTest {

    @Rule
    public TestName name = new TestName();

    @Before
    public void setup(){
        System.out.println("=================================================");
        System.out.println("currently executing test: "+ name.getMethodName());
        System.out.println("=================================================");
    }

    @Test
    public void test_insert_then_query_integer_btree() throws IOException {
        System.out.println("filepath: "+new File(".").getAbsolutePath());
        TestPrompt.main(new String[] {"./tests/cases/test_insert_then_query_integer_btree.txt"});
    }
    @Test
    public void test_insert_then_query_integer_columnscan(){
        TestPrompt.main(new String[] {"./tests/cases/test_insert_then_query_integer_columnscan.txt"});
    }
    @Test
    public void test_insert_then_query_integer_filescan(){
        TestPrompt.main(new String[] {"./tests/cases/test_insert_then_query_integer_filescan.txt"});
    }
    @Test
    public void test_insert_then_query_string_btree(){
        TestPrompt.main(new String[] {"./tests/cases/test_insert_then_query_string_btree.txt"});
    }
    @Test
    public void test_insert_then_query_string_columnscan(){
        TestPrompt.main(new String[] {"./tests/cases/test_insert_then_query_string_columnscan.txt"});
    }
    @Test
    public void test_insert_then_query_string_filescan(){
        TestPrompt.main(new String[] {"./tests/cases/test_insert_then_query_string_filescan.txt"});
    }

    @Test
    public void test_insert_then_query_string_bitmap() {
        TestPrompt.main(new String[] {"./tests/cases/test_insert_then_query_string_bitmap.txt"});
    }

    @Test
    @Ignore
    public void test_insert_then_index_then_bitmapjoin() {
        //todo
        // batch insert
        // create bitmap index
        // create second columnar file (batch insert)
        // create bitmap index
        // join
//        TestPrompt.main(new String[] {"./tests/cases/test_insert_then_index_then_bitmapjoin.txt"});
    }

    // todo test bitmap directory pages overflow
    // todo test bitmap data pages overflow
}