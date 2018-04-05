package tests;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Created by shashankbhushan on 3/25/18.
 */
public class TestPromptTest {

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

}