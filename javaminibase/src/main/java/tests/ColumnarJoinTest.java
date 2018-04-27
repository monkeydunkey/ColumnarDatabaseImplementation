package tests;

import java.io.*;
import java.util.*;
import java.lang.*;
import heap.*;
import bufmgr.*;
import diskmgr.*;
import global.*;
import columnar.*;
import chainexception.*;
import index.ColumnIndexScan;
import iterator.*;

/**
 * Note that in JAVA, methods can't be overridden to be more private.
 * Therefore, the declaration of all private functions are now declared
 * protected as opposed to the private type in C++.
 */

class ColumnarJoinTestDriver extends TestDriver implements GlobalConst {

    private final static boolean OK = true;
    private final static boolean FAIL = false;

    private int choice;
    private final static int reclen = 32;

    int[] data_1 = {1, 20, 13, 42, 15, 12, 24, 4};
    int[] data_2 = {3, 40, 23, 92, 25, 10, 11, 41};
    private static String data_3[] = {
            "raghu", "xbao", "cychan", "leela", "ketola", "soma", "ulloa",
            "dhanoa", "dsilva", "kurniawa", "dissoswa", "waic", "susanc", "kinc",
            "marc", "scottc", "yuc", "ireland", "rathgebe", "joyce", "daode",
            "yuvadee", "he", "huxtable", "muerle", "flechtne", "thiodore", "jhowe",
            "frankief", "yiching", "xiaoming", "jsong", "yung", "muthiah", "bloch",
            "binh", "dai", "hai", "handi", "shi", "sonthi", "evgueni", "chung-pi",
            "chui", "siddiqui", "mak", "tak", "sungk", "randal", "barthel",
            "newell", "schiesl", "neuman", "heitzman", "wan", "gunawan", "djensen",
            "juei-wen", "josephin", "harimin", "xin", "zmudzin", "feldmann",
            "joon", "wawrzon", "yi-chun", "wenchao", "seo", "karsono", "dwiyono",
            "ginther", "keeler", "peter", "lukas", "edwards", "mirwais", "schleis",
            "haris", "meyers", "azat", "shun-kit", "robert", "markert", "wlau",
            "honghu", "guangshu", "chingju", "bradw", "andyw", "gray", "vharvey",
            "awny", "savoy", "meltz"};
    int[] data_11 = {0, 2, 16, 29, 13, 21, 24, 4};
    int[] data_22 = {3, 0, 23, 92, 5, 10, 1, 41};
    private static String data_33[] = {
            "yuvadee", "he", "huxtable", "muerle", "flechtne", "thiodore", "jhowe",
            "frankief", "yiching", "xiaoming", "jsong", "yung", "muthiah", "bloch",
            "binh", "dai", "hai", "handi", "shi", "sonthi", "evgueni", "chung-pi",
            "chui", "siddiqui", "mak", "tak", "sungk", "randal", "barthel",
            "newell", "schiesl", "neuman", "heitzman", "wan", "gunawan", "djensen",
            "juei-wen", "josephin", "harimin", "xin", "zmudzin", "feldmann",
            "joon", "wawrzon", "yi-chun", "wenchao", "seo", "karsono", "dwiyono",
            "ginther", "keeler", "peter", "lukas", "edwards", "mirwais", "schleis",
            "haris", "meyers", "azat", "shun-kit", "robert", "markert", "wlau",
            "honghu", "guangshu", "chingju", "bradw", "andyw", "gray", "vharvey",
            "awny", "savoy", "meltz"};
    public ColumnarJoinTestDriver() {
        super("cjtest");
        choice = 100;      // big enough for file to occupy > 1 data page
        //choice = 2000;   // big enough for file to occupy > 1 directory page
        //choice = 5;
    }

    public boolean runTests() {

        System.out.println("\n" + "Running " + testName() + " tests...." + "\n");

        SystemDefs sysdef = new SystemDefs(dbpath, 100, 100, "Clock");

        // Kill anything that might be hanging around
        String newdbpath;
        String newlogpath;
        String remove_logcmd;
        String remove_dbcmd;
        String remove_cmd = "/bin/rm -rf ";

        newdbpath = dbpath;
        newlogpath = logpath;

        remove_logcmd = remove_cmd + logpath;
        remove_dbcmd = remove_cmd + dbpath;

        // Commands here is very machine dependent.  We assume
        // user are on UNIX system here
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("IO error: " + e);
        }

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("IO error: " + e);
        }

        //Run the tests. Return type different from C++
        boolean _pass = runAllTests();

        //Clean up again
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("IO error: " + e);
        }

        System.out.print("\n" + "..." + testName() + " tests ");
        System.out.print(_pass == OK ? "completely successfully" : "failed");
        System.out.print(".\n\n");

        return _pass;
    }

    protected boolean test1()
    {
        System.out.println("\n  Test 1: Initialize a columnar file with 2 int columns\n");
        boolean status = OK;
        Columnarfile outer = null;
        Columnarfile inner = null;

        System.out.println("  - Creating columnar files\n");
        try {
            AttrType[] attrTypes = new AttrType[3];
            attrTypes[0] = new AttrType(1);
            attrTypes[1] = new AttrType(1);
            attrTypes[2] = new AttrType(0);
            outer = new Columnarfile("test_file", 3, attrTypes);
            inner = new Columnarfile("test_file2", 3, attrTypes);
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not create the columnar files\n");
            e.printStackTrace();
        }

        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                != SystemDefs.JavabaseBM.getNumBuffers()) {
            System.err.println("*** The heap file has left pages pinned\n");
            status = FAIL;
        }

        if (status == OK)
            System.out.println("  Test 1 completed successfully.\n");

        return status;
    }

    protected boolean test2()
    {
        System.out.println("\n  Test 2: Opening the Columnar File created in the last step and add some entries\n");
        boolean status = OK;
        TID insertedVal;
        Columnarfile outer;
        Columnarfile inner;
        try {
            System.out.println("  - Opening already created columnar files\n");
            outer = new Columnarfile("test_file");
            inner = new Columnarfile("test_file2");

        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not read the created columnar files\n");
            e.printStackTrace();
            return status;
        }
        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                != SystemDefs.JavabaseBM.getNumBuffers()) {
            System.err.println("*** Opening columnar file has left pinned pages\n");
            status = FAIL;
            return status;
        }
        insertedVal = new TID(4);
        try {
            System.out.println("  - Opening the columnar file and adding a lot of data entries\n");
            outer = new Columnarfile("test_file");
            for (int i = 0; i < data_1.length; i++){
                byte[] dataArray = new byte[8 + 25];
                ValueIntClass val1 = new ValueIntClass(data_1[i]);
                ValueIntClass val2 = new ValueIntClass(data_2[i]);
                System.arraycopy (val1.getByteArr(), 0, dataArray, 0, 4);
                System.arraycopy (val2.getByteArr(), 0, dataArray, 4, 4);
                Convert.setStrValue(data_3[i], 8, dataArray);
                insertedVal = outer.insertTuple(dataArray);

                ValueIntClass val11 = new ValueIntClass(data_11[i]);
                ValueIntClass val22 = new ValueIntClass(data_22[i]);
                System.arraycopy (val11.getByteArr(), 0, dataArray, 0, 4);
                System.arraycopy (val22.getByteArr(), 0, dataArray, 4, 4);
                Convert.setStrValue(data_33[i], 8, dataArray);
                insertedVal = inner.insertTuple(dataArray);
            }

        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not insert values\n");
            e.printStackTrace();
            return status;
        }

        try{
            TupleScan tpScan = new TupleScan(outer);
            System.out.println(outer.getTupleCnt());

            TupleScan tpScan2 = new TupleScan(inner);
            System.out.println(inner.getTupleCnt());
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not read all the tuples\n");
            e.printStackTrace();
            return status;
        }

        System.out.println("Test 2 Status " + status);
        return status;
    }

    protected boolean test3() { return true; }
    protected boolean test4() { return true; }
    protected boolean test5() { return true; }
    protected boolean test6(){ return true; }

    protected boolean runAllTests() {

        boolean _passAll = OK;

        if (!test1()) {
            _passAll = FAIL;
        }
        if (!test2()) {
            _passAll = FAIL;
        }
        if (!test3()) {
            _passAll = FAIL;
        }
        if (!test4()) {
            _passAll = FAIL;
        }
        if (!test5()) {
            _passAll = FAIL;
        }
        if (!test6()) {
            _passAll = FAIL;
        }

        return _passAll;
    }

    protected String testName() {

        return "Columnar File";
    }
}

public class ColumnarJoinTest {

    public static void main(String argv[]) {

        ColumnarJoinTestDriver hd = new ColumnarJoinTestDriver();
        boolean dbstatus;

        dbstatus = hd.runTests();

        if (dbstatus != true) {
            System.err.println("Error encountered during Columnar file tests:\n");
            Runtime.getRuntime().exit(1);
        }

        Runtime.getRuntime().exit(0);
    }
}