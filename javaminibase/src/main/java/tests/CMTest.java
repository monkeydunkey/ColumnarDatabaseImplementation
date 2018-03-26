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

/**
 * Note that in JAVA, methods can't be overridden to be more private.
 * Therefore, the declaration of all private functions are now declared
 * protected as opposed to the private type in C++.
 */

class CMDriver extends TestDriver implements GlobalConst {

    private final static boolean OK = true;
    private final static boolean FAIL = false;

    private int choice;
    private final static int reclen = 32;

    public CMDriver() {
        super("hptest");
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

    protected boolean test1() {

        System.out.println("\n  Test 1: Initialize a columnar file with 2 int columns\n");
        boolean status = OK;
        Columnarfile f = null;

        System.out.println("  - Creating a columnar file\n");
        try {
            AttrType[] attrTypes = new AttrType[2];
            attrTypes[0] = new AttrType(1);
            attrTypes[1] = new AttrType(1);
            f = new Columnarfile("test_file", 2, attrTypes);
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not create a columnar file\n");
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

    protected boolean test2() {
        System.out.println("\n  Test 2: Opening the Columnar File created in the last step and add some entries\n");
        boolean status = OK;
        TID insertedVal;
        Columnarfile f = null;
        try {
            System.out.println("  - Opening Already created columnar file\n");
            f = new Columnarfile("test_file");

        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not read the created columnar file\n");
            e.printStackTrace();
            return status;
        }

        try {
            System.out.println("  - Adding some entries to the columnar file\n");
            byte[] dataArray = new byte[8];
            ValueIntClass val1 = new ValueIntClass(1);
            ValueIntClass val2 = new ValueIntClass(20);
            System.arraycopy (val1.getByteArr(), 0, dataArray, 0, 4);
            System.arraycopy (val2.getByteArr(), 0, dataArray, 4, 4);
            insertedVal = f.insertTuple(dataArray);
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not insert values\n");
            e.printStackTrace();
            return status;
        }
        try{
            System.out.println("  - Reading the inserted Value\n");
            byte[] storedDataArray  = f.getTuple(insertedVal).getTupleByteArray();
            byte[] val1 = new byte[4];
            byte[] val2 = new byte[4];
            System.arraycopy (storedDataArray, 0, val1, 0, 4);
            System.arraycopy (storedDataArray, 4, val2, 0, 4);
            ValueIntClass val1Class = new ValueIntClass(val1);
            ValueIntClass val2Class = new ValueIntClass(val2);
            if (val1Class.value != 1 || val2Class.value != 20){
                status = FAIL;
            }
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not insert values\n");
            e.printStackTrace();
            return status;
        }

        try{
            System.out.println("  - Updating the inserted value\n");
            byte[] dataArray = new byte[8];
            ValueIntClass val1 = new ValueIntClass(11);
            ValueIntClass val2 = new ValueIntClass(2);
            System.arraycopy (val1.getByteArr(), 0, dataArray, 0, 4);
            System.arraycopy (val2.getByteArr(), 0, dataArray, 4, 4);
            Tuple newtuple = new Tuple(dataArray, 0, 8);
            f.updateTuple(insertedVal, newtuple);
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not Update inserted values\n");
            e.printStackTrace();
            return status;
        }

        try{
            System.out.println("  - Reading the Updated Values\n");
            byte[] storedDataArray  = f.getTuple(insertedVal).getTupleByteArray();
            byte[] val1 = new byte[4];
            byte[] val2 = new byte[4];
            System.arraycopy (storedDataArray, 0, val1, 0, 4);
            System.arraycopy (storedDataArray, 4, val2, 0, 4);
            ValueIntClass val1Class = new ValueIntClass(val1);
            ValueIntClass val2Class = new ValueIntClass(val2);
            if (val1Class.value != 11 || val2Class.value != 2){
                status = FAIL;
            }
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Updated Values not correct values\n");
            e.printStackTrace();
            return status;
        }

        return status;
    }

    protected boolean test3() {
        return true;
    }

    //deal with variable size records.  it's probably easier to re-write
    //one instead of using the ones from C++
    protected boolean test5() {

        return true;
    }


    protected boolean test4() {

        return true;
    }

    protected boolean test6() {

        return true;
    }

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

public class CMTest {

    public static void main(String argv[]) {

        CMDriver hd = new CMDriver();
        boolean dbstatus;

        dbstatus = hd.runTests();

        if (dbstatus != true) {
            System.err.println("Error encountered during Columnar file tests:\n");
            Runtime.getRuntime().exit(1);
        }

        Runtime.getRuntime().exit(0);
    }
}

