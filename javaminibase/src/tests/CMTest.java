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

        System.out.println("\n  Test 1: Initialize a columnar file\n");
        boolean status = OK;
        RID rid = new RID();
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
    	System.out.println("\n  Test 2: Creating a Tuple Scan Object on columnar file\n");
        boolean status = OK;
        RID rid = new RID();
        Columnarfile f = null;

        System.out.println("  - Creating a columnar file\n");
        try {
            AttrType[] attrTypes = new AttrType[2];
            attrTypes[0] = new AttrType(1);
            attrTypes[1] = new AttrType(1);
            f = new Columnarfile("test_file_2", 2, attrTypes);
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
        
        return true;
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

        return "Heap File";
    }
}

// This is added to substitute the struct construct in C++
class DummyRecord {

    //content of the record
    public int ival;
    public float fval;
    public String name;

    //length under control
    private int reclen;

    private byte[] data;

    /**
     * Default constructor
     */
    public DummyRecord() {
    }

    /**
     * another constructor
     */
    public DummyRecord(int _reclen) {
        setRecLen(_reclen);
        data = new byte[_reclen];
    }

    /**
     * constructor: convert a byte array to DummyRecord object.
     *
     * @param arecord a byte array which represents the DummyRecord object
     */
    public DummyRecord(byte[] arecord)
            throws java.io.IOException {
        setIntRec(arecord);
        setFloRec(arecord);
        setStrRec(arecord);
        data = arecord;
        setRecLen(name.length());
    }

    /**
     * constructor: translate a tuple to a DummyRecord object
     * it will make a copy of the data in the tuple
     *
     * @param atuple: the input tuple
     */
    public DummyRecord(Tuple _atuple)
            throws java.io.IOException {
        data = new byte[_atuple.getLength()];
        data = _atuple.getTupleByteArray();
        setRecLen(_atuple.getLength());

        setIntRec(data);
        setFloRec(data);
        setStrRec(data);

    }

    /**
     * convert this class objcet to a byte array
     * this is used when you want to write this object to a byte array
     */
    public byte[] toByteArray()
            throws java.io.IOException {
        //    data = new byte[reclen];
        Convert.setIntValue(ival, 0, data);
        Convert.setFloValue(fval, 4, data);
        Convert.setStrValue(name, 8, data);
        return data;
    }

    /**
     * get the integer value out of the byte array and set it to
     * the int value of the DummyRecord object
     */
    public void setIntRec(byte[] _data)
            throws java.io.IOException {
        ival = Convert.getIntValue(0, _data);
    }

    /**
     * get the float value out of the byte array and set it to
     * the float value of the DummyRecord object
     */
    public void setFloRec(byte[] _data)
            throws java.io.IOException {
        fval = Convert.getFloValue(4, _data);
    }

    /**
     * get the String value out of the byte array and set it to
     * the float value of the HTDummyRecorHT object
     */
    public void setStrRec(byte[] _data)
            throws java.io.IOException {
        // System.out.println("reclne= "+reclen);
        // System.out.println("data size "+_data.size());
        name = Convert.getStrValue(8, _data, reclen - 8);
    }

    //Other access methods to the size of the String field and
    //the size of the record
    public void setRecLen(int size) {
        reclen = size;
    }

    public int getRecLength() {
        return reclen;
    }
}

public class CMTest {

    public static void main(String argv[]) {

        CMDriver hd = new CMDriver();
        boolean dbstatus;

        dbstatus = hd.runTests();

        if (dbstatus != true) {
            System.err.println("Error encountered during buffer manager tests:\n");
            Runtime.getRuntime().exit(1);
        }

        Runtime.getRuntime().exit(0);
    }
}

