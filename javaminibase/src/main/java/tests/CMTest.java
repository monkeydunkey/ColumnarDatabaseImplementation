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

class CMDriver extends TestDriver implements GlobalConst {

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
    public CMDriver() {
        super("cmtest");
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
            AttrType[] attrTypes = new AttrType[3];
            attrTypes[0] = new AttrType(1);
            attrTypes[1] = new AttrType(1);
            attrTypes[2] = new AttrType(0);
            f = new Columnarfile("test_file", 3, attrTypes);
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
            byte[] dataArray = new byte[8 + 25];
            ValueIntClass val1 = new ValueIntClass(1);
            ValueIntClass val2 = new ValueIntClass(20);
            ValueStrClass val3 = new ValueStrClass("Shashank");
            System.arraycopy (val1.getByteArr(), 0, dataArray, 0, 4);
            System.arraycopy (val2.getByteArr(), 0, dataArray, 4, 4);
            Convert.setStrValue(val3.value, 8, dataArray);
            insertedVal = f.insertTuple(dataArray);
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not insert values\n");
            e.printStackTrace();
            return status;
        }
        System.out.println("Test 2 Status " + status );
        try{
            System.out.println("  - Reading the inserted Value\n");
            byte[] storedDataArray  = f.getTuple(insertedVal).getTupleByteArray();
            byte[] val1 = new byte[4];
            byte[] val2 = new byte[4];
            byte[] val3 = new byte[25];
            System.arraycopy (storedDataArray, 0, val1, 0, 4);
            System.arraycopy (storedDataArray, 4, val2, 0, 4);
            String st = Convert.getStrValue(8, storedDataArray, 25);
            ValueIntClass val1Class = new ValueIntClass(val1);
            ValueIntClass val2Class = new ValueIntClass(val2);
            if (val1Class.value != 1 || val2Class.value != 20 || !st.trim().equals("Shashank")){
                status = FAIL;
            }
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not read inserted values\n");
            e.printStackTrace();
            return status;
        }
        try{
            System.out.println("  - Updating the inserted value\n");
            byte[] dataArray = new byte[8 + 25];
            ValueIntClass val1 = new ValueIntClass(11);
            ValueIntClass val2 = new ValueIntClass(2);
            System.arraycopy (val1.getByteArr(), 0, dataArray, 0, 4);
            System.arraycopy (val2.getByteArr(), 0, dataArray, 4, 4);
            Convert.setStrValue("Bhushan", 8, dataArray);
            Tuple newtuple = new Tuple(dataArray, 0, 8 + 25);
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
            byte[] val3 = new byte[25];
            System.arraycopy (storedDataArray, 0, val1, 0, 4);
            System.arraycopy (storedDataArray, 4, val2, 0, 4);
            String st = Convert.getStrValue(8, storedDataArray, 25);
            ValueIntClass val1Class = new ValueIntClass(val1);
            ValueIntClass val2Class = new ValueIntClass(val2);
            if (val1Class.value != 11 || val2Class.value != 2 || !st.trim().equals("Bhushan")){
                System.out.println("Failing here");
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
        System.out.println("In Here......");
        System.out.println("\n  Test 3: Opening the Columnar File created in the last step and add some entries\n");
        boolean status = OK;
        TID insertedVal;
        Columnarfile f = null;

        try {
            System.out.println("  - Opening Already created columnar file\n");
            AttrType[] attrTypes = new AttrType[3];
            attrTypes[0] = new AttrType(1);
            attrTypes[1] = new AttrType(1);
            attrTypes[2] = new AttrType(0);
            f = new Columnarfile("test_file2", 3, attrTypes);

        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not read the created columnar file\n");
            e.printStackTrace();
            return status;
        }

        System.out.println("  - ************ Tuple Scan Tests ********************* - \n");

        try {
            System.out.println("  - Adding some entries to the columnar file\n");
            byte[] dataArray = new byte[8 + 25];
            ValueIntClass val1 = new ValueIntClass(1);
            ValueIntClass val2 = new ValueIntClass(20);
            ValueStrClass val3 = new ValueStrClass("Shashank");
            System.arraycopy (val1.getByteArr(), 0, dataArray, 0, 4);
            System.arraycopy (val2.getByteArr(), 0, dataArray, 4, 4);
            Convert.setStrValue(val3.value, 8, dataArray);
            insertedVal = f.insertTuple(dataArray);

            val1 = new ValueIntClass(3);
            val2 = new ValueIntClass(33);
            val3 = new ValueStrClass("Bhushan");
            System.arraycopy (val1.getByteArr(), 0, dataArray, 0, 4);
            System.arraycopy (val2.getByteArr(), 0, dataArray, 4, 4);
            Convert.setStrValue(val3.value, 8, dataArray);
            insertedVal = f.insertTuple(dataArray);

            val1 = new ValueIntClass(9);
            val2 = new ValueIntClass(99);
            val3 = new ValueStrClass("Pujith");
            System.arraycopy (val1.getByteArr(), 0, dataArray, 0, 4);
            System.arraycopy (val2.getByteArr(), 0, dataArray, 4, 4);
            Convert.setStrValue(val3.value, 8, dataArray);
            insertedVal = f.insertTuple(dataArray);
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not insert values\n");
            e.printStackTrace();
            return status;
        }
        try {
            TupleScan tpScan = new TupleScan(f);
            for (int i = 0; i < f.getTupleCnt(); i++) {
                byte[] storedDataArray  = tpScan.getNext(insertedVal).getTupleByteArray();
                byte[] val11 = new byte[4];
                byte[] val12 = new byte[4];
                byte[] val3 = new byte[25];
                System.arraycopy (storedDataArray, 0, val11, 0, 4);
                System.arraycopy (storedDataArray, 4, val12, 0, 4);
                String st = Convert.getStrValue(8, storedDataArray, 25).trim();

                ValueIntClass val1Class = new ValueIntClass(val11);
                ValueIntClass val2Class = new ValueIntClass(val12);
                System.out.println("val1Class.value: "+val1Class.value+", val2Class.value: "+val2Class.value + ", 3rd Col: " + st);
            }

        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Error values\n");
            e.printStackTrace();
            return status;
        }
        return status;
    }
    protected boolean test4() {

        System.out.println("\n  Test 4: Running Columnar File scan on the table\n");
        boolean status = OK;

        Columnarfile f;
        TID insertedVal = new TID(4);
        try {
            System.out.println("  - Opening the columnar file and adding a lot of data entries\n");
            f = new Columnarfile("test_file");
            for (int i = 0; i < data_1.length; i++){
                byte[] dataArray = new byte[8 + 25];
                ValueIntClass val1 = new ValueIntClass(data_1[i]);
                ValueIntClass val2 = new ValueIntClass(data_2[i]);
                System.arraycopy (val1.getByteArr(), 0, dataArray, 0, 4);
                System.arraycopy (val2.getByteArr(), 0, dataArray, 4, 4);
                Convert.setStrValue(data_3[i], 8, dataArray);
                insertedVal = f.insertTuple(dataArray);
            }

        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not insert values\n");
            e.printStackTrace();
            return status;
        }



        try {
            System.out.println("  - Opening Already created columnar file\n");

            // set up an identity selection
            CondExpr[] expr = new CondExpr[2];
            expr[0] = new CondExpr();
            expr[0].op = new AttrOperator(AttrOperator.aopEQ);
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].type2 = new AttrType(AttrType.attrString);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 3);
            expr[0].operand2.string = "ketola";
            expr[0].next = null;
            expr[1] = null;

            FldSpec[] proj_list = new FldSpec[1];
            proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 2);
            int n_out_flds = 1;

            AttrType[] attrTypes = new AttrType[3];
            attrTypes[0] = new AttrType(1);
            attrTypes[1] = new AttrType(1);
            attrTypes[2] = new AttrType(0);
            short[] s1_sizes = {25};
            short len_in1 = 3;
            ColumnarFileScan cfscan = new ColumnarFileScan("test_file", attrTypes, s1_sizes,
                    len_in1, n_out_flds, proj_list,
                    expr);

            Tuple newtuple = cfscan.get_next();
            /*
            String v1 = newtuple.getStrFld(1);
            if (!v1.equals("ketola")){
                System.out.println(v1);
                status = FAIL;
            }
            */
            int v1 = newtuple.getIntFld(1);
            if (v1 != 25){
                System.out.println(v1);
                status = FAIL;
            }
            cfscan.close();

        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not apply filter on columnar file \n");
            e.printStackTrace();
            return status;
        }


        try {
            System.out.println("  - Opening the columnar file and adding an entry\n");
            f = new Columnarfile("test_file");
            byte[] dataArray = new byte[8 + 25];
            ValueIntClass val1 = new ValueIntClass(3);
            ValueIntClass val2 = new ValueIntClass(45);
            System.arraycopy (val1.getByteArr(), 0, dataArray, 0, 4);
            System.arraycopy (val2.getByteArr(), 0, dataArray, 4, 4);
            Convert.setStrValue("Shashank", 8, dataArray);
            insertedVal = f.insertTuple(dataArray);
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not insert values\n");
            e.printStackTrace();
            return status;
        }
        try {
            System.out.println(" - Marking the last inserted tuple for deletion\n");
            status = f.markTupleDeleted(insertedVal);
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not Mark tuple deleted\n");
            e.printStackTrace();
            return status;
        }
        try{
            TupleScan tpScan = new TupleScan(f);
            for (int i = 0; i < f.getTupleCnt() - 1; i++) {
                byte[] storedDataArray  = tpScan.getNext(insertedVal).getTupleByteArray();
                byte[] val11 = new byte[4];
                byte[] val12 = new byte[4];
                byte[] val3 = new byte[25];
                System.arraycopy (storedDataArray, 0, val11, 0, 4);
                System.arraycopy (storedDataArray, 4, val12, 0, 4);
                String st = Convert.getStrValue(8, storedDataArray, 25).trim();

                ValueIntClass val1Class = new ValueIntClass(val11);
                ValueIntClass val2Class = new ValueIntClass(val12);
                System.out.println("val1Class.value: "+val1Class.value+", val2Class.value: "+val2Class.value + ", 3rd Col: " + st);
            }
            Tuple nextData = tpScan.getNext(insertedVal);
            if (nextData != null){
                status = FAIL;
            }
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not read all the tuples\n");
            e.printStackTrace();
            return status;
        }


        try {
            System.out.println(" - purging the deleted record\n");
            Boolean pass = f.purgeAllDeletedTuples();
            if (!pass){
                status = FAIL;
            }
            if (f.getTupleCnt() != 9){
                status = FAIL;
            }
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not purge tuples marked for deletion\n");
            e.printStackTrace();
            return status;
        }


        return status;
    }
    protected boolean test5() {
        System.out.println("\n  Test 5: Running Column Index scan on the table using Int keys\n");
        boolean status = OK;

        Columnarfile f;
        TID insertedVal = new TID(5);
        try {
            System.out.println("  - Opening the columnar file and adding an entry");
            f = new Columnarfile("test_file");
            byte[] dataArray = new byte[8 + 25];
            ValueIntClass val1 = new ValueIntClass(3);
            ValueIntClass val2 = new ValueIntClass(45);
            System.arraycopy (val1.getByteArr(), 0, dataArray, 0, 4);
            System.arraycopy (val2.getByteArr(), 0, dataArray, 4, 4);
            Convert.setStrValue("Shashank", 8, dataArray);
            insertedVal = f.insertTuple(dataArray);

        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not open the columnar file\n");
            e.printStackTrace();
            return status;
        }

        try {
            System.out.println("  - Trying to created btree index on the 2nd column\n");
            f.createBTreeIndex(1);
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not create b tree index\n");
            e.printStackTrace();
            return status;
        }
        String indexName = "test_file.hdr." + String.valueOf(1) + ".Btree";

        try {
            System.out.println("  - Trying to query and get the first record using the index\n");
            // set up an identity selection
            CondExpr[] expr = new CondExpr[2];
            expr[0] = new CondExpr();
            expr[0].op = new AttrOperator(AttrOperator.aopEQ);
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].type2 = new AttrType(AttrType.attrInteger);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
            expr[0].operand2.integer = 25;
            expr[0].next = null;
            expr[1] = null;

            AttrType[] attrTypes = new AttrType[3];
            attrTypes[0] = new AttrType(1);
            attrTypes[1] = new AttrType(1);
            attrTypes[2] = new AttrType(0);
            short[] s1_sizes = new short[25];
            short len_in1 = 3;
            ColumnIndexScan cfscan = new ColumnIndexScan(new IndexType(1),  "test_file", indexName,
                    attrTypes[0],  (short)0, expr, false);

            Tuple newtuple = cfscan.get_next();
            if (newtuple == null || newtuple.getIntFld(1) != 15 || !newtuple.getStrFld(3).equals("ketola")){
                status = FAIL;
            }

            cfscan.close();
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not Scan using Btree index\n");
            e.printStackTrace();
            return status;
        }

        try {
            System.out.println(" - Marking the last inserted tuple for deletion\n");
            status = f.markTupleDeleted(insertedVal);
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not Mark tuple deleted\n");
            e.printStackTrace();
            return status;
        }

        try {
            System.out.println(" - purging the deleted record\n");
            Boolean pass = f.purgeAllDeletedTuples();
            if (!pass){
                status = FAIL;
            }
            if (f.getTupleCnt() != 9){
                status = FAIL;
            }
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not purge tuples marked for deletion\n");
            e.printStackTrace();
            return status;
        }

        return status;
    }
    protected boolean test6(){
        System.out.println("\n  Test 6: Running Column Index scan on the table using String keys\n");
        boolean status = OK;

        Columnarfile f;
        TID insertedVal = new TID(5);
        try {
            System.out.println("  - Opening the columnar file and adding an entry");
            f = new Columnarfile("test_file");
            byte[] dataArray = new byte[8 + 25];
            ValueIntClass val1 = new ValueIntClass(3);
            ValueIntClass val2 = new ValueIntClass(45);
            System.arraycopy (val1.getByteArr(), 0, dataArray, 0, 4);
            System.arraycopy (val2.getByteArr(), 0, dataArray, 4, 4);
            Convert.setStrValue("Shashank", 8, dataArray);
            insertedVal = f.insertTuple(dataArray);

        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not open the columnar file\n");
            e.printStackTrace();
            return status;
        }

        try {
            System.out.println("  - Trying to created btree index on the 3rd column\n");
            f.createBTreeIndex(2);
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not create b tree index\n");
            e.printStackTrace();
            return status;
        }
        String indexName = "test_file.hdr." + String.valueOf(2) + ".Btree";

        try {
            System.out.println("  - Trying to query and get the first record using the index\n");
            // set up an identity selection
            CondExpr[] expr = new CondExpr[2];
            expr[0] = new CondExpr();
            expr[0].op = new AttrOperator(AttrOperator.aopEQ);
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].type2 = new AttrType(AttrType.attrString);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 3);
            expr[0].operand2.string = "ketola";
            expr[0].next = null;
            expr[1] = null;

            AttrType[] attrTypes = new AttrType[3];
            attrTypes[0] = new AttrType(1);
            attrTypes[1] = new AttrType(1);
            attrTypes[2] = new AttrType(0);
            short[] s1_sizes = new short[25];
            short len_in1 = 3;
            ColumnIndexScan cfscan = new ColumnIndexScan(new IndexType(1),  "test_file", indexName,
                    attrTypes[2],  (short)25, expr, false);

            Tuple newtuple = cfscan.get_next();
            if (newtuple == null || newtuple.getIntFld(1) != 15 || !newtuple.getStrFld(3).equals("ketola")){
                status = FAIL;
            }

            cfscan.close();
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not Scan using Btree index\n");
            e.printStackTrace();
            return status;
        }

        try {
            System.out.println(" - Marking the last inserted tuple for deletion\n");
            status = f.markTupleDeleted(insertedVal);
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not Mark tuple deleted\n");
            e.printStackTrace();
            return status;
        }

        try {
            System.out.println(" - purging the deleted record\n");
            Boolean pass = f.purgeAllDeletedTuples();
            if (!pass){
                status = FAIL;
            }
            if (f.getTupleCnt() != 9){
                status = FAIL;
            }
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not purge tuples marked for deletion\n");
            e.printStackTrace();
            return status;
        }

        return status;}



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