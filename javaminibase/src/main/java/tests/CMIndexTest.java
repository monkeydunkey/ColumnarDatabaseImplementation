package tests;

import java.io.*;
import java.lang.*;
import heap.*;
import global.*;
import columnar.*;
import iterator.*;

/**
 * Note that in JAVA, methods can't be overridden to be more private.
 * Therefore, the declaration of all private functions are now declared
 * protected as opposed to the private type in C++.
 */

class CMIndexDriver extends TestDriver implements GlobalConst {

    private final static boolean OK = true;
    private final static boolean FAIL = false;

    private int choice;
    private final static int reclen = 32;

    int[] data_1 = {1, 20, 13, 42, 12, 12, 24, 4};
    int[] data_2 = {3, 40, 23, 92, 25, 10, 11, 41};
    private static String data_3[] = {
            "xbao", "xbao", "cychan", "cychan", "ketola", "soma", "yuc",
            "ketola", "marc", "yuc", "soma", "ketola", "susanc", "kinc",
            "marc", "scottc", "yuc", "yung", "rathgebe", "joyce", "daode",
            "yuvadee", "he", "huxtable", "muerle", "flechtne", "susanc", "jhowe",
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
    public CMIndexDriver() {
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
        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                != SystemDefs.JavabaseBM.getNumBuffers()) {
            System.err.println("*** Opening columnar file has left pinned pages\n");
            status = FAIL;
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

        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                != SystemDefs.JavabaseBM.getNumBuffers()) {
            System.err.println("*** The heap file has left pages pinned " + SystemDefs.JavabaseBM.getNumUnpinnedBuffers() + "\n");
            status = FAIL;
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

        insertedVal = new TID(4);
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

        return status;
    }
    protected boolean test3() {
        System.out.println("Test 3: Testing columnar index Scan - Only And Condition");
        boolean status = OK;

        Columnarfile f;
        TID insertedVal = new TID(4);
        try {
            f = new Columnarfile("test_file");
            System.out.println("  - Trying to created btree index on the 2nd column\n");
            f.createBTreeIndex(1);
            System.out.println("  - Trying to created btree index on the 1st column\n");
            f.createBTreeIndex(0);
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not create b tree index\n");
            e.printStackTrace();
            return status;
        }
        String[] indexName = {"test_file.hdr." + String.valueOf(0) + ".Btree", "test_file.hdr." + String.valueOf(1) + ".Btree"};



        try {
            System.out.println("Setting up the selection condition");
            // set up an identity selection
            CondExpr[] expr = new CondExpr[3];
            expr[0] = new CondExpr();
            expr[0].op = new AttrOperator(AttrOperator.aopEQ);
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].type2 = new AttrType(AttrType.attrInteger);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
            expr[0].operand2.integer = 13;

            expr[1] = new CondExpr();
            expr[1].op = new AttrOperator(AttrOperator.aopEQ);
            expr[1].type1 = new AttrType(AttrType.attrSymbol);
            expr[1].type2 = new AttrType(AttrType.attrString);
            expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 3);
            expr[1].operand2.string = "cychan";

            expr[0].next = null;
            expr[1].next = null;
            expr[2] = null;

            FldSpec[] proj_list = new FldSpec[1];
            proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 2);
            int n_out_flds = 1;

            AttrType[] attrTypes = new AttrType[3];
            attrTypes[0] = new AttrType(1);
            attrTypes[1] = new AttrType(1);
            attrTypes[2] = new AttrType(0);
            short[] s1_sizes = {25};
            short len_in1 = 3;

            IndexType[] indexes = {new IndexType(IndexType.B_Index), new IndexType(IndexType.None)};
            ColumnarIndexScan cfscan = new ColumnarIndexScan("test_file", indexes, indexName, attrTypes, s1_sizes,
                                                                len_in1, n_out_flds, proj_list, expr, false);

            System.out.println("Scan object Created");

            Tuple newtuple = cfscan.get_next();

            int v1 = newtuple.getIntFld(1);
            if (v1 != 23){
                System.out.println(v1);
                status = FAIL;
            }
            /*
            String v1 = newtuple.getStrFld(1);
            if (!v1.equals("xbao")){
                System.out.println(v1);
                status = FAIL;
            }
            */
            cfscan.close();

        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not apply filters on columnar file \n");
            e.printStackTrace();
            return status;
        }


        try {
            System.out.println("Setting up the selection condition");
            // set up an identity selection
            CondExpr[] expr = new CondExpr[3];
            expr[0] = new CondExpr();
            expr[0].op = new AttrOperator(AttrOperator.aopEQ);
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].type2 = new AttrType(AttrType.attrInteger);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
            expr[0].operand2.integer = 69;

            expr[1] = new CondExpr();
            expr[1].op = new AttrOperator(AttrOperator.aopEQ);
            expr[1].type1 = new AttrType(AttrType.attrSymbol);
            expr[1].type2 = new AttrType(AttrType.attrString);
            expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 3);
            expr[1].operand2.string = "cychan";

            expr[0].next = null;
            expr[1].next = null;
            expr[2] = null;

            FldSpec[] proj_list = new FldSpec[1];
            proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 2);
            int n_out_flds = 1;

            AttrType[] attrTypes = new AttrType[3];
            attrTypes[0] = new AttrType(1);
            attrTypes[1] = new AttrType(1);
            attrTypes[2] = new AttrType(0);
            short[] s1_sizes = {25};
            short len_in1 = 3;

            IndexType[] indexes = {new IndexType(IndexType.B_Index), new IndexType(IndexType.None)};
            ColumnarIndexScan cfscan = new ColumnarIndexScan("test_file", indexes, indexName, attrTypes, s1_sizes,
                    len_in1, n_out_flds, proj_list, expr, false);

            System.out.println("Scan object Created");

            Tuple newtuple = cfscan.get_next();
            if (newtuple != null){
                System.out.println("The query returned some value " + newtuple.getIntFld(1));
                status = FAIL;
            }
            cfscan.close();

        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not apply filters on columnar file \n");
            e.printStackTrace();
            return status;
        }

        return status;
    }
    protected boolean test4() {

        System.out.println("Test 4: Testing columnar index Scan with BTree and Bitmap Only And Condition");
        boolean status = OK;

        Columnarfile f;
        TID insertedVal = new TID(4);
        try {
            f = new Columnarfile("test_file");
            System.out.println("  - Trying to create bitmap index on the 3rd column\n");
            f.createBitMapIndex(2, f.type[2].getValueClass());
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not create b tree index\n");
            e.printStackTrace();
            return status;
        }
        String[] indexName = {"test_file.hdr." + String.valueOf(0) + ".Btree", "test_file.hdr." + String.valueOf(2) + ".BitMap"};



        try {
            System.out.println("Setting up the selection condition");
            // set up an identity selection
            CondExpr[] expr = new CondExpr[3];
            expr[0] = new CondExpr();
            expr[0].op = new AttrOperator(AttrOperator.aopLE);
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].type2 = new AttrType(AttrType.attrInteger);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
            expr[0].operand2.integer = 13;

            expr[1] = new CondExpr();
            expr[1].op = new AttrOperator(AttrOperator.aopEQ);
            expr[1].type1 = new AttrType(AttrType.attrSymbol);
            expr[1].type2 = new AttrType(AttrType.attrString);
            expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 3);
            expr[1].operand2.string = "ketola";

            expr[0].next = null;
            expr[1].next = null;
            expr[2] = null;

            FldSpec[] proj_list = new FldSpec[3];
            proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
            proj_list[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
            proj_list[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
            int n_out_flds = 3;

            AttrType[] attrTypes = new AttrType[3];
            attrTypes[0] = new AttrType(1);
            attrTypes[1] = new AttrType(1);
            attrTypes[2] = new AttrType(0);
            short[] s1_sizes = {25};
            short len_in1 = 3;

            IndexType[] indexes = {new IndexType(IndexType.B_Index), new IndexType(IndexType.BitMapIndex)};
            ColumnarIndexScan cfscan = new ColumnarIndexScan("test_file", indexes, indexName, attrTypes, s1_sizes,
                    len_in1, n_out_flds, proj_list, expr, false);

            System.out.println("Scan object Created");

            Tuple newtuple;
            for (int i =0;i<2;i++){
                newtuple = cfscan.get_next();
                AttrType[] outtypes = {new AttrType(1), new AttrType(1), new AttrType(0)};
                newtuple.print(outtypes);
            }

            if (cfscan.get_next() != null){
                status = FAIL;
                System.out.println("More results returned than expected");
            }
            /*
            String v1 = newtuple.getStrFld(1);
            if (!v1.equals("xbao")){
                System.out.println(v1);
                status = FAIL;
            }
            */
            cfscan.close();

        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not apply filters on columnar file \n");
            e.printStackTrace();
            return status;
        }

        return status;
    }
    protected boolean test5() {
        Boolean status = OK;
        return status;
    }
    protected boolean test6(){

        boolean status = OK;
        return status;
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

        return "Columnar Index Scan Test";
    }
}

public class CMIndexTest {

    public static void main(String argv[]) {

        CMIndexDriver hd = new CMIndexDriver();
        boolean dbstatus;

        dbstatus = hd.runTests();

        if (dbstatus != true) {
            System.err.println("Error encountered during Columnar Index Scan tests:\n");
            Runtime.getRuntime().exit(1);
        }

        Runtime.getRuntime().exit(0);
    }
}