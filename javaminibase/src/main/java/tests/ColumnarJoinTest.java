package tests;

import iterator.*;
import heap.*;
import global.*;
import index.*;
import java.io.*;
import java.util.*;
import java.lang.*;
import btree.*;

class ColumnarJoinsDriver implements GlobalConst {

    private boolean OK = true;
    private boolean FAIL = false;
    /**
     * Constructor
     */
    public ColumnarJoinsDriver() {

    }

    public boolean runTests() {
        System.out.print ("Finished columnar nested loop joins testing"+"\n");
        return true;
    }
}

public class ColumnarJoinTest extends TestDriver implements GlobalConst {
    public static void main(String argv[])
    {
        boolean status;
        ColumnarJoinsDriver jjoin = new ColumnarJoinsDriver();
        status = jjoin.runTests();

        if(!status) {
            System.out.println("Error ocurred during columnar join tests");
        }
        else {
            System.out.println("Columnar join tests completed successfully");
        }
    }
}
