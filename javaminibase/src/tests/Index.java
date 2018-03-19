package tests;

import global.*;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import btree.BT;
import btree.BTreeFile;
import columnar.*;
import java.io.*;
import java.util.Arrays;

import diskmgr.pcounter;

public class Index extends TestDriver implements GlobalConst {
    /*
     * Command line invocation of:
     * index COLUMNDBNAME COLUMNARFILENAME COLUMNNAME INDEXTYPE
     * COLUMNDBNAME, COLUMNARFILENAME, COLUMNNAME and INDEXTYPE are strings
     * INDEXTYPE is either BTREE or BITMAP
     */

    private int TRUE  = 1;
    private int FALSE = 0;
    private boolean OK = true;
    private boolean FAIL = false;

    public static void main( String argv[] ) {
        pcounter.initialize(); // Initializes read & write counters to 0

        String filePath = "./";
        System.out.println( "COLUMNDBNAME: " + argv[0] ); // Name of DB previously inserted
        System.out.println( "COLUMNARFILE: " + argv[1] );
        System.out.println( "COLUMNNAME: " + argv[2] );
        System.out.println( "INDEXTYPE: " + argv[3] );

        System.out.println( "Running index tests..." );

        try {
            SystemDefs sysdef = new SystemDefs( dbpath, NUMBUF+20, NUMBUF, "Clock" );
        }
        catch( Exception E ) {
            Runtime.getRuntime().exit(1);
        }

        // TODO - Implement rest of program
    }
}