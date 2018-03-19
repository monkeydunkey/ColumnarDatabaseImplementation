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

public class DeleteQuery extends TestDriver implements GlobalConst {
    /*
     * Command line invocation of:
     * delete_query COLUMNDBNAME COLUMNARFILENAME [TARGETCOLUMNNAMES] VALUECONSTRAINT NUMBUF ACCESSTYPE PURGED
     * COLUMNDBNAME and COLUMNARFILENAME are strings
     * TARGETCOLUMNNAMES is an array of strings
     * VALUECONSTRAINT is of the form "{COLUMNNAME OPERATOR VALUE}"
     * NUMBUF is an integer (MiniBase will use at most NUMBUF buffer pages to run the query
     * ACCESSTYPE is a string; valid inputs are "FILESCAN", "COLUMNSCAN", "BTREE", or "BITMAP"
     * PURGED is a boolean flag; 0 -> deleted tuples will not be purged from DB, 1 -> deleted tuples will be purged from DB
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
        System.out.println( "[TARGETCOLUMNNAMES]: " + argv[2] );
        System.out.println( "VALUECONSTRAINT: " + argv[3] );
        System.out.println( "NUMBUF: " + argv[4] );
        System.out.println( "ACCESSTYPE: " + argv[5] );
        System.out.println( "PURGED: " + argv[6] );

        System.out.println( "Running delete_query tests..." );

        try {
            SystemDefs sysdef = new SystemDefs( dbpath, NUMBUF+20, NUMBUF, "Clock" );
        }
        catch( Exception E ) {
            Runtime.getRuntime().exit(1);
        }

        // TODO - Implement rest of program
    }
}