package tests;

import global.*;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import btree.BT;
import btree.BTreeFile;
import columnar.*;
import bitmap.*;
import java.io.*;
import java.util.Arrays;

import diskmgr.pcounter;

public class index extends TestDriver implements GlobalConst {
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

    public static void main( String[] args ) {
        pcounter.initialize(); // Initializes read & write counters to 0

        String colDBName = args[0];
        String colFile = args[1];
        String colName = args[2];
        String ixType = args[3];

        /* Uncomment for debugging */
        /*
         *String filePath = "./";
         * System.out.println( "COLUMNDBNAME: " + args[0] ); // Name of DB previously inserted?
         * System.out.println( "COLUMNARFILE: " + args[1] );
         * System.out.println( "COLUMNNAME: " + args[2] );
         * System.out.println( "INDEXTYPE: " + args[3] );
         */

        System.out.println( "Running index tests..." );

        try {
            SystemDefs sysdef = new SystemDefs( dbpath, NUMBUF+20, NUMBUF, "Clock" );
        }
        catch( Exception E ) {
            Runtime.getRuntime().exit(1);
        }

        // TODO - Implement rest of program
        /*
         * Logic
         * Use COLUMNDBNAME to lookup the database (e.g.colDB1)
         * Use COLUMNARFILENAME to look up columnar file
         * Use COLUMNNAME to lookup attribute
         * Use INDEX to select how column in indexed
         */

        // Open the ColumnDB using the provided string
        ColumnDB.openDB(colDBName);
        // Retrieve the Columnarfile using the provided string
        Columnarfile cFile = new Columnarfile(colFile);
        // find columnNo (not implemented yet)
        int columnNo = cFile.getColumnNo(colName);
        
        bool success = false;

        if( ixType == "BTREE" )
        {
        	success = cFile.createBTreeIndex(columnNo);
        }
        else if( ixType == "BITMAP" )
        {
            ValueClass valClass = new ValueClass();
            success = cFile.createBitMapIndex(columnNo, valClass);
        }
        else
        {
        	System.out.println("Error - INDEXTYPE should be either BTREE or BITMAP!");
        }

        System.out.println("Index tests finished!");
        System.out.println("Disk read count: " + pcounter.rcounter);
        System.out.println("Disk write count: " + pcounter.wcounter);
    }
}