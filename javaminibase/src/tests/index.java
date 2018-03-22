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
     * COLUMNDBNAME, COLUMNARFILENAME, and INDEXTYPE
     * COLUMNNAME is an integer of the column index
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
        int colNum = Integer.parseInt(args[2]);
        String ixType = args[3];

        /* Uncomment for debugging */
        /*
         *String filePath = "./";
         * System.out.println( "COLUMNDBNAME: " + colDBName );
         * System.out.println( "COLUMNARFILE: " + colFile );
         * System.out.println( "COLUMNNAME: " + colName );
         * System.out.println( "INDEXTYPE: " + ixType );
         */

        System.out.println( "Running index tests...\n" );

        try {
            SystemDefs sysdef = new SystemDefs( colDBName, NUMBUF+20, NUMBUF, "Clock" );
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
        ColumnDB cDB = new ColumnDB();
        cDB.openDB(colDBName);
        // Retrieve the Columnarfile using the provided string
        Columnarfile cFile = new Columnarfile(colFile);
        
        bool success = false;

        if( ixType == "BTREE" )
        {
        	success = cFile.createBTreeIndex(colNum);
            BTreeFile btFile = new BTreeFile(cFile);
        }
        else if( ixType == "BITMAP" )
        {
            success = cFile.createBitMapIndex(colName, cFile.type[colNum]);
            BitMapFile bmFile = new BitMapFile(cFile);
        }
        else
        {
        	System.out.println("Error - INDEXTYPE should be either BTREE or BITMAP!");
        }

        System.out.println("Index tests finished!\n");
        System.out.println("Disk read count: " + pcounter.rcounter); // Maybe subtract from intital count?
        System.out.println("Disk write count: " + pcounter.wcounter);
    }
}