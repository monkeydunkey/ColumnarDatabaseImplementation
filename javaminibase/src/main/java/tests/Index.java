package tests;

import btree.*;
import bufmgr.*;
import global.*;
import heap.*;
import columnar.*;
import bitmap.*;
import java.io.*;
import java.util.Arrays;

import diskmgr.pcounter;

public class Index extends TestDriver implements GlobalConst {
    /*
     * Command line invocation of:
     * index COLUMNDBNAME COLUMNARFILENAME COLUMNNAME INDEXTYPE
     *
     *
     * COLUMNDBNAME, COLUMNARFILENAME, and INDEXTYPE
     * COLUMNNAME is an integer of the column index
     * INDEXTYPE is either BTREE or BITMAP
     */

    private int TRUE  = 1;
    private int FALSE = 0;
    private boolean OK = true;
    private boolean FAIL = false;

    public static void run( String[] args )
            throws HFDiskMgrException, InvalidTupleSizeException, HFException,
            InvalidSlotNumberException, SpaceNotAvailableException, HFBufMgrException,
            IOException, AddFileEntryException, GetFileEntryException, ConstructPageException,
            Exception
    {
        pcounter.initialize(); // Initializes read & write counters to 0

        final String COLUMN_DB_NAME = args[0];
        final String COLUMNAR_FILE_NAME = args[1];
        final String COLUMN_NAME = args[2];
        final String INDEX_TYPE = args[3];

        File db_file = new File(COLUMN_DB_NAME);
        if(db_file.exists()) {	// file found
            System.out.printf("An existing database (%s) was found, opening database.\n", COLUMN_DB_NAME);
            // open database with 100 buffers
            SystemDefs sysdef = new SystemDefs(COLUMN_DB_NAME,0,100,"Clock");
        }else
        {
            throw new Exception("EXCEPTION: Database provided: " + COLUMN_DB_NAME + " was not found, exiting.");
        }

        /* Uncomment for debugging */
        /*
         *String filePath = "./";
         * System.out.println( "COLUMNDBNAME: " + COLUMN_DB_NAME );
         * System.out.println( "COLUMNARFILE: " + COLUMNAR_FILE_NAME );
         * System.out.println( "COLUMNNAME: " + COLUMN_NAME );
         * System.out.println( "INDEXTYPE: " + INDEX_TYPE );
         */

        System.out.println( "Running index test...\n" );

        // TODO - Implement rest of program
        /*
         * Logic
         * Use COLUMN_DB_NAME to lookup the database (e.g.colDB1)
         * Use COLUMNAR_FILE_NAME to look up columnar file
         * Use COLUMN_NAME to lookup attribute
         * Use INDEX_TYPE to select how column in indexed
         */

        Columnarfile cFile = null;

        try {
            // Retrieve the Columnarfile using the provided string
            cFile = new Columnarfile(COLUMNAR_FILE_NAME);
        }
        catch(IOException e) {
            e.printStackTrace();
        }
        
        boolean success = false;

        if( INDEX_TYPE.equals("BTREE") )
        {
            if( cFile.getColumnIndexByName(COLUMN_NAME) == -1 )
            {
                System.out.println("Error - Column doesn't exist");
            }
            else
            {
                success = cFile.createBTreeIndex(cFile.getColumnIndexByName(COLUMN_NAME));
            }
        }
        else if( INDEX_TYPE.equals("BITMAP") )
        {
            // todo determine the type (ValueClass) to send as parameter 2
            int columnIndexByName = cFile.getColumnIndexByName(COLUMN_NAME);
            success = cFile.createBitMapIndex(columnIndexByName, cFile.getColumnTypeByName(COLUMN_NAME));
        }
        else
        {
        	System.out.println("Error - INDEXTYPE should be either BTREE or BITMAP!");
        }
        if(!success)
            System.out.println("Error - Could not create index!");
        else
            System.out.println("SUCCESS - Index created!");
        try {
            SystemDefs.JavabaseBM.resetAllPins();
            SystemDefs.JavabaseBM.flushAllPages();
            SystemDefs.JavabaseDB.closeDB();
        } catch (Exception ex){
            System.out.println("could not flush the pages");
            ex.printStackTrace();
        }
        System.out.println("Index test finished!\n");
        System.out.println("Disk read count: " + pcounter.rcounter);
        System.out.println("Disk write count: " + pcounter.wcounter);
    }
}