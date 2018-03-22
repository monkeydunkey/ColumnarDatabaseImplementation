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
import java.util.ArrayList;
import java.util.List;

import diskmgr.pcounter;

public class query extends TestDriver implements GlobalConst {
    /*
     * Command line invocation of:
     * query COLUMNDBNAME COLUMNARFILENAME [TARGETCOLUMNNAMES] VALUECONSTRAINT NUMBUF ACCESSTYPE
     * COLUMNDBNAME and COLUMNARFILENAME are strings
     * TARGETCOLUMNNAMES is an list of column names of the form "[TC1 TC2 ... TCN]"
     * VALUECONSTRAINT is of the form "{COLUMNNAME OPERATOR VALUE}"
     * NUMBUF is an integer (MiniBase will use at most NUMBUF buffer pages to run the query)
     * ACCESSTYPE is a string; valid inputs are "FILESCAN", "COLUMNSCAN", "BTREE", or "BITMAP"
     */

    private int TRUE  = 1;
    private int FALSE = 0;
    private boolean OK = true;
    private boolean FAIL = false;

    public static void main( String[] args ) {
        pcounter.initialize(); // Initializes read & write counters to 0

        String str = "";
        String dbName = "";
        String cfName = "";
        String valConst_ColName = "";
        String valConst_Operator = "";
        String valConst_Value = "";
        int numBuf = 0;
        String accessType = "";
        List<String> targetColNames = new ArrayList<>();
        boolean colNamesDone = false;
        int newIndex = 0;

        for( int i = 0; i < args.length; i++ )
        {
            if( i == 0 ) // COLUMNDBNAME
                dbName = args[i];
            else if( i == 1 ) // COLUMNARFILENAME
                cfName = args[i];
            else if( args[i].charAt(0) == '[' ) // Start of TAGETCOLUMNNAMES list
            {
                str = args[i].substring(1);

                if( args[i].charAt( args[i].length() - 1 ) == ']' ) // Covers case of only one element in TAGETCOLUMNNAMES list
                {
                    str = str.substring( 0, str.length() - 1 );
                    targetColNames.add(str);
                    colNamesDone = true;
                    newIndex = i + 1; // Pointing to start of VALUECONSTRAINT
                }
                else // First element in TARGETCOLUMNNAMES list
                {
                    targetColNames.add(str);
                }
            }
            else if( args[i].charAt( args[i].length() - 1 ) == ']' ) // Last element in TARGETCOLUMNNAMES list
            {
                    str = args[i].substring( 0, args[i].length() - 1 );
                    targetColNames.add(str);
                    colNamesDone = true;
                    newIndex = i + 1; // Pointing to start of VALUECONSTRAINT
            }
            else if( colNamesDone == false ) // In the middle of the TARGETCOLUMNNAMES list
            {
                targetColNames.add(args[i]);
            }
            else if( i == newIndex ) // VALUECONSTRAINT -> COLUMNNAME
            {
                valConst_ColName = args[i].substring(1); // Removes { from COLUMNNAME
            }
            else if( i == newIndex + 1 ) // VALUECONSTRAINT -> OPERATOR
            {
                valConst_Operator = args[i];
            }
            else if( i == newIndex + 2 ) // VALUECONSTRAINT -> VALUE
            {
                valConst_Value = args[i].substring( 0, args[i].length() - 1 ); // Removes } from VALUE
            }
            else if( i == newIndex + 3 ) // NUMBUF
            {
                numBuf = Integer.parseInt(args[i]);
            }
            else // ACCESSTYPE
            {
                accessType = args[i];
            }
        }

        /* Uncomment for debugging */
        /* System.out.println("COLUMNDNAME: " + dbName);
         * System.out.println("COLUMNARFILENAME: " + cfName);
         * System.out.println("TARGETCOLUMNNAMES: ");
         * for( int i = 0; i < targetColNames.size(); i++ )
         * {
         *   System.out.println(targetColNames.get(i));
         * }
         * System.out.println("VALUECONSTRAINT COLUMNNAME: " + valConst_ColName);
         * System.out.println("VALUECONSTRAINT OPERATOR: " + valConst_Operator);
         * System.out.println("VALUECONSTRAINT VALUE: " + valConst_Value);
         * System.out.println("NUMBUF: " + numBuf);
         * System.out.println("ACCESSTYPE: " + accessType);
         */

        System.out.println( "Running query tests...\n" );

        SystemDefs sysdef = new SystemDefs( dbName, numBuf+20, numBuf, "Clock" );

        // Open the ColumnDB using the provided string
        //ColumnDB cDB = new ColumnDB();
        //cDB.openDB(dbName);
        // Retrieve the Columnarfile using the provided string
        try {
            Columnarfile cFile = new Columnarfile(cfName);
        }
        catch( Exception E ) {
            Runtime.getRuntime().exit(1);
        }

        if( accessType == "FILESCAN" )
        {

        }
        else if( accessType == "COLUMNSCAN" )
        {

        }
        else if( accessType == "BTREE" )
        {

        }
        else if( accessType == "BITMAP" )
        {

        }
        else
        {
            System.out.println("Error - ACCESSTYPE should be either FILESCAN, COLUMNSCAN, BTREE, or BITMAP!");
        }

        System.out.println("query tests finished!\n");
        System.out.println("Disk read count: " + pcounter.rcounter);
        System.out.println("Disk write count: " + pcounter.wcounter);
    }
}