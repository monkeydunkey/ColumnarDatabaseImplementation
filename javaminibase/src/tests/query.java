package tests;

import btree.IntegerKey;
import btree.KeyClass;
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
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;

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

    public static void run( String[] args ) {
        pcounter.initialize(); // Initializes read & write counters to 0

        String str = "";
        String dbName = "";
        String cfName = "";
        String valConst_ColName = "";
        String valConst_Operator = "";
        int valConst_Value = 0;
        int numBuf = 0;
        String accessType = "";
        List<String> targetColNames = new ArrayList<>();
        boolean colNamesDone = false;
        Columnarfile cFile = null;
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
                valConst_Value = Integer.parseInt(args[i].substring( 0, args[i].length() - 1 )); // Removes } from VALUE
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

        SystemDefs sysdef = new SystemDefs( dbName, numBuf+20, numBuf, "Clock" ); // Open DB w/ user specified buff pages

        try {
            cFile = new Columnarfile(cfName);
        }
        catch( Exception E ) {
            Runtime.getRuntime().exit(1);
        }

        boolean success = false;

        if( accessType == "FILESCAN" )
        {
            // Get data from "Headerfile" heapfile of columnar file created above
            // Call FileScan consructor with relevant information
            // Consider TARGETCOLUMNNAMES here
            CondExpr[] outFilter = new CondExpr[1];
            outFilter[0] = new CondExpr();

            outFilter[1].op    = new AttrOperator(AttrOperator.aopEQ);
            outFilter[1].next  = null;
            outFilter[1].type1 = new AttrType(AttrType.attrSymbol);
            outFilter[1].type2 = new AttrType(AttrType.attrInteger);
            outFilter[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),2);
            outFilter[1].operand2.integer = valConst_Value;

            Tuple t = new Tuple();

            AttrType [] types = cFile.type;

            short [] sizes = new short[1];
            sizes[0] = 25; //first elt. is 25

            FldSpec[] projection = new FldSpec[4];
            projection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
            projection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
            projection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
            projection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

            CondExpr [] selects = new CondExpr [1];
            selects = null;


            FileScan am = null;
            try {
                am  = new FileScan(cfName, types, sizes, (short)4, (short)4, projection, null);
            }
            catch (Exception e) {
                status = FAIL;
                System.err.println (""+e);
            }
        }
        else if( accessType == "COLUMNSCAN" )
        {
            // Get data from "Headerfile" heapfile of columnar file created above
            // Call ColumnarFileScan consructor with relevant information
            // Consider TARGETCOLUMNNAMES here
        }
        else if( accessType == "BTREE" )
        {
            // success = cFile.createBTreeIndex(valConst_ColName.{GETCOLNUM});
            // Need a way of finding the column number given the column name
            KeyClass hiKey, lowKey;
            if( valConst_Operator == "=" )
            {
                hiKey = new IntegerKey(valConst_Value);
                lowKey = new IntegerKey(valConst_Value);
            }
            else if( valConst_Operator == ">" )
            {
                hiKey = null;
                lowKey = new IntegerKey(valConst_Value);
            }
            else if( valConst_Operator == "<" )
            {
                hiKey = new IntegerKey(valConst_Value);
                lowKey = null;
            }
            // Consider TARGETCOLUMNNAMES here
            // BTreeFile = ...
            // BTFileScan scan = {BTREEFILE}.new_scan(hiKey, lowKey);
            // KeyDataEntry entry = scan.get_next();
            // if(entry!=null)
            //  System.out.println("SCAN RESULT: "+ entry.key + " " + entry.data);
            // else
            //  System.out.println("AT THE END OF SCAN!");
        }
        else if( accessType == "BITMAP" )
        {
            // success = cFile.createBitMapIndex(valConst_ColName.{GETCOLNUM},{VALUECLASS});
            // Need a way of finding the column number given the column name
            // Consider TARGETCOLUMNNAMES here
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