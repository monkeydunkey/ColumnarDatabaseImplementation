package tests;

import btree.*;
import global.*;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import columnar.*;
import java.io.*;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import diskmgr.pcounter;
import iterator.*;

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
        //int valConst_Value = 0;
        String valConst_Value = "";
        int numBuf = 0;
        int columnCount = 0;
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
                //valConst_Value = Integer.parseInt(args[i].substring( 0, args[i].length() - 1 )); // Removes } from VALUE
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

        System.out.println( "Running query test...\n" );

        SystemDefs sysdef = new SystemDefs( dbName, numBuf+20, numBuf, "Clock" ); // Open DB w/ user specified buff pages

        try {
            cFile = new Columnarfile(cfName);
            columnCount = cFile.numColumns;
        }
        catch( Exception E ) {
            Runtime.getRuntime().exit(1);
        }

        boolean success = false;

        // Checking to see whether value is a string or an integer
        boolean isInt = false;
        try
        {
            Integer.parseInt(valConst_Value);
            isInt = true;
        }
        catch (NumberFormatException ex)
        {
            isInt = false;
        }

        if( accessType == "FILESCAN" || accessType == "COLUMNSCAN")
        {
            // Get data from "Headerfile" heapfile of columnar file created above
            // Call FileScan consructor with relevant information
            // Consider TARGETCOLUMNNAMES here
            boolean fScanNOTcScan = true;
            FileScan fScanObj;
            ColumnarFileScan cScanObj;
            if(accessType != "FILESCAN") {
                fScanNOTcScan = false;
                cScanObj = null;
            }else {
                fScanObj = null;
            }

            CondExpr[] outFilter = new CondExpr[1];
            outFilter[0] = new CondExpr();
            switch(valConst_Operator) {
                case "=":
                    outFilter[1].op = new AttrOperator(AttrOperator.aopEQ);
                    break;
                case ">":
                    outFilter[1].op = new AttrOperator(AttrOperator.aopGT);
                    break;
                case "<":
                    outFilter[1].op = new AttrOperator(AttrOperator.aopLT);
                    break;
                case "<=":
                    outFilter[1].op = new AttrOperator(AttrOperator.aopLE);
                    break;
                case ">=":
                    outFilter[1].op = new AttrOperator(AttrOperator.aopGE);
                    break;
                case "!=":
                    outFilter[1].op = new AttrOperator(AttrOperator.aopNE);
                    break;
                default:
                    success = false;
                    break;
            }
            outFilter[1].next  = null;
            outFilter[1].type1 = new AttrType(AttrType.attrSymbol);
            outFilter[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), cFile.getIndexByName(valConst_ColName));

            if( isInt )
            {
                outFilter[1].type2 = new AttrType(AttrType.attrInteger);
                outFilter[1].operand2.integer = Integer.parseInt(valConst_Value);
                /// have to add negative condition for type mismatch
            }
            else
            {
                outFilter[1].type2 = new AttrType(AttrType.attrString);
                outFilter[1].operand2.string = valConst_Value;
                /// have to add negative condition for type mismatch
            }

            Tuple t = new Tuple();
            AttrType [] types = cFile.type;
            short [] sizes = new short[1];
            sizes[0] = 25; //first elt. is 25

            FldSpec[] projection = new FldSpec[columnCount];
            String[] trgtColNamesArr = new String[targetColNames.size()];
            trgtColNamesArr = targetColNames.toArray(trgtColNamesArr);
            for (int i = 0 ; i < trgtColNamesArr.length; i++) {
                projection[i] = new FldSpec(new RelSpec(RelSpec.outer), cFile.getIndexByName(trgtColNamesArr[i]));
            }

            try {
                if(fScanNOTcScan) {
                    fScanObj = new FileScan(cfName, types, sizes, (short)columnCount,
                            (short)columnCount, projection, outFilter);
                }else{
                    cScanObj = new ColumnarFileScan(cfName, types, sizes, (short)columnCount,
                            (short)columnCount, projection, outFilter);
                }
            }catch (Exception e) {
                //status = FAIL;
                System.err.println (""+e);
            }
        }
        else if( accessType == "BTREE" || accessType == "BITMAP")
        {
            // success = cFile.createBTreeIndex(valConst_ColName.{GETCOLNUM});
            // Need a way of finding the column number given the column name
            KeyClass hiKey, lowKey;
            switch(valConst_Operator) { // todo add <=, >=, != ???
                case "=":
                    if(isInt)
                    {
                        hiKey = new IntegerKey(Integer.parseInt(valConst_Value));
                        lowKey = new IntegerKey(Integer.parseInt(valConst_Value));
                    }
                    else
                    {
                        hiKey = new StringKey(valConst_Value);
                        lowKey = new StringKey(valConst_Value);
                    }
                    break;
                case ">":
                    hiKey = null;
                    if(isInt)
                    {
                        lowKey = new IntegerKey(Integer.parseInt(valConst_Value));
                    }
                    else
                    {
                        lowKey = new StringKey(valConst_Value);
                    }
                    break;
                case "<":
                    if(isInt)
                    {
                        hiKey = new IntegerKey(Integer.parseInt(valConst_Value));
                    }
                    else
                    {
                        hiKey = new StringKey(valConst_Value);
                    }
                    lowKey = null;
                    break;
                default:
                    hiKey = new StringKey(valConst_Value);
                    lowKey = new StringKey(valConst_Value);
                    break;

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
        else
        {
            System.out.println("Error - ACCESSTYPE should be either FILESCAN, COLUMNSCAN, BTREE, or BITMAP!");
        }

        System.out.println("query test finished!\n");
        System.out.println("Disk read count: " + pcounter.rcounter);
        System.out.println("Disk write count: " + pcounter.wcounter);
    }
}