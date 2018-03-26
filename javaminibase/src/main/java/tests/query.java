package tests;

import btree.*;
import columnar.Columnarfile;
import global.*;
import global.AttrOperator;
import global.AttrType;
import global.GlobalConst;
import global.IndexType;
import global.ValueClass;
import global.ValueIntClass;
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
import iterator.ColumnarFileScan;
import index.ColumnIndexScan;
import iterator.FldSpec;
import iterator.RelSpec;

public class query implements GlobalConst {
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
        String valConst_Value = "";
        int numBuf = 0;
        int columnCount = 0;
        String accessType = "";
        List<String> targetColNames = new ArrayList<>();
        AttrType[] targetColType = null;
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
                valConst_Value = (args[i].substring( 0, args[i].length() - 1 )); // Removes } from VALUE
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

        try {
            cFile = new Columnarfile(cfName);
            columnCount = cFile.numColumns;
            targetColType = new AttrType[targetColNames.size()];
            for(int i = 0; i < targetColType.length; i++) {

                ValueClass val = cFile.getColumnTypeByName(targetColNames.get(i));
                if (val instanceof ValueIntClass) {
                    targetColType[i] = new AttrType(1);
                } else {
                    targetColType[i] = new AttrType(0);
                }
            }
        }
        catch( Exception E ) {
            Runtime.getRuntime().exit(1);
        }

        boolean success = false;
        boolean isInt = false;

        try{
            Integer.parseInt(valConst_Value);
            isInt = true;
        } catch (NumberFormatException ex){
            isInt = false;
        }

        CondExpr[] outFilter = new CondExpr[2];
        outFilter[0] = new CondExpr();
        outFilter[1] = new CondExpr();
        switch(valConst_Operator) {
            case "=":
                outFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
                break;
            case ">":
                outFilter[0].op = new AttrOperator(AttrOperator.aopGT);
                break;
            case "<":
                outFilter[0].op = new AttrOperator(AttrOperator.aopLT);
                break;
            case "<=":
                outFilter[0].op = new AttrOperator(AttrOperator.aopLE);
                break;
            case ">=":
                outFilter[0].op = new AttrOperator(AttrOperator.aopGE);
                break;
            case "!=":
                outFilter[0].op = new AttrOperator(AttrOperator.aopNE);
                break;
            case "default":
                success = false;
                break;
        }
        outFilter[0].next  = null;
        outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
        outFilter[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),
                cFile.getColumnIndexByName(valConst_ColName));

        if( isInt )
        {
            outFilter[0].type2 = new AttrType(AttrType.attrInteger);
            outFilter[0].operand2.integer = Integer.parseInt(valConst_Value);
            /// have to add negative condition for type mismatch
        }else{
            outFilter[0].type2 = new AttrType(AttrType.attrString);
            outFilter[0].operand2.string = valConst_Value;
            /// have to add negative condition for type mismatch
        }
        outFilter[1] = null;

        Tuple t = new Tuple();
        AttrType [] types = cFile.type;
        short [] sizes = new short[1];
        sizes[0] = 25;
        if( accessType.equals("FILESCAN") || accessType.equals("COLUMNSCAN") )
        {
            boolean fScanNOTcScan = true;
            ColumnarFileScan cScanObj = null;
            FileScan fScanObj = null;

            if(!accessType.equals("FILESCAN")) { fScanNOTcScan = false; }

            FldSpec[] projection = new FldSpec[columnCount];
            String[] trgtColNamesArr = new String[targetColNames.size()];
            trgtColNamesArr = targetColNames.toArray(trgtColNamesArr);
            for (int i = 0 ; i < trgtColNamesArr.length; i++) {
                projection[i] = new FldSpec(new RelSpec(RelSpec.outer),
                        cFile.getColumnIndexByName(trgtColNamesArr[i]));
            }
            try {
                if(fScanNOTcScan) {
                    fScanObj  = new FileScan(cfName, types, sizes, (short)columnCount,
                            (short)columnCount, projection, outFilter);
                    t = new Tuple();
                    t = fScanObj.get_next();
                    while(t != null) {
                        //t.print(targetColType);
                        t = fScanObj.get_next();
                    }
                }else{
                    cScanObj  = new ColumnarFileScan(cfName, types, sizes, (short)columnCount,
                            (short)columnCount, projection, outFilter);
                    t = new Tuple();
                    t = cScanObj.get_next();
                    while(t != null) {
                        //t.print(targetColType);
                        t = cScanObj.get_next();
                    }
                }
            }catch (Exception e) {
                success = false;
                System.err.println (""+e);
            }
        }
        else if( accessType.equals("BTREE") || accessType.equals("BITMAP") )
        {
            ColumnIndexScan ciScanObj = null;
            boolean btScanNOTbmScan = true;
            short bSize = 25;
            String indexName = "";
            AttrType colAttrType = null;

            if(!accessType.equals("BTREE"))
                btScanNOTbmScan = false;

            ValueClass colValCls = cFile.getColumnTypeByName(valConst_ColName);
            if (colValCls instanceof ValueIntClass) {
                colAttrType = new AttrType(1);
            } else {
                colAttrType = new AttrType(0);
            }

            try {
                if(btScanNOTbmScan) {
                    cFile.createBTreeIndex(cFile.getColumnIndexByName(valConst_ColName));
                    indexName = cfName + String.valueOf(cFile.getColumnIndexByName(valConst_ColName)) + ".Btree";
                    ciScanObj = new ColumnIndexScan(new IndexType(1), cfName, indexName,
                            colAttrType, bSize, outFilter, false);

                    t = new Tuple();
                    t = ciScanObj.get_next();
                    while(t != null) {
                        //t.print(targetColType);
                        t = ciScanObj.get_next();
                    }
                }else {
//                    cFile.createBitMapIndex(cFile.getColumnIndexByName(valConst_ColName), colValCls);
//                    indexName = cfName + String.valueOf(cFile.getColumnIndexByName(valConst_ColName)) + ".Bitmap";
//                    ciScanObj = new ColumnIndexScan(new IndexType(0), cfName, indexName,
//                            colAttrType, bSize, outFilter, false);
                }
            }catch(Exception e) {
                e.printStackTrace();
            }
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
