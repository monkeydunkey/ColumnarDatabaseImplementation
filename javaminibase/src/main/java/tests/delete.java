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
import global.ValueStrClass;
import heap.*;
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

public class delete implements GlobalConst {
    /*
     * Command line invocation of:
     * delete_query COLUMNDBNAME COLUMNARFILENAME [TARGETCOLUMNNAMES] VALUECONSTRAINT NUMBUF ACCESSTYPE PURGED
     * COLUMNDBNAME and COLUMNARFILENAME are strings
     * TARGETCOLUMNNAMES is an list of column names of the form "[TC1 TC2 ... TCN]"
     * VALUECONSTRAINT is of the form "{COLUMNNAME OPERATOR VALUE}"
     * NUMBUF is an integer (MiniBase will use at most NUMBUF buffer pages to run the query)
     * ACCESSTYPE is a string; valid inputs are "FILESCAN", "COLUMNSCAN", "BTREE", or "BITMAP"
     * PURGED is a boolean flag; 0 -> deleted tuples will not be purged from DB, 1 -> deleted tuples will be purged from DB
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
        boolean purgeFlag = false;

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
            else if( i == newIndex + 4 ) // ACCESSTYPE
            {
                accessType = args[i];
            }
            else // PURGED
            {
                if (args[i].equals("1")) {
                    purgeFlag = true;
                }
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

        System.out.println( "Running delete_query test...\n" );

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
        outFilter[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),
                cFile.getColumnIndexByName(valConst_ColName)+1);

        if( isInt )
        {
            outFilter[0].type2 = new AttrType(AttrType.attrInteger);
            outFilter[0].operand2.integer = Integer.parseInt(valConst_Value);
        }else{
            outFilter[0].type2 = new AttrType(AttrType.attrString);
            outFilter[0].operand2.string = valConst_Value;
        }
        outFilter[1] = null;

        Tuple t;
        AttrType [] types = cFile.type;
        int strCount = 0;

        if( accessType.equals("FILESCAN") || accessType.equals("COLUMNSCAN") )
        {
            boolean fScanNOTcScan = true;
            ColumnarFileScan fScanObj = null;
            FileScan cScanObj = null;

            if(!accessType.equals("FILESCAN")) { fScanNOTcScan = false; }

            String[] trgtColNamesArr = new String[targetColNames.size()];
            trgtColNamesArr = targetColNames.toArray(trgtColNamesArr);

            FldSpec[] projection = new FldSpec[targetColNames.size()];
            for (int i = 0 ; i < trgtColNamesArr.length; i++) {
                projection[i] = new FldSpec(new RelSpec(RelSpec.outer),
                        cFile.getColumnIndexByName(trgtColNamesArr[i])+1);
            }

            for (int i = 0 ; i < columnCount; i++) {
                ValueClass colValCls = cFile.getColumnTypeByName(cFile.columnNames[i]);
                if (colValCls instanceof ValueStrClass) {
                    strCount += 1;
                }
            }
            boolean setDelFlag = false;
            short[] sizes = new short[strCount];
            for( int i = 0; i < sizes.length; i++ )
            {
                sizes[i] = (short)25;
            }
            try {
                if(fScanNOTcScan) {
                    fScanObj  = new ColumnarFileScan(cfName, types, sizes, (short)columnCount,
                            trgtColNamesArr.length, projection, outFilter);

                    t = fScanObj.get_next(true);
                    while(t != null) {
                        t = fScanObj.get_next(true);
                    }
                    if(purgeFlag){
                        if(!cFile.purgeAllDeletedTuples()){
                            success = false;
                        }
                    }
                    fScanObj.close();
                }else{
                    fScanObj  = new ColumnarFileScan(cfName, types, sizes, (short)columnCount,
                            trgtColNamesArr.length, projection, outFilter);

                    t = fScanObj.get_next(true);
                    while(t != null) {
                        t = fScanObj.get_next(true);
                    }
                    if(purgeFlag){
                        if(!cFile.purgeAllDeletedTuples()){
                            success = false;
                        }
                    }

                    fScanObj.close();
                }
            }catch (Exception e) {
                success = false;
                System.err.println (""+e);
                e.printStackTrace();
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
                    indexName = cfName+".hdr." + String.valueOf(cFile.getColumnIndexByName(valConst_ColName)) + ".Btree";
                    ciScanObj = new ColumnIndexScan(new IndexType(1), cfName, indexName,
                            colAttrType, bSize, outFilter, false);

                    t = ciScanObj.get_next(true);
                    while(t != null) {
                        t = ciScanObj.get_next(true);
                    }
                    if(purgeFlag){
                        if(!cFile.purgeAllDeletedTuples()){
                            success = false;
                        }
                    }
                    ciScanObj.close();
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
        System.out.println("delete_query test finished!\n");
        System.out.println("Disk read count: " + pcounter.rcounter);
        System.out.println("Disk write count: " + pcounter.wcounter);

    }
//    public static boolean markTupleDel(Tuple t) throws InvalidUpdateException,
//            InvalidTupleSizeException,
//            HFException,
//            HFDiskMgrException,
//            HFBufMgrException,
//            Exception {
//        try{
//            System.out.println("marking Tuple Deletions");
//            if (!cFile.markTupleDeleted(cFile.deserializeTuple(t.getTupleByteArray()))){
//                return false;
//            }
//        }catch(Exception e){
//            e.printStackTrace();
//            return false;
//        }
//        return true;
//    }
//    public static boolean purgeTuples() throws InvalidTupleSizeException, IOException, Exception {
//        System.out.println("Purging Tuples: " + purgeFlag);
//        if(purgeFlag){
//            try{
//                if(!cFile.purgeAllDeletedTuples()){
//                    return false;
//                }
//            }catch(Exception e){
//                e.printStackTrace();
//                return false;
//            }
//        }
//        return true;
//    }
}