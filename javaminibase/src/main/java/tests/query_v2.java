package tests;

import btree.BTreeFile;
import columnar.Columnarfile;
import diskmgr.pcounter;
import global.*;
import heap.Tuple;
import index.ColumnIndexScan;
import index.ColumnarIndexScan;
import iterator.*;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class query_v2 implements GlobalConst {
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
        String queryPatt = "(\\w+)\\s(\\w+)\\s\\[((\\w+\\s?)*)\\]\\s\\{((\\w+\\s(=|!=|>|<)\\s\\w+(,\\s)?)*)\\}\\s(\\d+)";
        Pattern pattern = Pattern.compile(queryPatt);

        pcounter.initialize(); // Initializes read & write counters to 0
        String queryString = String.join(" ", args);
        String str = "";
        String dbName = "";
        String cfName = "";
        String[] singleSelects;
        String valConst_Value = "";
        int numBuf = 0;
        int columnCount = 0;
        String accessType = "";
        String[] targetColNames;
        IndexType[] colIndex;
        String[] colIndexNames;
        AttrType[] targetColType = null;
        boolean colNamesDone = false;
        Columnarfile cFile = null;
        int newIndex = 0;
        CondExpr[] condList;
        System.out.println(queryString);
        Matcher matcher = pattern.matcher(queryString);
        String[] parsedArr = new String[10];
        int count = 0;
        while (matcher.find()) {
            for (int j = 0; j <= matcher.groupCount(); j++) {
                System.out.println("------------------------------------");
                System.out.println("Group " + count + ": " + matcher.group(j));
                parsedArr[count] = matcher.group(j);
                count++;
            }
        }

        if (count == 10) {
            dbName = parsedArr[1];
            cfName = parsedArr[2];
            targetColNames = parsedArr[3].split("\\s+");
            singleSelects = parsedArr[5].split(",\\s");
            System.out.println(parsedArr[9]);
            numBuf = Integer.parseInt(parsedArr[9]);
        }
        else{
            System.out.println("Regex Match failed");
            return;
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

        try {

            cFile = new Columnarfile(cfName);
            columnCount = cFile.numColumns;
            //Creating the projection list and storing the output column types
            FldSpec[] projection = new FldSpec[targetColNames.length];
            targetColType = new AttrType[targetColNames.length];
            for(int i = 0; i < targetColType.length; i++) {

                ValueClass val = cFile.getColumnTypeByName(targetColNames[i]);
                if (val instanceof ValueIntClass) {
                    targetColType[i] = new AttrType(1);
                } else {
                    targetColType[i] = new AttrType(0);
                }
                projection[i] = new FldSpec(new RelSpec(RelSpec.outer),
                        cFile.getColumnIndexByName(targetColNames[i])+1);
            }

            condList = new CondExpr[singleSelects.length + 1];
            colIndex = new IndexType[singleSelects.length];
            colIndexNames = new String[singleSelects.length];
            //Creating the selection query
            for(int i = 0; i < singleSelects.length; i++) {
                boolean success = false;
                boolean isInt = false;
                String[] conditionStatement = singleSelects[i].split("\\s+");
                condList[i] = new CondExpr();
                if (i > 0){
                    condList[i - 1].next = condList[i];
                }
                condList[i].type1 = new AttrType(AttrType.attrSymbol);
                int colInd = cFile.getColumnIndexByName(conditionStatement[0]);
                condList[i].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),
                        colInd + 1);
                colIndex[i] = cFile.indexType[colInd];
                if (colIndex[i].indexType == IndexType.None){
                    colIndexNames[i] = "";
                } else {
                    colIndexNames[i] = cfName + ".hdr." + String.valueOf(colInd) + ((colIndex[i].indexType == IndexType.B_Index) ? ".Btree" : ".BitMap");
                }

                int opType = AttrOperator.aopEQ;
                switch(conditionStatement[1]) {
                    case "=":
                        opType = AttrOperator.aopEQ;
                        break;
                    case ">":
                        opType = AttrOperator.aopGT;
                        break;
                    case "<":
                        opType = AttrOperator.aopLT;
                        break;
                    case "<=":
                        opType = AttrOperator.aopLE;
                        break;
                    case ">=":
                        opType = AttrOperator.aopGE;
                        break;
                    case "!=":
                        opType = AttrOperator.aopNE;
                        break;
                    case "default":
                        success = false;
                        System.out.println("None of the operator matched deafulting to equality");
                        break;
                }
                condList[i].op = new AttrOperator(opType);
                try{
                    Integer.parseInt(conditionStatement[2]);
                    isInt = true;
                } catch (NumberFormatException ex){
                    isInt = false;
                }
                condList[i].type2 =  new AttrType((isInt) ?  AttrType.attrInteger: AttrType.attrString);
                if( isInt )
                {
                    condList[i].operand2.integer = Integer.parseInt(conditionStatement[2]);
                }else{
                    condList[i].operand2.string = conditionStatement[2];
                }
            }
            condList[condList.length - 1] = null;
            AttrType [] types = cFile.type;

            ArrayList<Integer> stringLengths = new ArrayList<Integer>();
            for (int j = 0; j < types.length; j++){
                if (types[j].attrType == AttrType.attrString){
                    stringLengths.add(cFile.offsets[j]);
                }
            }
            short[] stringSize = new short[stringLengths.size()];
            for (int j = 0; j < stringSize.length; j++){
                stringSize[j] = stringLengths.get(j).shortValue();
            }
            ColumnarIndexScan cfscan = new ColumnarIndexScan(cfName, colIndex, colIndexNames, types, stringSize,
                    types.length, targetColNames.length, projection, condList, false);


            TID emptyTID = new TID(5);
            Tuple newtuple = cfscan.get_next(emptyTID);

            while(newtuple != null) {
                newtuple.print(targetColType);
                newtuple = cfscan.get_next(emptyTID);
            }
            cfscan.close();
        }
        catch( Exception E ) {
            E.printStackTrace();
            Runtime.getRuntime().exit(1);
        }



        try {
            SystemDefs.JavabaseBM.resetAllPins();
            SystemDefs.JavabaseBM.flushAllPages();
        } catch (Exception ex){
            System.out.println("could not flush the pages");
            ex.printStackTrace();
        }
        System.out.println("query test finished!\n");
        System.out.println("Disk read count: " + pcounter.rcounter);
        System.out.println("Disk write count: " + pcounter.wcounter);

    }
}
