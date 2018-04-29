package tests;

import columnar.Columnarfile;
import diskmgr.pcounter;
import global.*;
import heap.Tuple;
import iterator.ColumnarIndexScan;
import iterator.*;

import java.io.File;
import java.util.ArrayList;
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

    public static void run( String[] args )
        throws Exception
    {
        String queryPatt = "(\\w+)\\s(\\w+)\\s\\[((\\w+\\s?)*)\\]\\s\\{((\\w+\\s(=|!=|>|<)\\s\\w+((\\sAND\\s|\\sOR\\s))?)*)\\}\\s(\\d+)";
        Pattern pattern = Pattern.compile(queryPatt);

        pcounter.initialize(); // Initializes read & write counters to 0
        String queryString = String.join(" ", args);
        String str = "";
        String dbName = "";
        String cfName = "";
        String[] OrSelects;
        String valConst_Value = "";
        int numBuf = 0;
        int columnCount = 0;
        String accessType = "";
        String[] targetColNames;
        ArrayList<IndexType> colIndex;
        ArrayList<String> colIndexNames;
        AttrType[] targetColType = null;
        boolean colNamesDone = false;
        Columnarfile cFile = null;
        int newIndex = 0;
        int indexCount = 0;
        CondExpr[] condList;
        System.out.println(queryString);
        Matcher matcher = pattern.matcher(queryString);
        String[] parsedArr = new String[11];
        int count = 0;
        while (matcher.find()) {
            for (int j = 0; j <= matcher.groupCount(); j++) {
//                System.out.println("------------------------------------");
//                System.out.println("Group " + count + ": " + matcher.group(j));
                parsedArr[count] = matcher.group(j);
                count++;
            }
        }

        if (count == 11) {
            dbName = parsedArr[1];
            cfName = parsedArr[2];
            targetColNames = parsedArr[3].split("\\s+");
            OrSelects = parsedArr[5].split("\\s+AND\\s+");
            System.out.println(parsedArr[10]);
            numBuf = Integer.parseInt(parsedArr[10]);
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

        File db_file = new File(dbName);
        if(db_file.exists()) {	// file found
            System.out.printf("An existing database (%s) was found, opening database with %d buffers.\n", dbName, numBuf);
            // open database with 100 buffers
            SystemDefs sysdef = new SystemDefs(dbName,0,numBuf,"Clock");
        }else
        {
            throw new Exception("EXCEPTION: Database provided: " + dbName + " was not found, exiting.");
        }

        System.out.println( "Running query test...\n" );

        try {
            cFile = new Columnarfile(cfName.trim());
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

            condList = new CondExpr[OrSelects.length + 1];
            System.out.println("Conditions: " + OrSelects[0]);
            colIndex = new ArrayList<IndexType>();//IndexType[singleSelects.length];
            colIndexNames = new ArrayList<String>();//String[singleSelects.length];
            //Creating the selection query
            for(int i = 0; i < OrSelects.length; i++) {
                boolean success = false;
                boolean isInt = false;
                String[] conditionStatements = OrSelects[i].split("\\s+OR\\s+");
                condList[i] = new CondExpr();
                CondExpr tempExpr = condList[i];
                for (int j = 0; j < conditionStatements.length; j++){
                    System.out.println(conditionStatements[j]);
                    String[] selectparts = conditionStatements[j].split("\\s+");
                    tempExpr.type1 = new AttrType(AttrType.attrSymbol);
                    int colInd = cFile.getColumnIndexByName(selectparts[0]);
                    tempExpr.operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),
                            colInd + 1);
                    colIndex.add(cFile.indexType[colInd]);
                    if (cFile.indexType[colInd].indexType == IndexType.None){
                        colIndexNames.add("");
                    } else {
                        colIndexNames.add(cfName + ".hdr." + String.valueOf(colInd) + ((cFile.indexType[colInd].indexType == IndexType.B_Index) ? ".Btree" : ".BitMap"));
                    }
                    System.out.println("Index name: " + colIndexNames.get(colIndexNames.size() - 1));
                    int opType = AttrOperator.aopEQ;
                    switch(selectparts[1]) {
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
                    tempExpr.op = new AttrOperator(opType);
                    try{
                        Integer.parseInt(selectparts[2]);
                        isInt = true;
                    } catch (NumberFormatException ex){
                        isInt = false;
                    }
                    tempExpr.type2 =  new AttrType((isInt) ?  AttrType.attrInteger: AttrType.attrString);
                    if( isInt )
                    {
                        tempExpr.operand2.integer = Integer.parseInt(selectparts[2]);
                    }else{
                        tempExpr.operand2.string = selectparts[2];
                    }
                    if (j < conditionStatements.length - 1){
                        tempExpr.next = new CondExpr();
                    }
                    tempExpr = tempExpr.next;
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
            IndexType[] colIndexArr = new IndexType[colIndex.size()];
            String[] ccolIndexNamesArr = new String[colIndex.size()];
            for (int j = 0; j < colIndex.size(); j++){
                colIndexArr[j] = colIndex.get(j);
                ccolIndexNamesArr[j] = colIndexNames.get(j);
                System.out.println("IndexName: " + ccolIndexNamesArr[j] + " index type " + colIndexArr[j]);
            }
            ColumnarIndexScan cfscan = new ColumnarIndexScan(cfName, colIndexArr, ccolIndexNamesArr, types, stringSize,
                    types.length, targetColNames.length, projection, condList, false);


            Tuple newtuple = cfscan.get_next();
            while(newtuple != null) {
                newtuple.print(targetColType);
                newtuple = cfscan.get_next();
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
            SystemDefs.JavabaseDB.closeDB();
        } catch (Exception ex){
            System.out.println("could not flush the pages");
            ex.printStackTrace();
        }
        System.out.println("query test finished!\n");
        System.out.println("Disk read count: " + pcounter.rcounter);
        System.out.println("Disk write count: " + pcounter.wcounter);

    }
}
