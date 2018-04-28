package tests;

import columnar.Columnarfile;
import diskmgr.pcounter;
import global.*;
import heap.Tuple;
import iterator.ColumnarIndexScan;
import iterator.*;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NestedLoopJoinCommand {

    public static void createSelectionQuery(CondExpr[] cond, String[] AndCond, Columnarfile cf,
                                            ArrayList<IndexType> colIndex, ArrayList<String> colIndexNames){

        //Creating the outer selection query
        for(int i = 0; i < cond.length; i++) {
            boolean success = false;
            boolean isInt = false;
            String[] conditionStatements = AndCond[i].split("\\s+OR\\s+");
            cond[i] = new CondExpr();
            CondExpr tempExpr = cond[i];
            for (int j = 0; j < conditionStatements.length; j++){
                String[] selectparts = conditionStatements[j].split("\\s+");
                tempExpr.type1 = new AttrType(AttrType.attrSymbol);
                int colInd = cf.getColumnIndexByName(selectparts[0]);
                tempExpr.operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),
                        colInd + 1);
                colIndex.add(cf.indexType[colInd]);
                if (cf.indexType[colInd].indexType == IndexType.None){
                    colIndexNames.add("");
                } else {
                    colIndexNames.add(cond + ".hdr." + String.valueOf(colInd) + ((cf.indexType[colInd].indexType == IndexType.B_Index) ? ".Btree" : ".BitMap"));
                }

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
            }


        }
        cond[cond.length - 1] = null;
    }


    public static void createJoinQuery(CondExpr[] cond, String[] AndCond, Columnarfile outerFile,
                                       Columnarfile innerFile){

        //Creating the outer selection query
        for(int i = 0; i < cond.length; i++) {
            boolean success = false;
            boolean isInt = false;
            String[] conditionStatements = AndCond[i].split("\\s+OR\\s+");
            cond[i] = new CondExpr();
            CondExpr tempExpr = cond[i];
            for (int j = 0; j < conditionStatements.length; j++){
                String[] selectparts = conditionStatements[j].split("\\s+");
                tempExpr.type1 = new AttrType(AttrType.attrSymbol);
                int colInd = outerFile.getColumnIndexByName(selectparts[0].split("\\.")[1]);
                tempExpr.operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),
                        colInd + 1);

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
                tempExpr.type1 = new AttrType(AttrType.attrSymbol);
                colInd = innerFile.getColumnIndexByName(selectparts[2].split("\\.")[1]);
                tempExpr.operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),
                        colInd + 1);
            }


        }
        cond[cond.length - 1] = null;
    }


    public static void run(String[] split) {
        pcounter.initialize();
        String dbName = "";
        String OuterFileName = "";
        String InnerFileName = "";
        String[] OuterConstraints;
        String[] InnerConstraints;
        String[] JoinConstraints;
        //String OuterAccType = "";
        String[] TargetColumns;
        int NumBuf;

        //Function Signature :
        //nlj COLUMNDB OUTERFILE INNERFILE OUTERCONST INNERCONST JOINCONST [TARGETCOLUMNS] NUMBUF

        //final String regex = "(\\w+) (\\w+) (\\w+) (\\{.+?\\}) (\\{.+?\\}) (\\{.+?\\}) (\\w+) (\\[.+?\\])\\s(.+$)";
        final String regex = "(\\w+) (\\w+) (\\w+) \\{((\\w+ (=|!=|>|<) \\w+(( AND | OR ))?)*)\\} \\{((\\w+ (=|!=|>|<) \\w+(( AND | OR ))?)*)\\} \\{((\\w+\\.\\w+ (=|!=|>|<) \\w+\\.\\w+(( AND | OR ))?)*)\\} \\[((\\w+\\.\\w+ ?)*)\\] (\\d+)";

        final String inputString = String.join(" ", split);
        final Pattern pattern = Pattern.compile(regex);
        final Matcher matcher = pattern.matcher(inputString);
        String[] parsedArr = new String[22];
        int parsedArr_count = 0;

        while (matcher.find()) {
            System.out.println("Full match: " + matcher.group(0));
            for (int i = 1; i <= matcher.groupCount(); i++) {
                System.out.println("------------------------------------");
                System.out.println("Group " + parsedArr_count + ": " + matcher.group(i));
                parsedArr[parsedArr_count] = matcher.group(i);
                parsedArr_count++;
            }
        }

        if (parsedArr_count == 21) {
            //Setting variables from given query
            dbName = parsedArr[0];
            OuterFileName = parsedArr[1];
            InnerFileName = parsedArr[2];
            OuterConstraints = parsedArr[3].split("\\s+AND\\s+");
            InnerConstraints = parsedArr[8].split("\\s+AND\\s+");
            JoinConstraints = parsedArr[13].split("\\s+AND\\s+");
            //OuterAccType = parsedArr[6];
            TargetColumns = parsedArr[18].split("\\s+");
            NumBuf = Integer.parseInt(parsedArr[20]);
        } else {
            System.out.println("Regex Match failed");
            return;
        }


        try {

            Columnarfile outerFile = new Columnarfile(OuterFileName);
            Columnarfile innerFile = new Columnarfile(InnerFileName);
            //columnCount = cFile.numColumns;
            //Creating the projection list and storing the output column types
            FldSpec[] projection = new FldSpec[TargetColumns.length];
            AttrType[] targetColType = new AttrType[TargetColumns.length];
            for(int i = 0; i < targetColType.length; i++) {
                String[] colParts = TargetColumns[i].split("\\.");
                ValueClass val = null;
                if (OuterFileName.equals(colParts[0])){
                    projection[i] = new FldSpec(new RelSpec(RelSpec.outer),
                            outerFile.getColumnIndexByName(colParts[1])+1);
                    val = outerFile.getColumnTypeByName(colParts[1]);
                } else {
                    projection[i] = new FldSpec(new RelSpec(RelSpec.innerRel),
                            innerFile.getColumnIndexByName(colParts[1])+1);
                    val = innerFile.getColumnTypeByName(colParts[1]);
                }

                if (val instanceof ValueIntClass) {
                    targetColType[i] = new AttrType(1);
                } else {
                    targetColType[i] = new AttrType(0);
                }

            }
            //Forming the outer condition expr
            CondExpr[] OuterCondList = new CondExpr[OuterConstraints.length + 1];
            ArrayList<IndexType> colIndex = new ArrayList<IndexType>();//IndexType[singleSelects.length];
            ArrayList<String> colIndexNames = new ArrayList<String>();//String[singleSelects.length];
            createSelectionQuery(OuterCondList, OuterConstraints, outerFile, colIndex, colIndexNames);

            //Forming the inner condition expr
            CondExpr[] InnerCondList = new CondExpr[InnerConstraints.length + 1];
            ArrayList<IndexType> InnerColIndex = new ArrayList<IndexType>();//IndexType[singleSelects.length];
            ArrayList<String> innerColIndexNames = new ArrayList<String>();//String[singleSelects.length];
            createSelectionQuery(InnerCondList, InnerConstraints, innerFile, InnerColIndex, innerColIndexNames);

            AttrType [] outerTypes = outerFile.type;
            FldSpec[] OuterProjection = new FldSpec[outerTypes.length];
            ArrayList<Integer> stringLengths = new ArrayList<Integer>();
            for (int j = 0; j < outerTypes.length; j++){
                OuterProjection[j] = new FldSpec(new RelSpec(RelSpec.outer), j+1);
                if (outerTypes[j].attrType == AttrType.attrString){
                    stringLengths.add(outerFile.offsets[j]);
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
            }



            ColumnarIndexScan cfscan = new ColumnarIndexScan(OuterFileName, colIndexArr, ccolIndexNamesArr, outerTypes, stringSize,
                    outerTypes.length, OuterProjection.length, OuterProjection, OuterCondList, false);

            CondExpr[] joinCondition = new CondExpr[JoinConstraints.length + 1];
            createJoinQuery(joinCondition, JoinConstraints, outerFile, innerFile);

            ColumnarNestedLoopJoins nlj = new ColumnarNestedLoopJoins(outerFile.type, outerFile.numColumns, stringSize,
                    innerFile.type, innerFile.numColumns, stringSize, NumBuf, cfscan, InnerFileName, joinCondition,
                    InnerCondList, projection, projection.length);


            Tuple newtuple = nlj.get_next();

            while(newtuple != null) {
                newtuple.print(targetColType);
                newtuple = nlj.get_next();
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