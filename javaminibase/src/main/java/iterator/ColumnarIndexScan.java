package iterator;
import columnar.*;
import global.*;
import btree.*;
import index.IndexException;
import index.UnknownIndexTypeException;
import heap.*;
import java.io.*;
import java.util.*;
/**
 * Created by shashankbhushan on 4/17/18.
 */
public class ColumnarIndexScan extends Iterator{
    /*
     Following is how the flow is going to look like. The incoming condition is assumed to be in conjuctive normal
     form i.e. it will be a conjunctions of disjunctions.
                        (P V Q) ^ (C V D)
     The way we are going to process the query is that we will process each of the disjunctions concurrently
     and store the positions returned in a hashset, there will be a hash for each of the disjunctive sub conditions.
     Additionally we will also have a hashmap storing the total count of a position across the different
     sub conditions. If the count for a given position becomes equal to the count of the number of sub conditions
     then we fetch that tuple (only required column) and return the tuple
     */

    /*
    **UPDATED**
     Following is how the flow is going to be a linked list each with just one condition connected by OR operators.
     This is how it is given in CondExpr.java

     So we are going to have a hashset of the position that we have returned till now, for each condition's get next
     if the position returned is not present in the hashset then we add it to the hashset and return the required
     columns
     */
    /*
        relName: Name of the columnarFile
        fldNum:
        index: List containing the type of index on each of the fields used in the condition. Should be None i.e. 0
               if the field has no index on it
        indName: List containing names of all the indexes. If there is no index on a given condition, leave the
                 string empty ("")
        types: Types of attributes
        noInFlds: Number of input fields
        noOutFlds: Number of output fields
        outFlds: The columns that needs to be returned
        selects: List of condition expressions
        indexOnly: Boolean flag specifying if the query only needs index or not
    */
    private String relName;
    private IndexType[] indexTypes;
    private String[] indexNames;
    private AttrType[] attrTypes;
    private short[] str_sizes;
    private int noInFlds;
    private int noOutFlds;
    private FldSpec[] outFlds;
    private CondExpr[] selects;
    boolean indexOnly;
    private IndexFile indFile;
    private Object[] indScan;
    private Columnarfile f;
    private int roundRobinInd = 0;
    private ColumnarIndexScanPosition[] orConditions;
    HashMap<Integer, Integer> positions;
    HashMap<Integer, TID> positionsTID;
    Tuple Jtuple;
    public ColumnarIndexScan(String relName, IndexType[] index,
                             String[] indName, AttrType[] types, short[] str_sizes,
                             int noInFlds, int noOutFlds, FldSpec[] outFlds,
                             CondExpr[] selects, boolean indexOnly)
            throws IndexException,
            InvalidTypeException,
            UnknownIndexTypeException
    {

        positions = new HashMap<Integer, Integer>();
        positionsTID = new HashMap<Integer, TID>();
        orConditions = new ColumnarIndexScanPosition[selects.length - 1];
        this.relName = relName;
        this.indexTypes = index;
        this.indexNames = indName;
        this.attrTypes = types;
        this.str_sizes = str_sizes;
        this.noInFlds = noInFlds;
        this.noOutFlds = noOutFlds;
        this.outFlds = outFlds;
        this.selects = selects;
        this.indexOnly = indexOnly;
        int indexInd = 0;
        indScan = new Object[index.length];
        try {
            f = new Columnarfile(relName);
        } catch (Exception e) {
            throw new IndexException(e, "ColumnIndexScan.java: Heapfile not created");
        }

        for (int i = 0; i < selects.length - 1; i++) {
            int orCondLength = 0;
            CondExpr tempExpr = selects[i];
            while (tempExpr != null){
                orCondLength++;
                tempExpr = tempExpr.next;
            }
            //System.out.println("Or Condition length: " + orCondLength);
            orConditions[i] = new ColumnarIndexScanPosition(f,
                    Arrays.copyOfRange(index, indexInd, indexInd + orCondLength),
                    Arrays.copyOfRange(indName, indexInd, indexInd + orCondLength),
                    selects[i],
                    indexOnly);
            indexInd += orCondLength;
        }


    }

    public static void Project(Columnarfile  f1, TID tid, AttrType type1[],
                               Tuple Jtuple, FldSpec  perm_mat[],
                               int nOutFlds
    )
            throws UnknowAttrType,
            WrongPermat,
            FieldNumberOutOfBoundException,
            IOException,
            Exception
    {


        for (int i = 0; i < nOutFlds; i++)
        {
            int ind = perm_mat[i].offset-1;
            switch (perm_mat[i].relation.key)
            {
                case RelSpec.outer:      // Field of outer (t1)
                    switch (type1[ind].attrType)
                    {
                        case AttrType.attrInteger:
                            Jtuple.setIntFld(i+1, Convert.getIntValue(0, f1.columnFile[ind].getRecord(tid.recordIDs[ind]).getTupleByteArray()));
                            break;
                        case AttrType.attrReal:
                            Jtuple.setFloFld(i+1, Convert.getFloValue(0, f1.columnFile[ind].getRecord(tid.recordIDs[ind]).getTupleByteArray()));
                            break;
                        case AttrType.attrString:
                            Jtuple.setStrFld(i+1, Convert.getStrValue(0, f1.columnFile[ind].getRecord(tid.recordIDs[ind]).getTupleByteArray(), f1.offsets[ind]));
                            break;
                        default:

                            throw new UnknowAttrType("Don't know how to handle attrSymbol, attrNull");

                    }
                    break;

                default:

                    throw new WrongPermat("something is wrong in perm_mat");

            }
        }
        return;
    }

    public Tuple get_next()
        throws ScanIteratorException,
            UnknownIndexTypeException,
            IndexException,
            InvalidRelation,
            TupleUtilsException,
            IOException,
            Exception
    {
        Tuple retTup = null;
        int runCount = 0;
        Object nextentry;
        TID tempTID = null;
        Integer position = null;
        while (retTup == null && runCount < selects.length - 1){
            runCount += 1;
            //System.out.println("It goes in loop");
            tempTID = orConditions[roundRobinInd].get_next();
            roundRobinInd = (roundRobinInd + 1) % (selects.length - 1);
            runCount = (tempTID == null) ? runCount : 0;

            if (tempTID != null){
                position = tempTID.position;
                if (positions.containsKey(position)){
                    positions.put(position, positions.get(position) + 1);
                } else{
                    positions.put(position, 1);
                }

                if (positions.get(position) >= selects.length - 1){
                    //We got this position from all the condition
                    //System.out.println("It does come here as well " + tempTID.position + " " + tempTID.numRIDs);
                    retTup =  new Tuple();
                    AttrType[] Jtypes = new AttrType[noOutFlds];
                    short[]    ts_size;
                    ts_size = TupleUtils.setup_op_tuple(retTup, Jtypes, attrTypes, noInFlds, str_sizes, outFlds, noOutFlds);
                    Project(f, tempTID, attrTypes, retTup, outFlds, noOutFlds);
                    break;
                }
            }
        }
       return retTup;
    }

    public void close()
            throws IOException,
            JoinsException,
            SortException,
            IndexException
    {
        for (int i = 0; i < orConditions.length; i++) {
            try {
                orConditions[i].close();
            } catch (Exception ex){
                System.out.println("There was an error in closing the iterator");
                ex.printStackTrace();
            }
        }
    }
}
