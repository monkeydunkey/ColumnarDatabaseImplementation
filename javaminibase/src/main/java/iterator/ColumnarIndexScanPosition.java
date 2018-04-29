package iterator;

import bitmap.BitMapFile;
import bitmap.BitMapFileScan;
import btree.*;
import columnar.Columnarfile;
import global.*;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidTypeException;
import heap.SerializedScan;
import heap.Tuple;
import index.IndexException;
import index.IndexUtils;
import index.UnknownIndexTypeException;
import iterator.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by shashankbhushan on 4/17/18.
 */
public class ColumnarIndexScanPosition {
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
    private CondExpr select;
    boolean indexOnly;
    private IndexFile indFile;
    private Object[] indScan;
    private Columnarfile f;
    private int roundRobinInd = 0;
    HashSet<Integer> positions;
    Tuple Jtuple;
    public ColumnarIndexScanPosition(Columnarfile cf, IndexType[] index,
                                     String[] indName, CondExpr select,
                                     boolean indexOnly)
            throws IndexException,
            InvalidTypeException,
            UnknownIndexTypeException
    {

        positions = new HashSet<Integer>();
        this.indexTypes = index;
        this.indexNames = indName;
        this.select = select;
        CondExpr currSel = select;
        this.indexOnly = indexOnly;
        int indexFileName = 0;
        indScan = new Object[index.length];
        this.f = cf;

        for (int i = 0; i < index.length; i++) {
            CondExpr[] tempExpr = new CondExpr[2];
            tempExpr[1] = null;
            tempExpr[0] = currSel;

            //CondExpr[] cpyArr = {currSel};
            //System.arraycopy(cpyArr, 0, tempExpr, 0, 1);
            tempExpr[0].next = null;
            switch (index[i].indexType) {
                case IndexType.BitMapIndex:
                    try {
                        indFile = new BitMapFile(indName[indexFileName]);
                    } catch (Exception e) {
                        throw new IndexException(e, "ColumnIndexScan.java: BitMap exceptions caught from BitMap constructor");
                    }
                    try {
                        indScan[i] = IndexUtils.BitMap_scan(tempExpr, indFile, f);
                    } catch (Exception e) {
                        throw new IndexException(e, "ColumnIndexScan.java: BitMap exceptions caught from IndexUtils.BitMap_scan().");
                    }
                    indexFileName++;
                    break;

                case IndexType.B_Index:
                    // error check the select condition
                    // must be of the type: value op symbol || symbol op value
                    // but not symbol op symbol || value op value
                    try {
                        indFile = new BTreeFile(indName[indexFileName]);
                    } catch (Exception e) {
                        throw new IndexException(e, "ColumnIndexScan.java: BTreeFile exceptions caught from BTreeFile constructor");
                    }

                    try {
                        //System.out.println("Operand Created " + tempExpr[0].op.toString() + " " + tempExpr[0].type1.toString() + " " + tempExpr[0].operand2.integer + " " + indName[indexFileName] + " " + tempExpr[0].operand1.symbol.offset);
                        if (indFile == null){
                            System.out.printf("Index file is null");
                        }
                        indScan[i] = IndexUtils.BTree_scan(tempExpr, indFile);
                    } catch (Exception e) {
                        throw new IndexException(e, "ColumnIndexScan.java: BTreeFile exceptions caught from IndexUtils.BTree_scan().");
                    }
                    indexFileName++;
                    break;
                case IndexType.None:
                    try {
                        int ColVal = tempExpr[0].operand1.symbol.offset - 1;
                        tempExpr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                        indScan[i] = new SerializedScan(f, ColVal, tempExpr);
                    } catch (Exception e) {
                        throw new IndexException(e, "ColumnIndexScan.java: BTreeFile exceptions caught from IndexUtils.BTree_scan().");
                    }

                    break;
                default:
                    throw new UnknownIndexTypeException("Only BTree, BitMap and column scan supported as of now");

            }
            currSel = currSel.next;
        }


    }

    public TID get_next()
        throws ScanIteratorException,
            UnknownIndexTypeException,
            IndexException,
            InvalidRelation,
            TupleUtilsException,
            IOException,
            Exception
    {
        int runCount = 0;
        Object nextentry;
        TID tempTID = null;
        Integer position = null;
        while (tempTID == null && runCount < indexTypes.length){
            runCount += 1;
            //System.out.println("It goes in loop");
            switch (indexTypes[roundRobinInd].indexType) {
                case IndexType.BitMapIndex:
                    try {
                        nextentry = ((BitMapFileScan)indScan[roundRobinInd]).get_next();
                        if (nextentry != null){
                            RID keyRID = ((LeafData)((KeyDataEntry)nextentry).data).getData();
                            tempTID = f.deserializeTuple(f.columnFile[f.numColumns + 1].getRecord(keyRID).getTupleByteArray());
                            position = tempTID.position;
                        }
                    } catch (Exception e){
                        throw new ScanIteratorException(e, "ColumnarIndexScan.java:  Btree Scan error");
                    }
                    break;

                case IndexType.B_Index:
                    // error check the select condition
                    // must be of the type: value op symbol || symbol op value
                    // but not symbol op symbol || value op value
                    try {
                        nextentry = ((BTFileScan)indScan[roundRobinInd]).get_next();
                        if (nextentry != null){
                            RID keyRID = ((LeafData)((KeyDataEntry)nextentry).data).getData();
                            tempTID = f.deserializeTuple(f.columnFile[f.numColumns + 1].getRecord(keyRID).getTupleByteArray());
                            position = tempTID.position;
                        }

                    } catch (Exception e){
                        throw new ScanIteratorException(e, "ColumnarIndexScan.java:  Btree Scan error");
                    }
                    break;
                case IndexType.None:
                    try {
                        SerializedScan tempScanObj = (SerializedScan)indScan[roundRobinInd];
                        tempTID = tempScanObj.getNextSerialized();
                        while (tempTID != null) {
                            Tuple tt = f.columnFile[tempScanObj.columnVal].getRecord(tempTID.recordIDs[tempScanObj.columnVal]);
                            //As we have to pass it to eval we need to add space for header information
                            int TotalSpaceNeeded = (1 + 2) * 2 + tt.getLength();
                            int headerOffset = (1 + 2) * 2;
                            short [] _string_sizes;
                            if (f.type[tempScanObj.columnVal].attrType == AttrType.attrString){
                                _string_sizes = new short[1];
                                _string_sizes[0] = (short)f.offsets[tempScanObj.columnVal];
                            } else {
                                _string_sizes = new short[0];
                            }
                            AttrType[] colType = {f.type[tempScanObj.columnVal]};
                            //System.out.println("The value comparison is " + new ValueIntClass(tt.getTupleByteArray()).value);
                            //System.out.println("Operand Created " + tempExpr[0].op.toString() + " " + tempExpr[0].type1.toString() + " " + tempExpr[0].operand2.integer + " " + tempScanObj.columnVal);
                            byte[] arr = new byte[TotalSpaceNeeded];
                            System.arraycopy (tt.getTupleByteArray(), 0, arr, headerOffset, tt.getLength());
                            tt = new Tuple(arr, 0, arr.length);

                            try {
                                tt.setHdr((short) 1, colType, _string_sizes);
                            }
                            catch (Exception e) {
                                throw new IndexException(e, "ColumnIndexScan.java: Heapfile error");
                            }

                            boolean eval;
                            try {
                                eval = PredEval.Eval(tempScanObj.scanCond, tt, null, colType, null);
                            }
                            catch (Exception e) {
                                throw new IndexException(e, "ColumnarIndexScan.java: Heapfile error");
                            }

                            if (eval) {
                                // There is no need for projection here so returning the tuple
                                position = tempTID.position;

                                break;
                            } else {
                                tempTID = tempScanObj.getNextSerialized();
                            }
                        }
                    } catch (Exception e) {
                        throw new IndexException(e, "ColumnIndexScan.java: BTreeFile exceptions caught from IndexUtils.BTree_scan().");
                    }

                    break;
                default:
                    throw new UnknownIndexTypeException("Only BTree, BitMap and column scan supported as of now");

            }
            roundRobinInd = (roundRobinInd + 1) % indexTypes.length;
            runCount = (position == null) ? runCount : 0;

            if (position != null && !positions.contains(position)){
                //System.out.println("It does come here as well internal " + tempTID.position + " " + tempTID.numRIDs);
                positions.add(position);
                break;
            }
        }
       return tempTID;
    }

    public void close()
            throws IndexException,
            UnknownIndexTypeException,
            Exception
    {
        for (int i = 0; i < indexTypes.length; i++) {
            switch (indexTypes[i].indexType) {
                case IndexType.BitMapIndex:
                    try {
                        ((BitMapFileScan) indScan[i]).delete_current();
                    } catch (Exception e) {
                        throw new IndexException(e, "BTree error in destroying index scan.");
                    }
                    break;

                case IndexType.B_Index:
                    try {
                        ((BTFileScan) indScan[i]).DestroyBTreeFileScan();
                    } catch (Exception e) {
                        throw new IndexException(e, "BTree error in destroying index scan.");
                    }
                    break;
                case IndexType.None:
                    try {
                        ((SerializedScan) indScan[i]).closeTupleScan();
                    } catch (Exception e) {
                        throw new Exception("Error in closing scan " + e.getMessage());
                    }
                    break;
                default:
                    throw new UnknownIndexTypeException("Only BTree, BitMap and column scan supported as of now");

            }
        }
    }
}
