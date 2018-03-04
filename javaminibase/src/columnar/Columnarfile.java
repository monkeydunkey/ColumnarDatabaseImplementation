package columnar;

import java.io.*;
import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;

public class Columnarfile {
    static int numColumns;
    AttrType[] type;

    public Columnarfile(int totalColumns, AttrType[] attrType){
        numColumns = totalColumns;
        type = attrType;

        /*
        The way the initialization should work is that for each of the of the
        columns we will have a separate heap file and we will use that heap
        file to manage data insertion, deletion and updates.

        The meta data file will also be stored in terms of a heap file. As the
        heap file structure is that of a directory. We do have to figure out
        how to store and add stuff. Maybe using the DataPageInfo object in package heap
        or creating something similar will be useful
         */

        

    }

    public void deleteColumnarFile(){
        throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }

    public TID insertTuple(byte[] tuplePtr){
        throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }

    public ValueClass insertTuple(byte[] tuplePtr){
        throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }

    public int getTupleCnt(){
        throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }

    public TupleScan openTupleScan(){
        throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }

    public Scan openColumnScan(int columnNo){
        throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }

    public boolean updateTuple(TID tid, Tuple newtuple){
        throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }

    public boolean updateColumnofTuple(TID tid, Tuple newtuple, int column){
        throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }

    public boolean createBTreeIndex(int column){
        throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }

    public boolean createBitMapIndex(int columnNo, valueClass value){
        throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }

    public boolean markTupleDeleted(TID tid){
        throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }

    public boolean purgeAllDeletedTuples(){
        throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }
}