package index;
import columnar.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import btree.*;
import iterator.*;
import heap.*;
import java.io.*;
import bitmap.*;

/*
* This class works on a single column and tells us the
* rows that are satisified on the given condition.
* */

public class ColumnIndexScan extends Iterator {

    /**
     * class constructor. set up the index scan.
     * @param index type of the index (B_Index, BitMap)
     * @param relName name of the input relation
     * @param indName name of the input index
     * @param types array of types in this relation
     * @param str_sizes array of string sizes (for attributes that are string)
     * @param selects conditions to apply, first one is primary
     * @param indexOnly whether the answer requires only the key or the tuple
     * @exception IndexException error from the lower layer
     * @exception InvalidTypeException tuple type not valid
     * @exception InvalidTupleSizeException tuple size not valid
     * @exception UnknownIndexTypeException index type unknown
     * @exception IOException from the lower layer
     */
    public ColumnIndexScan(IndexType index,
                           java.lang.String relName,
                           java.lang.String indName,
                           AttrType type,
                           short str_sizes,
                           CondExpr[] selects,
                           boolean indexOnly)

            throws IndexException,
            InvalidTypeException,
            InvalidTupleSizeException,
            UnknownIndexTypeException,
            IOException
    {
        //_fldNum = fldNum;
        //_noInFlds = noInFlds;
        //_types = types;
        _s_sizes = str_sizes;

        _types = new AttrType[1];
        _types[0] = type;
        //AttrType[] Jtypes = new AttrType[noOutFlds];
        short[] ts_sizes;
        Jtuple = new Tuple();

        /*
        try {
            ts_sizes = TupleUtils.setup_op_tuple(Jtuple, Jtypes, types, noInFlds, str_sizes, outFlds, noOutFlds);
        }
        catch (TupleUtilsException e) {
            throw new IndexException(e, "ColumnIndexScan.java: TupleUtilsException caught from TupleUtils.setup_op_tuple()");
        }
        catch (InvalidRelation e) {
            throw new IndexException(e, "ColumnIndexScan.java: InvalidRelation caught from TupleUtils.setup_op_tuple()");
        }*/

        _selects = selects;
        //perm_mat = outFlds;
        //_noOutFlds = noOutFlds;
        tuple1 = new Tuple();
        /*try {
            tuple1.setHdr((short) str_sizes);
        }
        catch (Exception e) {
            throw new IndexException(e, "ColumnIndexScan.java: Heapfile error");
        }*/

        //t1_size = tuple1.size();
        index_only = indexOnly;  // added by bingjie miao

        try {
            f = new Columnarfile(relName);
        }
        catch (Exception e) {
            throw new IndexException(e, "ColumnIndexScan.java: Heapfile not created");
        }
        //Getting all the string lengths
        int totalStrings = 0;
        for (int i = 0; i < f.numColumns; i++){
            totalStrings += (f.type[i].attrType == AttrType.attrString) ? f.offsets[i]: 0;
        }
        _string_sizes = new short[totalStrings];
        int _string_count = 0;
        for (int i = 0; i < f.numColumns; i++){
            if (f.type[i].attrType == AttrType.attrString){
                _string_sizes[_string_count] = (short)f.offsets[i];
                _string_count += 1;
            }
        }
        //Calculated all the string lengths

        switch(index.indexType) {
            case IndexType.BitMapIndex:
                try{
                    indFile = new BTreeFile(indName);
                }
                catch (Exception e) {
                    throw new IndexException(e, "ColumnIndexScan.java: BitMap exceptions caught from BitMap constructor");
                }

                try {
                    indScan = (BTFileScan) IndexUtils.BTree_scan(selects, indFile);
                }
                catch (Exception e) {
                    throw new IndexException(e, "ColumnIndexScan.java: BTreeFile exceptions caught from IndexUtils.BTree_scan().");
                }
                break;

            case IndexType.B_Index:
                // error check the select condition
                // must be of the type: value op symbol || symbol op value
                // but not symbol op symbol || value op value
                try {
                    indFile = new BTreeFile(indName);
                }
                catch (Exception e) {
                    throw new IndexException(e, "ColumnIndexScan.java: BTreeFile exceptions caught from BTreeFile constructor");
                }

                try {
                    indScan = (BTFileScan) IndexUtils.BTree_scan(selects, indFile);
                }
                catch (Exception e) {
                    throw new IndexException(e, "ColumnIndexScan.java: BTreeFile exceptions caught from IndexUtils.BTree_scan().");
                }

                break;
            case IndexType.None:
            default:
                throw new UnknownIndexTypeException("Only BTree index and BitMap supported as of now");

        }

    }

    /**
     * returns the next tuple.
     * if <code>index_only</code>, only returns the key value
     * (as the first field in a tuple)
     * otherwise, retrive the tuple and returns the whole tuple
     * @return the tuple
     * @exception IndexException error from the lower layer
     * @exception UnknownKeyTypeException key type unknown
     * @exception IOException from the lower layer
     */
    public Tuple get_next()
            throws IndexException,
            UnknownKeyTypeException,
            IOException
    {
        RID rid;
        int unused;
        KeyDataEntry nextentry = null;

        try {
            nextentry = indScan.get_next();
        }
        catch (Exception e) {
            throw new IndexException(e, "ColumnIndexScan.java: BTree error");
        }

        while(nextentry != null) {
            // not index_only, need to return the whole tuple
            rid = ((LeafData)nextentry.data).getData();
            try {
                //TODO: skip the ones which are marked for deletion
                TID tid = f.deserializeTuple(f.columnFile[f.numColumns + 1].getRecord(rid).getTupleByteArray());
                tuple1 = f.getTuple(tid);
                //As we have to pass it to eval we need to add space for header information
                int TotalSpaceNeeded = (f.numColumns + 2) * 2 + tuple1.getLength();
                int headerOffset = (f.numColumns + 2) * 2;
                byte[] arr = new byte[TotalSpaceNeeded];
                System.arraycopy (tuple1.getTupleByteArray(), 0, arr, headerOffset, tuple1.getLength());
                tuple1 = new Tuple(arr, 0, arr.length);
            }
            catch (Exception e) {
                throw new IndexException(e, "ColumnIndexScan.java: getRecord failed");
            }

            try {
                tuple1.setHdr((short) f.numColumns, f.type, _string_sizes);
            }
            catch (Exception e) {
                throw new IndexException(e, "ColumnIndexScan.java: Heapfile error");
            }

            boolean eval;
            try {
                eval = PredEval.Eval(_selects, tuple1, null, f.type, null);
            }
            catch (Exception e) {
                throw new IndexException(e, "ColumnIndexScan.java: Heapfile error");
            }

            if (eval) {
                // There is no need for projection here so returning the tuple
                return tuple1;
            }

            try {
                nextentry = indScan.get_next();
            }
            catch (Exception e) {
                throw new IndexException(e, "ColumnIndexScan.java: BTree error");
            }
        }

        return null;
    }

    /**
     * Cleaning up the index scan, does not remove either the original
     * relation or the index from the database.
     * @exception IndexException error from the lower layer
     * @exception IOException from the lower layer
     */
    public void close() throws IOException, IndexException
    {
        if (!closeFlag) {
            if (indScan instanceof BTFileScan) {
                try {
                    ((BTFileScan)indScan).DestroyBTreeFileScan();
                }
                catch(Exception e) {
                    throw new IndexException(e, "BTree error in destroying index scan.");
                }
            }

            closeFlag = true;
        }
    }

    public FldSpec[]      perm_mat;
    private IndexFile     indFile;
    private IndexFileScan indScan;
    private AttrType[]    _types;
    private short        _s_sizes;
    private CondExpr[]    _selects;
    private int           _noInFlds;
    private int           _noOutFlds;
    private Columnarfile   f;
    private Tuple         tuple1;
    private Tuple         Jtuple;
    private int           t1_size;
    private int           _fldNum;
    private boolean       index_only;
    private short[]       _string_sizes;

}
