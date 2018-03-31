package iterator;


import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import columnar.*;

import java.lang.*;
import java.io.*;

public class ColumnarFileScan extends Iterator{
    private AttrType[] _in1;
    private short in1_len;
    private short[] s_sizes;
    private Columnarfile f;
    private TupleScan scan;
    private Tuple     tuple1;
    private Tuple    Jtuple;
    private int        t1_size;
    private int nOutFlds;
    private CondExpr[]  OutputFilter;
    public FldSpec[] perm_mat;



    /**
     *constructor
     *@param file_name heapfile to be opened
     *@param in1[]  array showing what the attributes of the input fields are.
     *@param s1_sizes[]  shows the length of the string fields.
     *@param len_in1  number of attributes in the input tuple
     *@param n_out_flds  number of fields in the out tuple
     *@param proj_list  shows what input fields go where in the output tuple
     *@param outFilter  select expressions
     *@exception IOException some I/O fault
     *@exception FileScanException exception from this class
     *@exception TupleUtilsException exception from this class
     *@exception InvalidRelation invalid relation
     */
    public  ColumnarFileScan(java.lang.String file_name,
                             AttrType[] in1,
                             short[] s1_sizes,
                             short len_in1,
                             int n_out_flds,
                             FldSpec[] proj_list,
                             CondExpr[] outFilter)
            throws IOException,
            FileScanException,
            TupleUtilsException,
            InvalidRelation
    {
        _in1 = in1;
        in1_len = len_in1;
        s_sizes = s1_sizes;

        Jtuple =  new Tuple();
        AttrType[] Jtypes = new AttrType[n_out_flds];
        short[]    ts_size;
        ts_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes, in1, len_in1, s1_sizes, proj_list, n_out_flds);

        OutputFilter = outFilter;
        perm_mat = proj_list;
        nOutFlds = n_out_flds;
        tuple1 =  new Tuple();

        try {
            tuple1.setHdr(in1_len, _in1, s1_sizes);
        }catch (Exception e){
            throw new FileScanException(e, "setHdr() failed");
        }
        t1_size = tuple1.size();

        try {
            f = new Columnarfile(file_name);
            if (f == null){
                System.out.println("Columnar File is null");
            }
        }
        catch(Exception e) {
            throw new FileScanException(e, "Opening Columnar File failed");
        }

        try {
            scan = new TupleScan(f);
        }
        catch(Exception e){
            throw new FileScanException(e, "Creating tuple scan failed");
        }
    }

    /**
     *@return shows what input fields go where in the output tuple
     */
    public FldSpec[] show()
    {
        return perm_mat;
    }

    /**
     *@return the result tuple
     *@exception JoinsException some join exception
     *@exception IOException I/O errors
     *@exception InvalidTupleSizeException invalid tuple size
     *@exception InvalidTypeException tuple type not valid
     *@exception PageNotReadException exception from lower layer
     *@exception PredEvalException exception from PredEval class
     *@exception UnknowAttrType attribute type unknown
     *@exception FieldNumberOutOfBoundException array out of bounds
     *@exception WrongPermat exception for wrong FldSpec argument
     */
    public Tuple get_next()
            throws JoinsException,
            IOException,
            InvalidTupleSizeException,
            InvalidTypeException,
            PageNotReadException,
            PredEvalException,
            UnknowAttrType,
            FieldNumberOutOfBoundException,
            WrongPermat
    {
        TID tid = new TID(f.numColumns);

        while(true) {
            if((tuple1 =  scan.getNext(tid)) == null) {
                return null;
            }
            int TotalSpaceNeeded = (in1_len + 2) * 2 + tuple1.getLength();
            int headerOffset = (in1_len + 2) * 2;
            byte[] arr = new byte[TotalSpaceNeeded];
            System.arraycopy (tuple1.getTupleByteArray(), 0, arr, headerOffset, tuple1.getLength());
            tuple1 = new Tuple(arr, 0, arr.length);
            tuple1.setHdr(in1_len, _in1, s_sizes);
            if (PredEval.Eval(OutputFilter, tuple1, null, _in1, null) == true){
                Projection.Project(tuple1, _in1,  Jtuple, perm_mat, nOutFlds);
                return  Jtuple;
            }
        }
    }

    public Tuple get_next(boolean delFlag)
            throws JoinsException,
            IOException,
            InvalidTupleSizeException,
            InvalidTypeException,
            PageNotReadException,
            PredEvalException,
            UnknowAttrType,
            FieldNumberOutOfBoundException,
            WrongPermat
    {
        TID tid = new TID(f.numColumns + 2);
        //System.out.println("numColumns: "+f.numColumns);
        try{
            while(true) {
                if((tuple1 =  scan.getNextInternal(tid)) == null) {
                    return null;
                }
                int TotalSpaceNeeded = (in1_len + 4) * 2 + tuple1.getLength();
                int headerOffset = (in1_len + 4) * 2;
                byte[] arr = new byte[TotalSpaceNeeded];
                System.arraycopy (tuple1.getTupleByteArray(), 0, arr, headerOffset, tuple1.getLength());
                tuple1 = new Tuple(arr, 0, arr.length);

                AttrType[] tempAttrArr = new AttrType[in1_len + 2];
                for (int i = 0; i < in1_len + 2; i++){
                    tempAttrArr[i] = (i < in1_len) ? _in1[i] : new AttrType(1);
                }

                tuple1.setHdr((short)(in1_len + 2), tempAttrArr, s_sizes);
                if (PredEval.Eval(OutputFilter, tuple1, null, _in1, null) == true){
                    //removing as projecting here is useless
                    //Projection.Project(tuple1, _in1,  Jtuple, perm_mat, nOutFlds);
                    if (delFlag){
                        if (!f.markTupleDeleted(tid)){
                            return null;
                        }
                    }
                    return  Jtuple;
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return Jtuple;
    }

    /**
     *implement the abstract method close() from super class Iterator
     *to finish cleaning up
     */
    public void close()
    {

        if (!closeFlag) {
            scan.closeTupleScan();
            closeFlag = true;
        }
    }

}
