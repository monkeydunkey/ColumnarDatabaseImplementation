package iterator;

import bufmgr.PageNotReadException;
import columnar.Columnarfile;
import global.AttrType;
import global.*;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.*;
import index.IndexException;

import java.io.*;
import java.lang.*;

/**
 *
 *  This file contains an implementation of the nested loops join
 *  algorithm as described in the Shapiro paper.
 *  The algorithm is extremely simple:
 *
 *      foreach tuple r in R do
 *          foreach tuple s in S do
 *              if (ri == sj) then add (r, s) to the result.
 */

public class ColumnarNestedLoopJoins extends Iterator {
    // Class Members
    private AttrType        _in1[], _in2[];
    private   int           in1_len, in2_len;
    private   Iterator      outer;
    private   short         t2_str_sizescopy[];
    private   CondExpr      OutputFilter[];
    private   CondExpr      RightFilter[];
    private   int           n_buf_pgs;          // # of buffer pages available.
    private   boolean       done;               // Is the join complete
    private   boolean       get_from_outer;     // if TRUE, a tuple is got from outer
    private   Tuple         outer_tuple, inner_tuple;
    private   Tuple         Jtuple;             // Joined tuple
    private   FldSpec       perm_mat[];
    private   int           nOutFlds;
    private   Columnarfile  cf;
    private   ColumnarFileScan     inner;
    private   String        relName;
    private   FldSpec[]     inner_rel_proj_list;

    /** Constructor
     * Initialize the two relations which are joined, including relation type,
     * @param in1               Array containing field types of R
     * @param len_in1           # of columns in R
     * @param t1_str_sizes      shows the length of the string fields
     * @param in2               Array containing field types of S
     * @param len_in2           # of columns in S
     * @param t2_str_sizes      shows the length of the string fields.
     * @param amt_of_mem        IN PAGES
     * @param am1               access method for left i/p to join
     * @param columnarFileName  access columnarfile for right i/p to join
     * @param outFilter         select expressions
     * @param rightFilter       reference to filter applied on right i/p
     * @param proj_list         shows what input fields go where in the output tuple
     * @param n_out_flds        number of outer relation fields
     * @exception IOException   some I/O fault
     * @exception NestedLoopException exception from this class
     */
    public ColumnarNestedLoopJoins(
            AttrType[] in1,
            int len_in1,
            short[] t1_str_sizes,
            AttrType[] in2,
            int len_in2,
            short[] t2_str_sizes,
            int amt_of_mem,
            Iterator am1,
            String columnarFileName,
            CondExpr[] outFilter,
            CondExpr[] rightFilter,
            FldSpec[] proj_list,
            int n_out_flds ) throws NestedLoopException
    {
        _in1 = new AttrType[in1.length];
        _in2 = new AttrType[in2.length];
        System.arraycopy(in1,0,_in1,0,in1.length);
        System.arraycopy(in2,0,_in2,0,in2.length);
        in1_len = len_in1;
        in2_len = len_in2;

        outer = am1;
        t2_str_sizescopy =  t2_str_sizes;
        inner_tuple = new Tuple();
        Jtuple = new Tuple();
        OutputFilter = outFilter;
        RightFilter  = rightFilter;

        n_buf_pgs    = amt_of_mem;
        inner = null;
        done  = false;
        get_from_outer = true;

        AttrType[] Jtypes = new AttrType[n_out_flds];
        short[]    t_size;

        perm_mat = proj_list;
        nOutFlds = n_out_flds;
        try {
            t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
                    in1, len_in1, in2, len_in2,
                    t1_str_sizes, t2_str_sizes,
                    proj_list, nOutFlds);
        }
        catch(Exception e) {
            throw new NestedLoopException(e,"TupleUtilsException is caught by NestedLoopsJoins.java");
        }
        this.relName = columnarFileName;
        inner_rel_proj_list = new FldSpec[len_in2];
        for (int i = 0; i < len_in2; i++){
            inner_rel_proj_list[i] = new FldSpec(new RelSpec(RelSpec.outer), i);
        }
        try {
            cf = new Columnarfile(columnarFileName);
        }
        catch(Exception e) {
            throw new NestedLoopException(e, "Create new columnarfile failed.");
        }
    }

    /**
     *@return The joined tuple is returned
     *@exception IOException I/O errors
     *@exception JoinsException some join exception
     *@exception IndexException exception from super class
     *@exception InvalidTupleSizeException invalid tuple size
     *@exception InvalidTypeException tuple type not valid
     *@exception PageNotReadException exception from lower layer
     *@exception TupleUtilsException exception from using tuple utilities
     *@exception PredEvalException exception from PredEval class
     *@exception SortException sort exception
     *@exception LowMemException memory error
     *@exception UnknowAttrType attribute type unknown
     *@exception UnknownKeyTypeException key type unknown
     *@exception Exception other exceptions
     */
    public Tuple get_next()
            throws IOException,
            JoinsException ,
            IndexException,
            InvalidTupleSizeException,
            InvalidTypeException,
            PageNotReadException,
            TupleUtilsException,
            PredEvalException,
            SortException,
            LowMemException,
            UnknowAttrType,
            UnknownKeyTypeException,
            Exception
    {
        // This is a DUMBEST form of a join, not making use of any key information...

        if(done)
            return null;

        do
        {
            // If get_from_outer is true, get a tuple from the outer, delete
            // an existing scan on the file, and reopen a new scan on the file
            // If a get_next on the outer returns DONE?, then the nested loops
            // join is done too

            if( get_from_outer )
            {
                get_from_outer = false;
                if( inner != null )     // If this not the first time,
                {
                    // close scan
                    inner = null;
                }

                try {
                    inner = new ColumnarFileScan(relName, _in2, t2_str_sizescopy, (short)in2_len, in2_len, inner_rel_proj_list,RightFilter);  //todo change this later!
                }
                catch(Exception e) {
                    throw new NestedLoopException(e, "openScan failed");
                }

                if( (outer_tuple = outer.get_next()) == null )
                {
                    done = true;
                    if( inner != null )
                    {
                        inner = null;
                    }

                    return null;
                }
            }  // ENDS: if (get_from_outer == TRUE)

            // The next step is to get a tuple from the inner,
            // while the inner is not completely scanned && there
            // is no match (with pred),get a tuple from the inner

            TID tid = new TID(in2_len);
            while( (inner_tuple = inner.get_next() ) != null )
            {
               if( PredEval.Eval(OutputFilter, outer_tuple, inner_tuple, _in1, _in2) )
                    {
                        // Apply a projection on the outer and inner tuples
                        Projection.Join(outer_tuple, _in1,
                                inner_tuple, _in2,
                                Jtuple, perm_mat, nOutFlds);
                        return Jtuple;
                    }
            }


            // There has been no match. (otherwise, we would have
            //returned from t//he while loop. Hence, inner is
            //exhausted, => set get_from_outer = TRUE, go to top of loop

            get_from_outer = true; // Loop back to top and get next outer tuple

        } while(true);
    }

    /**
     * implement the abstract method close() from super class Iterator
     *to finish cleaning up
     *@exception IOException I/O error from lower layers
     *@exception JoinsException join error from lower layers
     *@exception IndexException index access error
     */
    public void close() throws JoinsException, IOException, IndexException
    {
        if( !closeFlag )
        {
            try {
                outer.close();
            }
            catch(Exception e) {
                throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
            }
            closeFlag = true;
        }
    }
}
