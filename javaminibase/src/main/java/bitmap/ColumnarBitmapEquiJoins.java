package bitmap;

import columnar.Columnarfile;
import global.AttrType;
import heap.*;
import iterator.FldSpec;

import java.io.IOException;

public class ColumnarBitmapEquiJoins {


    /**
     *
     * After passing all the appropriate fields, calling getNext() will give you the next joined tuple
     * @param in1 attribute types of the first (outer) columnar file
     * @param len_in1 number of columns of the first (outer) columnar file
     * @param t1_str_sizes list of string sizes of the first (outer) columnar file
     * @param in2 attribute types of the second (inner) columnar file
     * @param len_in2 number of columns of the second (inner) columnar file
     * @param t2_str_sizes list of string sizes of the second (inner) columnar file
     * @param amt_of_mem amount of memory from BitMapJoin Input
     * @param leftColumnarFileName first (outer) columnar file
     * @param leftJoinField index of the field to join
     * @param rightColumnarFileName second (inner) columnar file
     * @param rightJoinField index of the field to join
     * @param proj_list fields to display on the final output
     * @param n_out_flds number of fields to display on the final output
     */
    public ColumnarBitmapEquiJoins(
            AttrType[] in1,
            int len_in1,
            short[] t1_str_sizes,
            AttrType[] in2,
            int len_in2,
            short[] t2_str_sizes,
            int amt_of_mem,
            String leftColumnarFileName,
            int leftJoinField,
            String rightColumnarFileName,
            int rightJoinField,
            FldSpec[] proj_list,
            int n_out_flds) throws HFDiskMgrException, InvalidTupleSizeException, HFException, IOException, InvalidSlotNumberException, SpaceNotAvailableException, HFBufMgrException {

        Columnarfile outerColumnarfile = new Columnarfile(leftColumnarFileName);
        Columnarfile innerColumnarfile = new Columnarfile(rightColumnarFileName);

    }

    public Tuple get_next() {
        return null;//todo
    }

    public void close() {
        //todo
    }
}
