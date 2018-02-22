package global;

import java.util.Arrays;
import java.util.Objects;

public class TID extends java.lang.Object {
    int numRIDs;
    int position;
    RID[] recordIDs;

    public TID(int numRIDs) {
        this.numRIDs = numRIDs;
    }

    public TID(int numRIDs, int position) {
        this.numRIDs = numRIDs;
        this.position = position;
    }

    public TID(int numRIDs, int position, RID[] recordIDs) {
        this.numRIDs = numRIDs;
        this.position = position;
        this.recordIDs = recordIDs;
    }

    void copyTid(TID tid){
        this.numRIDs = tid.numRIDs;
        this.position = tid.position;
        this.recordIDs = tid.recordIDs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TID tid = (TID) o;
        return numRIDs == tid.numRIDs &&
                position == tid.position &&
                Arrays.equals(recordIDs, tid.recordIDs);
    }

    /** Write the rid into a byte array at offset
     * @param ary the specified byte array
     * @param offset the offset of byte array to write
     * @exception java.io.IOException I/O errors
     */
    public void writeToByteArray(byte [] ary, int offset) throws java.io.IOException {
        Convert.setIntValue ( numRIDs, offset, ary);
        Convert.setIntValue ( position, offset+4, ary);

        //todo add each rid to the byte array
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public void setRID(int column, RID recordId){
        //todo set the RID of the given column
    }
}
