package global;

import java.util.Arrays;
import java.util.Objects;

/**
 * Tuple ID Class
 */
public class TID extends java.lang.Object {
    public int numRIDs;
    public int position;
    public RID[] recordIDs;

    public TID(int numRIDs) {
        this.numRIDs = numRIDs;
        this.recordIDs = new RID[numRIDs];
        for (int i = 0; i < numRIDs; i++){
            this.recordIDs[i] = new RID();
        }
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
        // start at 0 offset, next offset will be 0 + size of this item
        Convert.setIntValue ( numRIDs, offset, ary);
        Convert.setIntValue ( position, offset+4, ary);// previous: int = 4, +4

        for (RID recordID : recordIDs) {
            Convert.setIntValue(recordID.slotNo, offset+4, ary);
            recordID.pageNo.writeToByteArray(ary, offset+4);// this is just writing an int, so it is size 4
        }

    }

    @Override
    public String toString() {
        return "TID{" +
                "numRIDs=" + numRIDs +
                ", position=" + position +
                ", recordIDs=" + Arrays.toString(recordIDs) +
                '}';
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public void setRID(int column, RID recordId){
        recordIDs[column] = recordId;
    }
}
