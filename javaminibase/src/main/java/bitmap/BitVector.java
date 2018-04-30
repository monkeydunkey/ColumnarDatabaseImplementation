package bitmap;

import global.Convert;
import global.RID;
import heap.InvalidSlotNumberException;
import heap.Tuple;

import java.io.IOException;

public class BitVector {
    boolean allBoolsAreFalse;
    int arraySize;
    boolean[] booleans;

    /**
     *      *         //bmPage.getRecord(new RID(pageno, 0)); == true/false flag is the entire byte array false? size 0
     *      *         //record = bmPage.getRecord(new RID(pageno, 1)); == size of the array on this page
     *      *         //record = bmPage.getRecord(new RID(pageno, 2)); == the array of bytes
     * @param bmPage
     * @throws IOException
     * @throws InvalidSlotNumberException
     */
    public BitVector(BMPage bmPage) throws IOException, InvalidSlotNumberException {
        parseAllBoolsAreFalse(bmPage);
        parseArraySize(bmPage);
        byte[] tupleByteArray = bmPage.getRecord(new RID(bmPage.getCurPage(), 2)).getTupleByteArray();
        if(allBoolsAreFalse){
            booleans = new boolean[arraySize];
        }else{

        }
        boolean[] booleans = BitMapFile.fromBytes(record.getTupleByteArray(), ar);
    }

    private void parseArraySize(BMPage bmPage) throws IOException, InvalidSlotNumberException {
        arraySize = Convert.getIntValue(0,bmPage.getRecord(new RID(bmPage.getCurPage(), 1)).getTupleByteArray());
    }

    private void parseAllBoolsAreFalse(BMPage bmPage) throws IOException, InvalidSlotNumberException {
        byte[] bytes = bmPage.getRecord(new RID(bmPage.getCurPage(), 0)).getTupleByteArray();
        int allBoolsAreFalseInt = bytes[0];
        allBoolsAreFalse = allBoolsAreFalseInt == 1;
    }

    public boolean isAllBoolsAreFalse() {
        return allBoolsAreFalse;
    }

    public int getArraySize() {
        return arraySize;
    }

    public boolean[] getBooleans() {
        return booleans;
    }
}
