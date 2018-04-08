package bitmap;

import btree.*;
import heap.InvalidSlotNumberException;
import heap.Tuple;

import java.io.IOException;

public class BitMapFileScan extends IndexFileScan {

    private final BMHeaderPageDirectoryRecord directoryForValue;
    private final boolean[] booleans;
    private int currentPosition = 0;


    public BitMapFileScan(BMHeaderPageDirectoryRecord directoryForValue) throws InvalidSlotNumberException, IOException, PinPageException {
        this.directoryForValue = directoryForValue;
        boolean[] booleans = BM.givenDirectoryPageGetBitMap(directoryForValue);
        this.booleans = booleans;
    }

    @Override
    public KeyDataEntry get_next() throws ScanIteratorException {
        //key == equals to value
        // data == RID of that entry
        // @see ColumnIndexScan.getNext()
        // how to get RID from position?

        // find next value that matches boolean == 1
        Tuple tuple = null;
        while(tuple == null){
            if(booleans[currentPosition]){
                //return tuple for this position
                System.out.println("position matches bitmap index and condition! "+currentPosition);
                currentPosition++;
                return null;
            }else{
                currentPosition++;
            }
            if(currentPosition>=booleans.length){
                break;// none found
            }
        }

        return null;
    }

    @Override
    public void delete_current() throws ScanDeleteException {

    }

    @Override
    public int keysize() {
        return 0;
    }
}
