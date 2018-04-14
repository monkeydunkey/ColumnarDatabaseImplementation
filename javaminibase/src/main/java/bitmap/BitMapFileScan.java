package bitmap;

import btree.*;
import columnar.Columnarfile;
import global.RID;
import global.ValueIntClass;
import global.ValueStrClass;
import heap.InvalidSlotNumberException;
import heap.Tuple;

import java.io.IOException;

public class BitMapFileScan extends IndexFileScan {

    private final BMHeaderPageDirectoryRecord directoryForValue;
    private final boolean[] booleans;
    private final Columnarfile columnarFile;
    private int currentPosition = 0;


    public BitMapFileScan(BMHeaderPageDirectoryRecord directoryForValue, Columnarfile columnarfile) throws InvalidSlotNumberException, IOException, PinPageException {
        this.directoryForValue = directoryForValue;
        boolean[] booleans = BM.givenDirectoryPageGetBitMap(directoryForValue);
        this.booleans = booleans;
        this.columnarFile = columnarfile;
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
                System.out.println("position matches bitmap index and condition! "+currentPosition);
                currentPosition++;

                KeyClass keyClass;

                if(directoryForValue.getValueClass() instanceof ValueStrClass){
                   keyClass = new StringKey(((ValueStrClass) directoryForValue.getValueClass()).value);
                }else if(directoryForValue.getValueClass() instanceof ValueIntClass){
                   keyClass = new IntegerKey(((ValueIntClass) directoryForValue.getValueClass()).value);
                }

//                new RID()// todo: need page number and slot number by position

                //columnarFile.getRIDByPosition(currentPosition);
//                KeyDataEntry keyDataEntry = new KeyDataEntry(keyClass, );


                return null; //return tuple for this position
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
