package bitmap;

import btree.PinPageException;
import btree.UnpinPageException;
import diskmgr.Page;
import global.*;
import heap.InvalidSlotNumberException;
import heap.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/*
Bit Map File Implementation

// columns value
// rows record


(see BT for analogy):

 */
public class BM {

    public void printBitMap(BitMapHeaderPage header) {
        ArrayList<BMHeaderPageDirectoryRecord> directoryRecords = getDirectoryRecords(header);
        for (int i = 0; i < directoryRecords.size(); i++) {
            BMHeaderPageDirectoryRecord directoryRecord = directoryRecords.get(i);
            System.out.println("BitVector "+String.valueOf(i)+": for value: "+directoryRecord.getValueClass().toString());

            try {
                boolean[] booleans = givenDirectoryPageGetBitMap(directoryRecord);
                System.out.println("Values: "+ Arrays.toString(booleans));
                // record.getTupleByteArray() byte array to boolean array
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static boolean[] givenDirectoryPageGetBitMap(BMHeaderPageDirectoryRecord directoryRecord) throws PinPageException, IOException, InvalidSlotNumberException {
        PageId pageno = directoryRecord.getBmPageId();
        BMPage bmPage = new BMPage(pageno, pinPage(pageno));
        Tuple record = bmPage.getRecord(new RID(pageno, 0));
        boolean[] booleans = BitMapFile.fromBytes(record.getTupleByteArray(), directoryRecord.arraySize);
        return booleans;
    }

    public static ArrayList<BMHeaderPageDirectoryRecord> getDirectoryRecords(BitMapHeaderPage header) {
        try{
            short slotCnt = header.getSlotCnt();// returns 0 after creating the BitMap
            RID[] rids = new RID[slotCnt];
            ArrayList<BMHeaderPageDirectoryRecord> bmHeaderPageDirectoryRecords = new ArrayList<>();

            for (int i = 0; i < slotCnt; i++) {
                rids[i] = new RID(header.getPageId(), i);
            }

            for (int i = 0; i < slotCnt; i++) {
                try{
                    Tuple record = header.getRecord(rids[i]);
                    BMHeaderPageDirectoryRecord record1 = new BMHeaderPageDirectoryRecord(record.getTupleByteArray());
                    bmHeaderPageDirectoryRecords.add(record1);
                }catch(Exception e){
                    e.printStackTrace();
                }
            }

            return bmHeaderPageDirectoryRecords;

        }catch (Exception e){
            e.printStackTrace();
        }
        return new ArrayList<>();
    }

    public static Page pinPage(PageId pageno)
            throws PinPageException {
        try {
            Page page = new Page();
            SystemDefs.JavabaseBM.pinPage(pageno, page, false/*Rdisk*/);
            return page;
        } catch (Exception e) {
            e.printStackTrace();
            throw new PinPageException(e, "");
        }
    }

    public static void unpinPage(PageId pageno, boolean dirty)
            throws UnpinPageException {
        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        } catch (Exception e) {
            e.printStackTrace();
            throw new UnpinPageException(e, "");
        }
    }

    public static BMHeaderPageDirectoryRecord getDirectoryForValue(ValueClass valueClass, BitMapHeaderPage header) {
        ArrayList<BMHeaderPageDirectoryRecord> directoryRecords = getDirectoryRecords(header);
        for (int i = 0; i < directoryRecords.size(); i++) {
            BMHeaderPageDirectoryRecord directoryRecord = directoryRecords.get(i);
            ValueClass directoryRecordValueClass = directoryRecord.getValueClass();
            if(valueClass instanceof ValueStrClass){
                if(directoryRecordValueClass instanceof ValueStrClass){
                    ValueStrClass directoryRecordValueClassValue = (ValueStrClass) directoryRecordValueClass;
                    ValueStrClass valueClassValue = (ValueStrClass) valueClass;
                    if(valueClassValue.equals(directoryRecordValueClassValue)){
                        return directoryRecord;
                    }
                }
            }else if(valueClass instanceof ValueIntClass){

            }else{
                throw new RuntimeException("Unknown Value Class type");
            }
        }
        return null;
    }

}
