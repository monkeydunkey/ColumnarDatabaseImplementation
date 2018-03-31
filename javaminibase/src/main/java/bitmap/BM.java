package bitmap;

import btree.PinPageException;
import diskmgr.Page;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.Tuple;

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
                PageId pageno = directoryRecord.getBmPageId();
                BMPage bmPage = new BMPage(pinPage(pageno));
                System.out.println("bmPage.getSlotCnt(): "+bmPage.getSlotCnt());
                Tuple record = bmPage.getRecord(new RID(pageno, 0));
                boolean[] booleans = BitMapFile.fromBytes(record.getTupleByteArray(), directoryRecord.arraySize);
                System.out.println("Values: "+ Arrays.toString(booleans));
                // record.getTupleByteArray() byte array to boolean array
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private ArrayList<BMHeaderPageDirectoryRecord> getDirectoryRecords(BitMapHeaderPage header) {
        try{
            short slotCnt = header.getSlotCnt();
            System.out.println("header slot count: "+slotCnt);
            RID[] rids = new RID[slotCnt];
            ArrayList<BMHeaderPageDirectoryRecord> bmHeaderPageDirectoryRecords = new ArrayList<>();

            for (int i = 0; i < slotCnt; i++) {
                rids[i] = new RID(header.getPageId(), i);
            }

            for (int i = 0; i < slotCnt; i++) {
                try{
                    Tuple record = header.getRecord(rids[i]);
                    BMHeaderPageDirectoryRecord record1 = new BMHeaderPageDirectoryRecord(record.getTupleByteArray());
                    System.out.println("record1: "+ record1.bmPageId + " : "+record.getTupleByteArray().length);
                    bmHeaderPageDirectoryRecords.add(record1);
                }catch(Exception e){
                    e.printStackTrace();
                }
            }

            System.out.println("bmHeaderPageDirectoryRecords.size(): "+bmHeaderPageDirectoryRecords.size());

            return bmHeaderPageDirectoryRecords;

        }catch (Exception e){
            e.printStackTrace();
        }
        return new ArrayList<>();
    }

    private Page pinPage(PageId pageno)
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
}
