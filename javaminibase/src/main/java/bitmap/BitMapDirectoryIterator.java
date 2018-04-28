package bitmap;

import btree.ConstructPageException;
import btree.UnpinPageException;
import diskmgr.Page;
import global.PageId;
import global.RID;
import heap.InvalidSlotNumberException;
import heap.Tuple;

import java.io.IOException;
import java.util.ArrayList;

public class BitMapDirectoryIterator {

    private BitMapHeaderPage header;
    int currentSlot = -1; // start at negative one so when we increment at the beginging the first value will be 0

    public BitMapDirectoryIterator(PageId bitMapHeaderPageId) throws ConstructPageException {
        this.header = new BitMapHeaderPage(bitMapHeaderPageId);
    }

    public BMHeaderPageDirectoryRecord getNext() throws IOException, InvalidSlotNumberException, UnpinPageException, ConstructPageException {
        currentSlot++;
        BMHeaderPageDirectoryRecord record1 = null;
        if(currentSlot < header.getSlotCnt()) {
            return getRecord();
        }else{
            PageId nextPage = header.getNextPage(); // get next page
            BM.unpinPage(header.getPageId(), false);// release the current page
            header = new BitMapHeaderPage(nextPage);// this will internally pin the page.
            currentSlot = 0; // reset the current slot to 0 now that we are on a new page.
            return getRecord();
        }
    }

    private BMHeaderPageDirectoryRecord getRecord() throws IOException, InvalidSlotNumberException {
        Tuple record = header.getRecord(new RID(header.getPageId(), currentSlot));
        BMHeaderPageDirectoryRecord record1 = new BMHeaderPageDirectoryRecord(record.getTupleByteArray());
        return record1;
    }

    public void close() throws IOException, UnpinPageException {
        BM.unpinPage(header.getPageId(), false);// release the current page
    }
}
