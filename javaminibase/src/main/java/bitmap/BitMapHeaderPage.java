package bitmap;


import btree.ConstructPageException;
import diskmgr.Page;
import global.Convert;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.InvalidSlotNumberException;
import heap.Tuple;

import java.io.IOException;

public class BitMapHeaderPage extends BMPage{

    private PageId _rootId;

    /**
     * @see btree.BTreeHeaderPage#BTreeHeaderPage()
     * @throws ConstructPageException
     */
    public BitMapHeaderPage()
            throws ConstructPageException {
        super();
        try {
            Page apage = new Page();
            PageId pageId = SystemDefs.JavabaseBM.newPage(apage, 1);
            if (pageId == null)
                throw new ConstructPageException(null, "new page failed");
            this.init(pageId, apage);

        } catch (Exception e) {
            throw new ConstructPageException(e, "construct header page failed");
        }
    }

    /**
     * @see btree.BTreeHeaderPage#BTreeHeaderPage(PageId pageno)
     * @param pageno
     * @throws ConstructPageException
     */
    public BitMapHeaderPage(PageId pageno) throws ConstructPageException {
        super();
        try {
            SystemDefs.JavabaseBM.pinPage(pageno, this, false/*Rdisk*/);
        } catch (Exception e) {
            throw new ConstructPageException(e, "pinpage failed");
        }
    }

    /**
     * @see btree.BTreeHeaderPage#set_rootId(PageId rootId)
     * @param rootId
     * @throws IOException
     */
    public void set_rootId(PageId rootId) throws IOException {
        setNextPage(rootId);
    }

    /**
     * @see btree.BTreeHeaderPage#getPageId()
     * @return
     * @throws IOException
     */
    PageId getPageId()
            throws IOException {
        return getCurPage();
    }

    /**
     * copies out record with RID rid into record pointer.
     * <br>
     * Status getRecord(RID rid, char *recPtr, int& recLen)
     *
     * @return a tuple contains the record
     * @throws InvalidSlotNumberException Invalid slot number
     * @throws IOException                I/O errors
     * @param    rid the record ID
     * @see Tuple
     */
    public Tuple getRecord(RID rid)
            throws IOException,
            InvalidSlotNumberException {
        short recLen;
        short offset;
        byte[] record;
        PageId pageNo = new PageId();
        pageNo.pid = rid.pageNo.pid;
        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        int slotNo = rid.slotNo;

        // length of record being returned
        recLen = getSlotLength(slotNo);
        int slotCnt = Convert.getShortValue(SLOT_CNT, data);
        if ((slotNo >= 0) && (slotNo < slotCnt) && (recLen > 0)
                && (pageNo.pid == curPage.pid)) {
            offset = getSlotOffset(slotNo);
            record = new byte[recLen];
            System.arraycopy(data, offset, record, 0, recLen);
            Tuple tuple = new Tuple(record, 0, recLen);
            return tuple;
        } else {
            throw new InvalidSlotNumberException(null, "HEAPFILE: INVALID_SLOTNO");
        }


    }

    /**
     * @param slotno slot number
     * @return the offset of record the given slot contains
     * @throws IOException I/O errors
     */
    public short getSlotOffset(int slotno)
            throws IOException {
        int position = DPFIXED + slotno * SIZE_OF_SLOT;
        short val = Convert.getShortValue(position + 2, data);
        return val;
    }
}
