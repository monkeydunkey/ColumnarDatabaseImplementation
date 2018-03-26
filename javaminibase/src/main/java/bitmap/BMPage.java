package  bitmap;

import java.io.*;
import java.lang.*;

import global.*;
import diskmgr.*;
import heap.ConstSlot;
import heap.InvalidSlotNumberException;
import heap.Tuple;

public class BMPage extends Page
        implements ConstSlot, GlobalConst{

    public static final int SIZE_OF_SLOT = 4;
    public static final int DPFIXED = 4 * 2 + 3 * 4;

    public static final int SLOT_CNT = 0;
    public static final int USED_PTR = 2;
    public static final int FREE_SPACE = 4;
    public static final int TYPE = 6;
    public static final int PREV_PAGE = 8;
    public static final int NEXT_PAGE = 12;
    public static final int CUR_PAGE = 16;

    /**
     * page number of this page
     */
    protected PageId curPage = new PageId();
    /**
     * number of slots in use
     */
    private short slotCnt;
    /**
     * offset of first used byte by data records in data[]
     */
    private short usedPtr;
    /**
     * number of bytes free in data[]
     */
    private short freeSpace;
    /**
     * an arbitrary value used by subclasses as needed
     */
    private short type;
    /**
     * backward pointer to data page
     */
    private PageId prevPage = new PageId();
    /**
     * forward pointer to data page
     */
    private PageId nextPage = new PageId();


    public BMPage(){}

    /* Open a BMPage and make this point to the given page */
    public BMPage(Page page){data = page.getpage();}

    /* Returns amount of available space */
    public int available_space()
            throws IOException {
        freeSpace = Convert.getShortValue(FREE_SPACE, data);
        return (freeSpace - SIZE_OF_SLOT);
    }

    /* Dump contents of page */
    public void dumpPage()
            throws IOException {
        int i, n;
        int length, offset;

        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        nextPage.pid = Convert.getIntValue(NEXT_PAGE, data);
        usedPtr = Convert.getShortValue(USED_PTR, data);
        freeSpace = Convert.getShortValue(FREE_SPACE, data);
        slotCnt = Convert.getShortValue(SLOT_CNT, data);

        System.out.println("dumpPage");
        System.out.println("curPage= " + curPage.pid);
        System.out.println("nextPage= " + nextPage.pid);
        System.out.println("usedPtr= " + usedPtr);
        System.out.println("freeSpace= " + freeSpace);
        System.out.println("slotCnt= " + slotCnt);

        for (i = 0, n = DPFIXED; i < slotCnt; n += SIZE_OF_SLOT, i++) {
            length = Convert.getShortValue(n, data);
            offset = Convert.getShortValue(n + 2, data);
            System.out.println("slotNo " + i + " offset= " + offset);
            System.out.println("slotNo " + i + " length= " + length);
        }

    }

    /* See if page is empty */
    public boolean empty()
            throws IOException {
        int i;
        short length;
        // look for an empty slot
        slotCnt = Convert.getShortValue(SLOT_CNT, data);

        for (i = 0; i < slotCnt; i++) {
            length = getSlotLength(i);
            if (length != EMPTY_SLOT)
                return false;
        }

        return true;
    }

    /**
     * @throws IOException I/O errors
     * @param    slotno    slot number
     * @return the length of record the given slot contains
     */
    public short getSlotLength(int slotno)
            throws IOException {
        int position = DPFIXED + slotno * SIZE_OF_SLOT;
        short val = Convert.getShortValue(position, data);
        return val;
    }

    /* Constructor of class BMPage initialize a new page */
    public void init(PageId pageNo, Page apage)
            throws IOException {
        data = apage.getpage();

        slotCnt = 0;                // no slots in use
        Convert.setShortValue(slotCnt, SLOT_CNT, data);

        curPage.pid = pageNo.pid;
        Convert.setIntValue(curPage.pid, CUR_PAGE, data);

        nextPage.pid = prevPage.pid = INVALID_PAGE;
        Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
        Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);

        usedPtr = (short) MAX_SPACE;  // offset in data array (grow backwards)
        Convert.setShortValue(usedPtr, USED_PTR, data);

        freeSpace = (short) (MAX_SPACE - DPFIXED);    // amount of space available
        Convert.setShortValue(freeSpace, FREE_SPACE, data);

    }

    /**
     * @see heap.HFPage#insertRecord(byte[])
     * @param record
     * @return true if successful, false otherwise
     * @throws IOException
     */
    public boolean insertRecord(byte[] record)
            throws IOException {
        int recLen = record.length;
        int spaceNeeded = recLen + SIZE_OF_SLOT;

        // Start by checking if sufficient space exists.
        // This is an upper bound check. May not actually need a slot
        // if we can find an empty one.

        freeSpace = Convert.getShortValue(FREE_SPACE, data);
        if (spaceNeeded > freeSpace) {
            return false;

        } else {

            // look for an empty slot
            slotCnt = Convert.getShortValue(SLOT_CNT, data);
            int i;
            short length;
            for (i = 0; i < slotCnt; i++) {
                length = getSlotLength(i);
                if (length == EMPTY_SLOT)
                    break;
            }

            if (i == slotCnt)   //use a new slot
            {
                // adjust free space
                freeSpace -= spaceNeeded;
                Convert.setShortValue(freeSpace, FREE_SPACE, data);

                slotCnt++;
                Convert.setShortValue(slotCnt, SLOT_CNT, data);

            } else {
                // reusing an existing slot
                freeSpace -= recLen;
                Convert.setShortValue(freeSpace, FREE_SPACE, data);
            }

            usedPtr = Convert.getShortValue(USED_PTR, data);
            usedPtr -= recLen;    // adjust usedPtr
            Convert.setShortValue(usedPtr, USED_PTR, data);

            //insert the slot info onto the data page
            setSlot(i, recLen, usedPtr);

            // insert data onto the data page
            System.arraycopy(record, 0, data, usedPtr, recLen);
            curPage.pid = Convert.getIntValue(CUR_PAGE, data);
            return true;
        }
    }

    /**
     * sets slot contents
     *
     * @param slotno the slot number
     * @param length length of record the slot contains
     * @throws IOException I/O errors
     * @param    offset offset of record
     */
    public void setSlot(int slotno, int length, int offset)
            throws IOException {
        int position = DPFIXED + slotno * SIZE_OF_SLOT;
        Convert.setShortValue((short) length, position, data);
        Convert.setShortValue((short) offset, position + 2, data);
    }

    /* Constructor of class BMPage open a existed BMPage */
    public void openBMpage(Page apage) {
        data = apage.getpage();
    }

    public PageId getCurPage()
            throws IOException {
        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        return curPage;
    }

    public PageId getNextPage()
            throws IOException {
        nextPage.pid = Convert.getIntValue(NEXT_PAGE, data);
        return nextPage;
    }

    public PageId getPrevPage()
            throws IOException {
        prevPage.pid = Convert.getIntValue(PREV_PAGE, data);
        return prevPage;
    }

    public void setCurPage(PageId pageNo)
            throws IOException {
        curPage.pid = pageNo.pid;
        Convert.setIntValue(curPage.pid, CUR_PAGE, data);
    }

    public void setNextPage(PageId pageNo)
            throws IOException {
        nextPage.pid = pageNo.pid;
        Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);
    }

    public void setPrevPage(PageId pageNo)
            throws IOException {
        prevPage.pid = pageNo.pid;
        Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
    }

    public byte[] getBMpageArray() {
        return data;
    }

    /**
     * @return slotCnt used in this page
     * @throws IOException I/O errors
     */
    public short getSlotCnt()
            throws IOException {
        slotCnt = Convert.getShortValue(SLOT_CNT, data);
        return slotCnt;
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
        slotCnt = Convert.getShortValue(SLOT_CNT, data);
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


    public void writeBMPageArray(byte[] data) throws IOException, FileIOException, InvalidPageNumberException {
        SystemDefs.JavabaseDB.write_page(new PageId(getCurPage().pid), new Page(data));
    }

}