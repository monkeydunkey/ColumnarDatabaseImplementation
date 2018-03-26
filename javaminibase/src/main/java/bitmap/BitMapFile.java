package bitmap;

import btree.*;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import columnar.Columnarfile;
import diskmgr.Page;
import global.*;
import heap.HFBufMgrException;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Create a class called BitMapFile with the following specifications (see BTreeFile for analogy):
 */
public class BitMapFile implements GlobalConst{

    private String dbname;
    private BitMapHeaderPage headerPage;
    private PageId headerPageId;
    private BMPage cursorBMPage;
    private LinkedList<Boolean> cursorBuffer;

    /**
     * BitMapFile class; an index file with given filename should already exist, then this opens it.
     * @param filename the Bit Map File tree file name. Input parameter.
     * @throws GetFileEntryException
     */
    public BitMapFile(java.lang.String filename) throws GetFileEntryException, ConstructPageException {
        headerPageId=get_file_entry(filename);
        headerPage= new BitMapHeaderPage(headerPageId);
        dbname = filename;
    }

    /**
     * BitMapFile class; an index file with given filename should not already exist; this creates the BitMap file
     * from scratch.
     * @see btree.BTreeFile#BTreeFile(String filename, int keytype,int keysize, int delete_fashion)
     * @param filename
     * @param columnfile
     * @param ColumnNo
     * @param value
     */
    public BitMapFile(java.lang.String filename, Columnarfile columnfile, int ColumnNo, ValueClass value) throws GetFileEntryException, AddFileEntryException, ConstructPageException, IOException {
        headerPageId = get_file_entry(filename);
        if (headerPageId == null) //file not exist
        {
            headerPage= new BitMapHeaderPage();
            headerPageId = headerPage.getPageId();
            add_file_entry(filename, headerPageId);
            headerPage.set_rootId(new PageId(INVALID_PAGE));
        } else {
            headerPage= new  BitMapHeaderPage(headerPageId);
        }

        dbname = new String(filename);
    }

    void close() throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
        //    Close the BitMap file.
        if (headerPage != null) {
            SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
            headerPage = null;
        }
    }

    void destroyBitMapFile() throws IteratorException, IOException, PinPageException, ConstructPageException, FreePageException, UnpinPageException, DeleteFileEntryException {
        if (headerPage != null) {
            PageId pgId = headerPage.getPageId();
            if (pgId.pid != INVALID_PAGE)
                _destroyFile(pgId);
            unpinPage(headerPageId);
            freePage(headerPageId);
            delete_file_entry(dbname);
            headerPage = null;
        }
    }

    private void _destroyFile(PageId pageno)
            throws IOException,
            IteratorException,
            PinPageException,
            ConstructPageException,
            UnpinPageException,
            FreePageException {

        Page page = pinPage(pageno);
        // todo traverse to free all child pages

//        if (sortedPage.getType() == NodeType.INDEX) {
//
//            BTIndexPage indexPage = new BTIndexPage(page, headerPage.get_keyType());
//            RID rid = new RID();
//            PageId childId;
//            KeyDataEntry entry;
//            for (entry = indexPage.getFirst(rid);
//                 entry != null; entry = indexPage.getNext(rid)) {
//                childId = ((IndexData) (entry.data)).getData();
//                _destroyFile(childId);
//            }
//        } else { // BTLeafPage
//
//            unpinPage(pageno);
//            freePage(pageno);
//        }

    }


    private void unpinPage(PageId pageno)
            throws UnpinPageException {
        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);
        } catch (Exception e) {
            e.printStackTrace();
            throw new UnpinPageException(e, "");
        }
    }

    private void freePage(PageId pageno)
            throws FreePageException {
        try {
            SystemDefs.JavabaseBM.freePage(pageno);
        } catch (Exception e) {
            e.printStackTrace();
            throw new FreePageException(e, "");
        }

    }

    private void delete_file_entry(String filename)
            throws DeleteFileEntryException {
        try {
            SystemDefs.JavabaseDB.delete_file_entry(filename);
        } catch (Exception e) {
            e.printStackTrace();
            throw new DeleteFileEntryException(e, "");
        }
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

    BitMapHeaderPage getHeaderPage(){
        return headerPage;
    }

    public boolean Delete(int position){
        return false;// todo
        //    set the entry at the given position to 0.
    }

    public boolean Insert(int position){
        return false;//todo
        //    set the entry at the given position to 1.
    }

    private PageId get_file_entry(String filename)
            throws GetFileEntryException {
        try {
            return SystemDefs.JavabaseDB.get_file_entry(filename);
        } catch (Exception e) {
            e.printStackTrace();
            throw new GetFileEntryException(e, "");
        }
    }

    private void add_file_entry(String fileName, PageId pageno)
            throws AddFileEntryException {
        try {
            SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
        } catch (Exception e) {
            e.printStackTrace();
            throw new AddFileEntryException(e, "");
        }
    }

    public void createNewHeadPage() throws IOException, HFBufMgrException {
        BMPage bmPage = getNewBMPage();
        bmPage.setPrevPage(headerPageId);
        cursorBMPage = bmPage;
    }

    public void cursorInsert(boolean bit) throws IOException {
        // write x number of bits to local buffer
        // when x number of bits has been exceeded local buffer, create new page and link
        cursorBuffer.add(bit);

        // if cursorBuffersize is close to available size on page, write and create a new page and link
        // size in bits                 available space in bytes
        //       1                              4
        //       8                              1

        if((Math.ceil(cursorBuffer.size() / 8) + 4) == ((double) cursorBMPage.available_space())){
            flushCursor();
        }


        //write data to page
    }

    public byte[] toBytes(boolean[] input) {
        byte[] toReturn = new byte[input.length / 8];
        for (int entry = 0; entry < toReturn.length; entry++) {
            for (int bit = 0; bit < 8; bit++) {
                if (input[entry * 8 + bit]) {
                    toReturn[entry] |= (128 >> bit);
                }
            }
        }

        return toReturn;
    }
    
    public boolean[] toBooleanArray(List list){
        boolean[] booleans = new boolean[list.size()];
        Iterator it = list.iterator();
        int i =0;

        while(it.hasNext()){
            booleans[i] = (boolean) it.next();
            i++;
        }

        return booleans;
    }

    public void flushCursor() throws IOException {
        // write current buffer to page
        cursorBMPage.insertRecord(toBytes(toBooleanArray(cursorBuffer)));
    }

    private BMPage getNewBMPage() throws HFBufMgrException, IOException {
        Page apage = new Page();
        PageId pageId = new PageId();
        pageId = newPage(apage, 1);

        BMPage bmPage = new BMPage();
        bmPage.init(pageId, apage);
        return bmPage;
    }

    private PageId newPage(Page page, int num)
            throws HFBufMgrException {

        PageId tmpId = new PageId();

        try {
            tmpId = SystemDefs.JavabaseBM.newPage(page,num);
        }
        catch (Exception e) {
            throw new HFBufMgrException(e,"Heapfile.java: newPage() failed");
        }

        return tmpId;

    } // end of newPage

    public void cursorComplete() throws IOException {
        flushCursor();
    }

    public void setCursorUniqueValue(ValueClass value) throws IOException, HFBufMgrException {
        // at this point BMPage should be pointing to the first page of that Unique value
        // update the header file to contain the unique values mapping to the link list of pages

        createNewHeadPage();
        BMHeaderPageDirectoryRecord directoryRecord = new BMHeaderPageDirectoryRecord(cursorBMPage.curPage, value);
        headerPage.insertRecord(directoryRecord.getByteArray());

        // insert a directory page with the following info
        // UniqueValue (string or int) -> FirstPage of that

        // if its the first time this is called do above, if its the second time, then get a new page and set that to the currentBPPage, then do the above


        // BitMap header file maintains BitVector To Page Id Mapping
        // ex:
        // BitMapHeaderPage:
        //      UniqueValue (string or int) Vector1 -> PageId
        //      UniqueValue (string or int) Vector2 -> PageId
        //      UniqueValue (string or int) Vector3 -> PageId
        //      UniqueValue (string or int) Vector4 -> PageId
        // (Vector for each unique value)
        // PageId is begining of link list of pages


    }

    public void initCursor() {
        cursorBuffer = new LinkedList<>();
    }
}
