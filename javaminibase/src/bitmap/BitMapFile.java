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

/**
 * Create a class called BitMapFile with the following specifications (see BTreeFile for analogy):
 */
public class BitMapFile implements GlobalConst{

    private String dbname;
    private BitMapHeaderPage headerPage;
    private PageId headerPageId;
    private BMPage cursorBMPage;

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

    public void initCursor() throws IOException, HFBufMgrException {
//        headerPage.set_rootId( create new page (first page) )
        // todo, we need more then just a pointer to the root,
        // we need a directory

        if(headerPage.getNextPage().pid != INVALID_PAGE){

            // below is allocate a new page
            // set the next link to null (INVALID PAGE)
            //
            BMPage bmPage = getNewBMPage();

            bmPage.setPrevPage(headerPageId);
            cursorBMPage = bmPage;
        }
    }

    public void cursorInsert(boolean bit){
        // write x number of bits to local buffer
        // when x number of bits has been exceeded local buffer, create new page and link

        // BitMap header file maintains BitVector To Page Id Mapping
        // ex:
        // BitMapHeaderPage:
        //      Vector1 -> PageId
        //      Vector2 -> PageId
        //      Vector3 -> PageId
        //      Vector4 -> PageId
        // (Vector for each unique value)
        // PageId is begining of link list of pages

        //write data to page
    }

    public void flushCursor(){
        // write current buffer to page

    }

    private BMPage getNewBMPage() throws HFBufMgrException, IOException {
        Page apage = new Page();
        PageId pageId = new PageId();
        pageId = newPage(apage, 1);

        BMPage bmPage = new BMPage(apage);
        bmPage.setNextPage(new PageId(INVALID_PAGE));
        bmPage.setCurPage(pageId);
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
}
