package bitmap;

import btree.*;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import columnar.Columnarfile;
import diskmgr.Page;
import global.*;

import java.io.IOException;

/**
 * Create a class called BitMapFile with the following specifications (see BTreeFile for analogy):
 */
public class BitMapFile implements GlobalConst{

    private String dbname;
    private BitMapHeaderPage headerPage;
    private PageId headerPageId;

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

    private void unpinPage(PageId pageno, boolean dirty)
            throws UnpinPageException {
        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        } catch (Exception e) {
            e.printStackTrace();
            throw new UnpinPageException(e, "");
        }
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

    boolean Delete(int position){
        return false;// todo
        //    set the entry at the given position to 0.
    }

    boolean Insert(int position){
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
}
