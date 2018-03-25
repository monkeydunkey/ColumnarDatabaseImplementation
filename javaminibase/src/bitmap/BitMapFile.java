package bitmap;

import bufmgr.*;
import diskmgr.Page;
import global.*;
import heap.*;
import columnar.*;

import java.io.IOException;

private BitMapHeaderPage headerPage;
private PageId  headerPageId;
private String  dbname;
private Columnarfile cFile;
private int columnNo;
private ValueClass value;

/**
 * Create a class called BitMapFile with the following specifications (see BTreeFile for analogy):
 */
public class BitMapFile implements GlobalConst extends HeapFile{

    public void close()

    {
        try {
            if (headerPage != null) {
                SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
                headerPage = null;
            }
        }
        catch (Exception e) {
            e.printStackTree();
        }
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
