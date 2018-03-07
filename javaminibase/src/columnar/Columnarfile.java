package columnar;

import java.io.*;
import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;

public class Columnarfile {
    static int numColumns;
    AttrType[] type;
    Heapfile[] columnFile;
    PageId      _metaPageId;   // page number of header page
    int         _ftype;
    private     boolean     _file_deleted;
    private     String 	 _fileName;
    private static int tempfilecount = 0;

    private static String[] _convertToStrings(byte[][] byteStrings) {
        String[] data = new String[byteStrings.length];
        for (int i = 0; i < byteStrings.length; i++) {
            data[i] = new String(byteStrings[i], Charset.defaultCharset());

        }
        return data;
    }


    private static byte[][] _convertToBytes(String[] strings) {
        byte[][] data = new byte[strings.length][];
        for (int i = 0; i < strings.length; i++) {
            String string = strings[i];
            data[i] = string.getBytes(Charset.defaultCharset()); // you can chose charset
        }
        return data;
    }



    public Columnarfile(String name, int totalColumns, AttrType[] attrType)
            throws HFException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException{
        numColumns = totalColumns;
        type = attrType;

        /*
        The way the initialization should work is that for each of the of the
        columns we will have a separate heap file and we will use that heap
        file to manage data insertion, deletion and updates.

        The meta data file will also be stored in terms of a heap file. As the
        heap file structure is that of a directory. We do have to figure out
        how to store and add stuff. Maybe using the DataPageInfo object in package heap
        or creating something similar will be useful

        The way heap file are implemented there does not seems any use in specifying
        the data type. Probably we should store that information in the metadata file

        Metadata attributes:
        1. Attribute type of each of the file
        2. name of the each of the columns which will be tablename.columnId

        The name of the column should be enough to reference the heap file stored for that
        column in memory

         */

        // Copying them directly from the heap file constructor.
        _file_deleted = true;
        _fileName = null;

        // We are assuming that the table name will be provided we will not be creating
        // temporary tables right now. Maybe later we can add a clause for it as well
        // if it is required for running queries
        _fileName = name + "." + 'hdr';
        _ftype = ORDINARY;
        columnFile = Heapfile[totalColumns];

        // The constructor gets run in two different cases.
        // In the first case, the file is new and the header page
        // must be initialized.  This case is detected via a failure
        // in the db->get_file_entry() call.  In the second case, the
        // file already exists and all that must be done is to fetch
        // the header page into the buffer pool

        // try to open the file

        Page apage = new Page();
        _metaPageId = null;
        //Getting the file entry from harddisk I think
        if (_ftype == ORDINARY)
            _metaPageId = get_file_entry(_fileName);

        if(_metaPageId==null)
        {
            // file doesn't exist. First create it.
            _metaPageId = newPage(apage, 1);
            // check error
            if(_metaPageId == null)
                throw new HFException(null, "can't new page");

            add_file_entry(_fileName, _metaPageId);
            // check error(new exception: Could not add file entry

            HFPage metaPage = new HFPage();
            metaPage.init(_metaPageId, apage);
            PageId pageId = new PageId(INVALID_PAGE);

            firstDirPage.setNextPage(pageId);
            firstDirPage.setPrevPage(pageId);

            //Initializing heap files for each of the column
            for (int i = 0; i < totalColumns; i++){
                columnName = name + "." + String.valueOf(i);
                columnFile[i] = new Heapfile(columnName);
                String[] metaInfo = new String[2];
                metaInfo[0] = columnName;
                metaInfo[1] = attrType[i].toString();
                metaPage.insertRecord(_convertToBytes(metaInfo));
            }
            unpinPage(_metaPageId, true /*dirty*/ );
        }
        else{
            //got to read the entries and populate the meta data information
            //TODO: read from the meta data page
            PageId metaPageId = new PageId(_metaPageId.pid);
            PageId nextDirPageId = new PageId();  // OK
            HFPage metaPage = new HFPage();
            Tuple atuple;
            //pinning the page so that it is not flushed while we are getting the column metadata
            pinPage(metaPageId, metaPage, false/*Rdisk*/);
            // Right now the assumption is that we are only storing as many tuples as the number of rows
            // We would need to update this if we want to store somothing else here as well
            for (columnRid = metaPage.firstRecord(), i = 0;
                 columnRid != null;
                 columnRid = metaPage.nextRecord(columnRid), i++)
            {
                atuple = metaPage.getRecord(columnRid);
                // convert this byte array to string to get the column name and attr type back
                String[] metaInfo = _convertToStrings(atuple.getTupleByteArray());
                columnFile[i] = new Heapfile(metaInfo[0]);
            }
            unpinPage(metaPageId, true /* = DIRTY */);
        }
        _file_deleted = false;



    }

    public void deleteColumnarFile(){
        throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }

    public TID insertTuple(byte[] tuplePtr){
        throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }

    public ValueClass insertTuple(byte[] tuplePtr){
        throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }

    public int getTupleCnt(){
        throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }

    public TupleScan openTupleScan(){
        throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }

    public Scan openColumnScan(int columnNo){
        throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }

    public boolean updateTuple(TID tid, Tuple newtuple){
        throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }

    public boolean updateColumnofTuple(TID tid, Tuple newtuple, int column){
        throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }

    public boolean createBTreeIndex(int column){
        throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }

    public boolean createBitMapIndex(int columnNo, valueClass value){
        throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }

    public boolean markTupleDeleted(TID tid){
        throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }

    public boolean purgeAllDeletedTuples(){
        throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }


    // The following functions are copied from heap class and is something that should be done
    // as it introduces code duplication but it should be fine for our purposes

    /**
     * short cut to access the pinPage function in bufmgr package.
     * @see bufmgr.pinPage
     */
    private void pinPage(PageId pageno, Page page, boolean emptyPage)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
        }
        catch (Exception e) {
            throw new HFBufMgrException(e,"Heapfile.java: pinPage() failed");
        }

    } // end of pinPage

    /**
     * short cut to access the unpinPage function in bufmgr package.
     * @see bufmgr.unpinPage
     */
    private void unpinPage(PageId pageno, boolean dirty)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        }
        catch (Exception e) {
            throw new HFBufMgrException(e,"Heapfile.java: unpinPage() failed");
        }

    } // end of unpinPage

    private void freePage(PageId pageno)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.freePage(pageno);
        }
        catch (Exception e) {
            throw new HFBufMgrException(e,"Heapfile.java: freePage() failed");
        }

    } // end of freePage

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

    private PageId get_file_entry(String filename)
            throws HFDiskMgrException {

        PageId tmpId = new PageId();

        try {
            tmpId = SystemDefs.JavabaseDB.get_file_entry(filename);
        }
        catch (Exception e) {
            throw new HFDiskMgrException(e,"Heapfile.java: get_file_entry() failed");
        }

        return tmpId;

    } // end of get_file_entry

    private void add_file_entry(String filename, PageId pageno)
            throws HFDiskMgrException {

        try {
            SystemDefs.JavabaseDB.add_file_entry(filename,pageno);
        }
        catch (Exception e) {
            throw new HFDiskMgrException(e,"Heapfile.java: add_file_entry() failed");
        }

    } // end of add_file_entry

    private void delete_file_entry(String filename)
            throws HFDiskMgrException {

        try {
            SystemDefs.JavabaseDB.delete_file_entry(filename);
        }
        catch (Exception e) {
            throw new HFDiskMgrException(e,"Heapfile.java: delete_file_entry() failed");
        }

    } // end of delete_file_entry


}