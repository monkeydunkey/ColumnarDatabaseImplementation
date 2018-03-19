package columnar;

import java.io.*;
import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;
import java.nio.charset.Charset;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;



interface  Filetype {
    int TEMP = 0;
    int ORDINARY = 1;

}

public class Columnarfile implements Filetype,  GlobalConst {

    static int numColumns;
    AttrType[] type;
    Heapfile[] columnFile;
    PageId      _metaPageId;   // page number of header page
    int         _ftype;
    private int tupleCount = 0;
    private     boolean     _file_deleted;
    private     String 	 _fileName;
    private static int tempfilecount = 0;
    private static int INTSIZE = 4;


    private static String[] _convertToStrings(byte[] byteStrings) {
        

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
            InvalidSlotNumberException,
            InvalidTupleSizeException,
            SpaceNotAvailableException,

            IOException{
        numColumns = totalColumns;
        type = attrType;

       

        // Copying them directly from the heap file constructor.
        _file_deleted = true;
        _fileName = null;

        // We are assuming that the table name will be provided we will not be creating
        // temporary tables right now. Maybe later we can add a clause for it as well
        // if it is required for running queries

        _fileName = name + "." + "hdr";
        _ftype = ORDINARY;
        columnFile = new Heapfile[totalColumns];


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

            metaPage.setNextPage(pageId);
            metaPage.setPrevPage(pageId);

            //Initializing heap files for each of the column
            for (int i = 0; i < totalColumns; i++){
                String columnName = name + "." + String.valueOf(i);
                columnFile[i] = new Heapfile(columnName);
                /*
                String[] metaInfo = new String[2];
                //metaInfo[0] = columnName;
                metaInfo[1] = ;
                */
                metaPage.insertRecord(_convertToBytes(columnName + "$" + attrType[i].toString()));

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

            int i = 0;
            for (RID columnRid = metaPage.firstRecord();
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
        
        _file_deleted = true;

        for (int i = 0; i < numColumns; i++){
            columnFile[i].deleteFile();
        }
    }

    public TID insertTuple(byte[] tuplePtr) throws SpaceNotAvailableException{
        if(tupleptr.length >= MAX_SPACE)    {
            throw new SpaceNotAvailableException(null, "Columnarfile: no available space");
        }

        int i = 0;
        int offset = 0; //The starting location of each column
        TID tid = new TID(numColumns);
        tid.recordIDs = new RID[numColumns];
        
        for (AttrType attr: type) {
          tid.recordIDs[i] = new RID();
          //scan each column type  
          if (attr.attrType == AttrType.attrInteger) {
            //insert type int
            int intAttr = Convert.getIntValue(offset,tupleptr);
            offset = offset + INTSIZE;
            
            byte[] intValue = new byte[INTSIZE];
            Convert.setIntValue(intAttr, 0, intValue);
            tid.recordIDs[i] = columnFile[i].insertRecord(intValue);
          }
          if (attr.attrType == AttrType.attrString) {
            //insert type String
            String strAttr = Convert.getStrValue(offset,tupleptr,Size.STRINGSIZE);
            offset = offset + Size.STRINGSIZE;

            byte[] strValue = new byte[Size.STRINGSIZE];
            Convert.setStrValue(strAttr, 0, strValue);
            tid.recordIDs[i] = columnFile[i].insertRecord(strValue);
          }
          
          i++;
        }

        tid.numRIDs = i;
        tid.position = columnFile[0].RidToPos(tid.recordIDs[0]);
        return tid;
    }
    
    /** Initiate a tuple sequential scan on Columnar File.
     * @exception InvalidTupleSizeException Invalid tuple size
     * @exception IOException I/O errors
     *
     */
    public TupleScan openTupleScan() 
      throws InvalidTupleSizeException,
  	   IOException
      {
        TupleScan tpScan = new TupleScan(this);
        return tpScan;
      }

    public Tuple getTuple(TID tid){
        //Tuple[] tupleArr = new Tuple[numColumns];
        Tuple tupleArr;
        int totalLength = 0;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (int i = 0; i < numColumns; i++) {
            tupleArr = columnFile[i].getRecord(tid.recordIDs[i]);
            totalLength += tupleArr.getLength();
            outputStream.write( tupleArr.getTupleByteArray());
        }
        return new Tuple(outputStream.toByteArray(), 0, totalLength);


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



    // The following functions are copied from heap class and is something that should not be done

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