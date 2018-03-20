package columnar;

import diskmgr.Page;
import global.*;
import heap.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
interface  Filetype {
    int TEMP = 0;
    int ORDINARY = 1;

}

public class Columnarfile implements Filetype,  GlobalConst {
    public static int numColumns;
    public AttrType[] type;
    public Heapfile[] columnFile;
    public Heapfile   HeaderFile;
    public PageId      _metaPageId;   // page number of header page
    public int         _ftype;
    private     boolean     _file_deleted;
    private     String 	 _fileName;
    private     int INTSIZE = 4;
    private     int STRINGSIZE = 25; //The default string size
    private static int tempfilecount = 0;
    private     int[] offsets; //store the offset count for each column
    private static String _convertToStrings(byte[] byteStrings) {
        /*
        String[] data = new String[byteStrings.length];
        for (int i = 0; i < byteStrings.length; i++) {
            data[i] = new String(byteStrings[i], Charset.defaultCharset());

        }
        return data;
        */
        return new String(byteStrings);
    }


    private static byte[] _convertToBytes(String st) {
        /*
        byte[][] data = new byte[strings.length][];
        for (int i = 0; i < strings.length; i++) {
            String string = strings[i];
            data[i] = string.getBytes(Charset.defaultCharset()); // you can chose charset
        }
        return data;
        */
        return st.getBytes();
    }

    private byte[] _getColumnHeaderInsertTuple(String ColumnName, int Type, int Offset){
        byte[] ColumnNameByteArr = ColumnName.getBytes();
        ValueIntClass typeArr = new ValueIntClass(Type);
        ValueIntClass offsetArr = new ValueIntClass(Offset);
        byte[] arr = new byte[ColumnNameByteArr.length + 8]; // it is 4 + 4 for 2 ints
        System.arraycopy (ColumnNameByteArr, 0, arr, 0, ColumnNameByteArr.length);
        System.arraycopy (typeArr.getByteArr(), 0, arr, ColumnNameByteArr.length, 4);
        System.arraycopy (offsetArr.getByteArr(), 0, arr, ColumnNameByteArr.length + 4, 4);
        return arr;
    }

    private String _getColumnStringName(byte[] arr){
        byte[] stringArr = new byte[arr.length - 8]; // it is 4 + 4 for 2 ints
        System.arraycopy (arr, 0, stringArr, 0, stringArr.length);
        return _convertToStrings(stringArr);
    }


    //Assumming this constructor will only be called to create a new Columnar File
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
        _fileName = name + "." + "hdr";
        _ftype = ORDINARY;
        // +1 for storing the deletion heap file
        columnFile = new Heapfile[totalColumns + 1];
        offsets = new int[totalColumns];
        // The constructor gets run in two different cases.
        // In the first case, the file is new and the header page
        // must be initialized.  This case is detected via a failure
        // in the db->get_file_entry() call.  In the second case, the
        // file already exists and all that must be done is to fetch
        // the header page into the buffer pool

        // try to open the file
        HeaderFile = new Heapfile(_fileName);
        ValueIntClass columnCount = new ValueIntClass(totalColumns + 1);
        HeaderFile.insertRecord(columnCount.getByteArr());

        //Initializing heap files for each of the column
        for (int i = 0; i < totalColumns; i++){
            String columnName = name + "." + String.valueOf(i);
            columnFile[i] = new Heapfile(columnName);
            /*
                String[] metaInfo = new String[2];
                //metaInfo[0] = columnName;
                metaInfo[1] = ;
             */
            int offset = attrType[i].toString() == "attrInteger" ? INTSIZE : STRINGSIZE;
            HeaderFile.insertRecord(_getColumnHeaderInsertTuple(columnName, attrType[i].attrType, offset));
            offsets[i] = offset;
        }
        //Inserting the final entry for delete marking file
        String columnName = name + ".deletion";
        columnFile[totalColumns] = new Heapfile(columnName);

        int offset = INTSIZE;
        HeaderFile.insertRecord(_getColumnHeaderInsertTuple(columnName, 1, offset));
    }

    public Columnarfile(String name)
            throws HFException,
            HFBufMgrException,
            HFDiskMgrException,
            InvalidSlotNumberException,
            InvalidTupleSizeException,
            SpaceNotAvailableException,
            IOException
    {
        _fileName = name + "." + "hdr";
        HeaderFile = new Heapfile(_fileName);
        Scan headerFileScan = HeaderFile.openScan();
        RID emptyRID = new RID();
        Tuple colCountTuple = headerFileScan.getNext(emptyRID);
        ValueIntClass columnCount = new ValueIntClass(colCountTuple.getTupleByteArray());
        numColumns = columnCount.value - 1;
        columnFile = new Heapfile[numColumns + 1];
        offsets = new int[numColumns];
        type = new AttrType[numColumns];
        for (int i = 0; i < columnCount.value; i++){
            colCountTuple = headerFileScan.getNext(emptyRID);
            byte[] colData = colCountTuple.getTupleByteArray();
            String colName = Convert.getStrValue(0, colData, colData.length - 8);
            int colType = Convert.getIntValue(colData.length - 8, colData);
            int colOffset = Convert.getIntValue(colData.length - 4, colData);
            if (i != columnCount.value - 1) {
                offsets[i] = colOffset;
                type[i] = new AttrType(colType);
            }
            columnFile[i] = new Heapfile(colName);
        }
    }

    public void deleteColumnarFile()
            throws InvalidSlotNumberException,
            FileAlreadyDeletedException,
            InvalidTupleSizeException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException
    {

        _file_deleted = true;

        for (int i = 0; i < numColumns; i++){
            columnFile[i].deleteFile();
        }
    }

    public TID insertTuple(byte[] tupleptr)
            throws SpaceNotAvailableException,
            InvalidSlotNumberException,
            InvalidTupleSizeException,
            SpaceNotAvailableException,
            HFException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException
    {
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
            offset = offset + offsets[i];

            byte[] intValue = new byte[offsets[i]];
            Convert.setIntValue(intAttr, 0, intValue);
            tid.recordIDs[i] = columnFile[i].insertRecord(intValue);
          }
          if (attr.attrType == AttrType.attrString) {
            //insert type String
            String strAttr = Convert.getStrValue(offset,tupleptr,offsets[i]);
            offset = offset + offsets[i];

            byte[] strValue = new byte[offsets[i]];
            Convert.setStrValue(strAttr, 0, strValue);
            tid.recordIDs[i] = columnFile[i].insertRecord(strValue);
          }

          i++;
        }

        tid.numRIDs = i;
        tid.position = columnFile[0].RidToPos(tid.recordIDs[0]);
        return tid;
    }






    public Tuple getTuple(TID tid)
            throws InvalidSlotNumberException,
            InvalidTupleSizeException,
            HFException,
            HFDiskMgrException,
            HFBufMgrException,
            Exception
    {
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

    public int getTupleCnt()
            throws InvalidSlotNumberException,
            InvalidTupleSizeException,
            HFDiskMgrException,
            HFBufMgrException,
            IOException
    {
        //As all the heap files containing the different columns should have the same row count, getting
        // row count from any one should be enough
        return columnFile[0].getRecCnt();
    }

    public ValueClass getValue(TID tid, int column)
            throws InvalidSlotNumberException,
            InvalidTupleSizeException,
            HFException,
            HFDiskMgrException,
            HFBufMgrException,
            Exception
    {
        Tuple tupleArr = columnFile[column].getRecord(tid.recordIDs[column]);
        ValueClass colVal;
        switch (type[column].attrType){
            case AttrType.attrString:
                colVal = new ValueStrClass(tupleArr.getTupleByteArray());
                break;
            case AttrType.attrInteger:
                colVal = new ValueIntClass(tupleArr.getTupleByteArray());
                break;
            default:
                throw new Exception("Unexpected AttrType" + type[column].toString());
            }
        return colVal;
    }
    /*
    // Commenting it to avoid build failures
    public TupleScan openTupleScan(){
        throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }
    */
    public Scan openColumnScan(int columnNo)
            throws InvalidTupleSizeException,
            IOException
    {
        return columnFile[columnNo].openScan();
    }

    public boolean updateTuple(TID tid, Tuple newtuple)
            throws InvalidSlotNumberException,
            InvalidUpdateException,
            InvalidTupleSizeException,
            HFException,
            HFDiskMgrException,
            HFBufMgrException,
            Exception
    {
        byte[] arr;
        ValueClass updateValue;
        boolean retValue = true;
        //int offset = 0; //The starting location of each column

        for (int i = 0; i < numColumns; i++) {
            AttrType attr = type[i];
            switch (attr.attrType){
                case AttrType.attrString:
                    updateValue = new ValueStrClass(newtuple.getStrFld(i));
                    break;
                case AttrType.attrInteger:
                    updateValue = new ValueIntClass(newtuple.getIntFld(i));
                    break;
                default:
                    throw new Exception("Unexpected AttrType" + type[i].toString());
            }
            arr = updateValue.getByteArr();
            retValue &= updateColumnofTuple(tid, new Tuple(arr, 0, arr.length), i);
        }
        return retValue;
    }

    public boolean updateColumnofTuple(TID tid, Tuple newtuple, int column)
            throws InvalidSlotNumberException,
            InvalidUpdateException,
            InvalidTupleSizeException,
            HFException,
            HFDiskMgrException,
            HFBufMgrException,
            Exception
    {
        return columnFile[column].updateRecord(tid.recordIDs[column], newtuple);

    }

    public boolean createBTreeIndex(int column){
        throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }

    public boolean createBitMapIndex(int columnNo, ValueClass value){
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