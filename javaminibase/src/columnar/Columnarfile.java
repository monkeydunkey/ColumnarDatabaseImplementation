package columnar;

import btree.*;
import diskmgr.Page;
import global.*;
import heap.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

interface  Filetype {
    int TEMP = 0;
    int ORDINARY = 1;

}

public class Columnarfile implements Filetype,  GlobalConst {
    public static int numColumns;
    public AttrType[] type;
    public Heapfile[] columnFile;
    public IndexType[] indexType;
    public Heapfile   HeaderFile;
    public PageId      _metaPageId;   // page number of header page
    public int         _ftype;
    public String[] columnNames;


    private     boolean     _file_deleted;
    private     String 	 _fileName;
    private     int INTSIZE = 4;
    private     int STRINGSIZE = 25; //The default string size
    private static int tempfilecount = 0;
    private     int headerTupleOffset = 12;
    private     RID[] headerRIDs;
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

    private byte[] _getColumnHeaderInsertTuple(String ColumnName, int Type, int Offset, int index)
    {
        byte[] ColumnNameByteArr = ColumnName.getBytes();
        ValueIntClass typeArr = new ValueIntClass(Type);
        ValueIntClass offsetArr = new ValueIntClass(Offset);
        ValueIntClass indexArr = new ValueIntClass(index);
        byte[] arr = new byte[ColumnNameByteArr.length + headerTupleOffset]; // it is 4 + 4 + 4 for 3 ints
        System.arraycopy (ColumnNameByteArr, 0, arr, 0, ColumnNameByteArr.length);
        System.arraycopy (typeArr.getByteArr(), 0, arr, ColumnNameByteArr.length, 4);
        System.arraycopy (offsetArr.getByteArr(), 0, arr, ColumnNameByteArr.length + 4, 4);
        System.arraycopy (indexArr.getByteArr(), 0, arr, ColumnNameByteArr.length + 8, 4);
        return arr;
    }

    private String _getColumnStringName(byte[] arr){
        byte[] stringArr = new byte[arr.length - headerTupleOffset]; // it is 4 + 4 for 2 ints
        System.arraycopy (arr, 0, stringArr, 0, stringArr.length);
        return _convertToStrings(stringArr);
    }
    /*
    The aim is to represent the tuple information in form of a byte arr so that it can be stored in a heap file
    and then retrieved
     */
    public byte[] serializeTuple(TID tid){
        // 4 + 4 for numRIDs and Position. Then as we have 2 extra RID in each and every tuple. One for marking tuples
        //deleted and the second one for storing the serialized data. But as we are currently serializing the data
        // we don't have the last entry as that is what we are trying to create in this function
        byte[] serializedTuple = new byte[2*INTSIZE + (tid.numRIDs - 1)*(2*INTSIZE)];
        ValueIntClass numRIDArr = new ValueIntClass(tid.numRIDs);
        ValueIntClass positionArr = new ValueIntClass(tid.position);
        System.arraycopy (numRIDArr.getByteArr(), 0, serializedTuple, 0, INTSIZE);
        System.arraycopy (positionArr.getByteArr(), 0, serializedTuple, INTSIZE, INTSIZE);
        int curr_offset = 2 * INTSIZE;
        RID tempRID;
        ValueIntClass pageArr;
        ValueIntClass slotArr;
        for (int i = 0; i < tid.numRIDs - 1; i++){
            tempRID = tid.recordIDs[i];
            pageArr = new ValueIntClass(tempRID.pageNo.pid);
            slotArr = new ValueIntClass(tempRID.slotNo);
            System.arraycopy (pageArr.getByteArr(), 0, serializedTuple, curr_offset, INTSIZE);
            curr_offset += INTSIZE;
            System.arraycopy (slotArr.getByteArr(), 0, serializedTuple, curr_offset, INTSIZE);
            curr_offset += INTSIZE;
        }
        return serializedTuple;
    }

    public TID deserializeTuple(byte [] arr){
        byte[] numRIDArr = new byte[INTSIZE];
        byte[] posArr = new byte[INTSIZE];
        int curr_offset = 0;
        System.arraycopy (arr, curr_offset, numRIDArr, 0, INTSIZE);
        curr_offset += INTSIZE;
        System.arraycopy (arr, curr_offset, posArr, 0, INTSIZE);
        curr_offset += INTSIZE;
        ValueIntClass numRID = new ValueIntClass(numRIDArr);
        ValueIntClass position = new ValueIntClass(posArr);
        byte[] pageArr;
        byte[] slotArr;
        ValueIntClass slotNum;
        ValueIntClass pageNum;
        TID tid = new TID(numRID.value);
        tid.position = position.value;
        for (int i = 0; i < numRID.value - 1; i++){
            pageArr = new byte[INTSIZE];
            slotArr = new byte[INTSIZE];
            System.arraycopy (arr, curr_offset, pageArr, 0, INTSIZE);
            curr_offset += INTSIZE;
            System.arraycopy (arr, curr_offset, slotArr, 0, INTSIZE);
            curr_offset += INTSIZE;
            slotNum = new ValueIntClass(slotArr);
            pageNum = new ValueIntClass(pageArr);
            PageId tempPage = new PageId(pageNum.value);
            RID tempRID = new RID(tempPage, slotNum.value);
            tid.recordIDs[i] = tempRID;
        }
        return tid;

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
        indexType = new IndexType[numColumns];
        columnNames = new String[numColumns];
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
        1. name of the each of the columns which will be tablename.columnId
        2. Attribute type of each of the file
        3. Size of the attributes
        4. Index type on the column

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
        // +2 for storing the deletion heap file and RID to TID mapping heap file
        columnFile = new Heapfile[totalColumns + 2];
        headerRIDs = new RID[totalColumns + 2];
        offsets = new int[totalColumns];
        // The constructor gets run in two different cases.
        // In the first case, the file is new and the header page
        // must be initialized.  This case is detected via a failure
        // in the db->get_file_entry() call.  In the second case, the
        // file already exists and all that must be done is to fetch
        // the header page into the buffer pool

        // try to open the file
        HeaderFile = new Heapfile(_fileName);
        ValueIntClass columnCount = new ValueIntClass(totalColumns + 2);
        HeaderFile.insertRecord(columnCount.getByteArr());

        //Initializing heap files for each of the column
        for (int i = 0; i < totalColumns; i++){

            String columnName = name + "." + String.valueOf(i);
            columnNames[i] = columnName;
            columnFile[i] = new Heapfile(columnName);
            /*
                String[] metaInfo = new String[2];
                //metaInfo[0] = columnName;
                metaInfo[1] = ;
             */
            int offset = attrType[i].toString() == "attrInteger" ? INTSIZE : STRINGSIZE;
            headerRIDs[i] = HeaderFile.insertRecord(_getColumnHeaderInsertTuple(columnName, attrType[i].attrType, offset, 0));
            offsets[i] = offset;
            indexType[i] = new IndexType(0);
        }
        //Inserting the final entry for delete marking file
        String columnName = name + ".deletion";
        columnFile[totalColumns] = new Heapfile(columnName);
        headerRIDs[totalColumns] = HeaderFile.insertRecord(_getColumnHeaderInsertTuple(columnName, 1, INTSIZE, 0));

        //Inserting the final entry for tuple tracking file
        columnName = name + ".tupleTracking";
        columnFile[totalColumns + 1] = new Heapfile(columnName);
        headerRIDs[totalColumns + 1] = HeaderFile.insertRecord(_getColumnHeaderInsertTuple(columnName, 1, INTSIZE, 0));
    }

    public Columnarfile(String name, int totalColumns, AttrType[] attrType, String[] colNames)
            throws HFException,
            HFBufMgrException,
            HFDiskMgrException,
            InvalidSlotNumberException,
            InvalidTupleSizeException,
            SpaceNotAvailableException,
            IOException{
        numColumns = totalColumns;
        columnNames = colNames;
        type = attrType;
        indexType = new IndexType[numColumns];

        _file_deleted = true;
        _fileName = null;


        _fileName = name + "." + "hdr";
        _ftype = ORDINARY;
        // +2 for storing the deletion heap file and RID to TID mapping heap file
        columnFile = new Heapfile[totalColumns + 2];
        headerRIDs = new RID[totalColumns + 2];
        offsets = new int[totalColumns];


        // try to open the file
        HeaderFile = new Heapfile(_fileName);
        ValueIntClass columnCount = new ValueIntClass(totalColumns + 2);
        HeaderFile.insertRecord(columnCount.getByteArr());

        //Initializing heap files for each of the column
        for (int i = 0; i < totalColumns; i++){
            String columnName = name + "." + columnNames[i];
            columnFile[i] = new Heapfile(columnName);
            /*
                String[] metaInfo = new String[2];
                //metaInfo[0] = columnName;
                metaInfo[1] = ;
             */
            int offset = attrType[i].toString() == "attrInteger" ? INTSIZE : STRINGSIZE;
            headerRIDs[i] = HeaderFile.insertRecord(_getColumnHeaderInsertTuple(columnName, attrType[i].attrType, offset, 0));
            offsets[i] = offset;
            indexType[i] = new IndexType(0);
        }
        //Inserting the final entry for delete marking file
        String columnName = name + ".deletion";
        columnFile[totalColumns] = new Heapfile(columnName);
        headerRIDs[totalColumns] = HeaderFile.insertRecord(_getColumnHeaderInsertTuple(columnName, 1, INTSIZE, 0));

        //Inserting the final entry for tuple tracking file
        columnName = name + ".tupleTracking";
        columnFile[totalColumns + 1] = new Heapfile(columnName);
        headerRIDs[totalColumns + 1] = HeaderFile.insertRecord(_getColumnHeaderInsertTuple(columnName, 1, INTSIZE, 0));
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
        columnNames = new String[numColumns];
        Scan headerFileScan = HeaderFile.openScan();
        RID emptyRID = new RID();
        Tuple colCountTuple = headerFileScan.getNext(emptyRID);
        ValueIntClass columnCount = new ValueIntClass(colCountTuple.getTupleByteArray());
        numColumns = columnCount.value - 2;
        columnFile = new Heapfile[columnCount.value];
        headerRIDs = new RID[columnCount.value];
        indexType = new IndexType[numColumns];
        offsets = new int[numColumns];
        type = new AttrType[numColumns];

        for (int i = 0; i < columnCount.value; i++){
            emptyRID = new RID();
            colCountTuple = headerFileScan.getNext(emptyRID);
            //Storing the RID that was filled in by getNext
            headerRIDs[i] = emptyRID;
            byte[] colData = colCountTuple.getTupleByteArray();

            byte[] StringArr = new byte[colData.length - 8];
            System.arraycopy (colData, 0, StringArr, 0, StringArr.length);
            String colName = new String(StringArr);

            int colType = Convert.getIntValue(colData.length - 12, colData);
            int colOffset = Convert.getIntValue(colData.length - 8, colData);
            int index = Convert.getIntValue(colData.length - 4, colData);

            if (i < numColumns) {
                offsets[i] = colOffset;
                type[i] = new AttrType(colType);
                indexType[i] = new IndexType(index);
                columnNames[i] = colName.split(".")[1];
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
            try {
                columnFile[i].deleteFile();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void setAttrOffset(int[] offsetArr)
            throws Exception
    {
        if (offsets.length != offsetArr.length){
            throw new Exception("Offset array length is not correct");
        } else {
            offsets = offsetArr.clone();
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
        TID tid = new TID(numColumns + 2);
        tid.recordIDs = new RID[numColumns + 2];

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
        ValueIntClass newRow = new ValueIntClass(0);
        tid.recordIDs[numColumns] = columnFile[numColumns].insertRecord(newRow.getByteArr());
        tid.numRIDs = i;
        tid.recordIDs[numColumns + 1] = columnFile[numColumns].insertRecord(serializeTuple(tid));
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
        try {
            Tuple tupleArr;
            int totalLength = 0;
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            for (int i = 0; i < numColumns; i++) {
                tupleArr = columnFile[i].getRecord(tid.recordIDs[i]);
                totalLength += tupleArr.getLength();
                outputStream.write(tupleArr.getTupleByteArray());
            }
            return new Tuple(outputStream.toByteArray(), 0, totalLength);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return null;

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
        byte[] data;
        int totalOffset = 0;
        for (int i = 0; i < numColumns; i++) {
            AttrType attr = type[i];
            data = new byte[offsets[i]];
            System.arraycopy (newtuple.getTupleByteArray(), totalOffset, data, 0, offsets[i]);
            totalOffset += offsets[i];
            switch (attr.attrType){
                case AttrType.attrString:
                    updateValue = new ValueStrClass(data);
                    break;
                case AttrType.attrInteger:
                    updateValue = new ValueIntClass(data);
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
    /*
     Helper function to be called when an index is created or update on a column
    */
    public void updateIndexType(int column, int index)
            throws Exception
    {
        indexType[column] = new IndexType(index);
        String columnName = _fileName + "." + String.valueOf(column);
        byte[] headerUpdateArr = _getColumnHeaderInsertTuple(columnName, type[column].attrType, offsets[column], index);
        HeaderFile.updateRecord(headerRIDs[column], new Tuple(headerUpdateArr, 0, headerUpdateArr.length));
    }

    public boolean createBTreeIndex(int column)
            throws  GetFileEntryException,
                    ConstructPageException,
                    IOException,
                    AddFileEntryException
    {
        try {
            String indexFileName = _fileName + "." + String.valueOf(column) + ".Btree";
            //Setting the delete fashion to 1 which seems to be the default
            BTreeFile btree = new BTreeFile(indexFileName, type[column].attrType, offsets[column], 1);
            TupleScan cfs = new TupleScan(this);
            TID emptyTID = new TID(numColumns);
            Tuple dataTuple =  cfs.getNext(emptyTID);
            while (dataTuple != null){
                int offset = 0;
                KeyClass key;
                for (int i = 0; i < column; i++) {
                    offset += offsets[i];
                }
                byte[] dataArr = new byte[offsets[column]];
                System.arraycopy (dataTuple.getTupleByteArray(), offset, dataArr, 0, offsets[column]);
                switch (type[column].attrType){
                    case AttrType.attrString:
                        ValueStrClass st = new ValueStrClass(dataArr);
                        key = new StringKey(st.value);
                        break;
                    case AttrType.attrInteger:
                        ValueIntClass it = new ValueIntClass(dataArr);
                        key = new IntegerKey(it.value);
                        break;
                    default:
                        throw new Exception("Unexpected AttrType" + type[column].toString());
                }
                btree.insert(key, emptyTID.recordIDs[numColumns + 1]);
            }
        }
        catch (Exception ex){
            ex.printStackTrace();
            return false;
        }
        return true;
    }

    public boolean createBitMapIndex(int columnNo, ValueClass value){
        throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }

    public boolean markTupleDeleted(TID tid)
            throws InvalidSlotNumberException,
            InvalidUpdateException,
            InvalidTupleSizeException,
            HFException,
            HFDiskMgrException,
            HFBufMgrException,
            Exception
    {
        ValueIntClass toDelete = new ValueIntClass(1);
        byte[] arr = toDelete.getByteArr();
        return columnFile[numColumns].updateRecord(tid.recordIDs[numColumns], new Tuple(arr, 0, arr.length));
    }

    public boolean purgeAllDeletedTuples(){
        throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }


    public void setColumnNames(String[] columnnames) {
        this.columnNames = columnnames;
    }

    public String[] getColumnNames() {
        return this.columnNames;
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


    public int getColumnIndexByName(String columnName){
//        if(getColumnNames() != null){
//            return Arrays.asList(getColumnNames()).indexOf(columnName);
//        }
        for( int i = 0; i < columnNames.length; i++ )
        {
            if( columnNames[i].equals(columnName))
            {
                return i;
            }
        }
        return -1;
    }

    public ValueClass getColumnTypeByName(String columnName){
        int columnIndexByName = getColumnIndexByName(columnName);
        if(columnIndexByName != -1){
            AttrType attrType = type[columnIndexByName];
            if(attrType != null){
                return attrType.getValueClass();
            }
        }
        return null;
    }


}