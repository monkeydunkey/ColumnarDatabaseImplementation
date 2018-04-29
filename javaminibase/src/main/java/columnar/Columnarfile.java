package columnar;

import bitmap.BM;
import bitmap.BitMapFile;
import btree.*;
import diskmgr.Page;
import global.*;
import heap.*;
import iterator.ColumnarFileScan;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;

interface Filetype {
    int TEMP = 0;
    int ORDINARY = 1;

}

public class Columnarfile implements Filetype, GlobalConst {
    public int numColumns;
    public AttrType[] type;
    public Heapfile[] columnFile;
    public IndexType[] indexType;
    public Heapfile HeaderFile;
    public PageId _metaPageId;   // page number of header page
    public int _ftype;
    public String[] columnNames;
    private int currBatchInsertToken = -1;
    private int currPageNo = -1;
    private int currSlotno = -1;
    private int currPosition = 0;
    private boolean _file_deleted;
    private String _fileName;
    private int INTSIZE = 4;
    public int STRINGSIZE = 25; //The default string size
    private static int tempfilecount = 0;
    private int headerTupleOffset = 12;
    private RID[] headerRIDs;
    public int[] offsets; //store the offset count for each column

    public String getFileName() {
        return _fileName;
    }

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

    private byte[] _getColumnHeaderInsertTuple(String ColumnName, int Type, int Offset, int index) {
        byte[] ColumnNameByteArr = ColumnName.getBytes();
        ValueIntClass typeArr = new ValueIntClass(Type);
        ValueIntClass offsetArr = new ValueIntClass(Offset);
        ValueIntClass indexArr = new ValueIntClass(index);
        byte[] arr = new byte[ColumnNameByteArr.length + headerTupleOffset]; // it is 4 + 4 + 4 for 3 ints
        System.arraycopy(ColumnNameByteArr, 0, arr, 0, ColumnNameByteArr.length);
        System.arraycopy(typeArr.getByteArr(), 0, arr, ColumnNameByteArr.length, 4);
        System.arraycopy(offsetArr.getByteArr(), 0, arr, ColumnNameByteArr.length + 4, 4);
        System.arraycopy(indexArr.getByteArr(), 0, arr, ColumnNameByteArr.length + 8, 4);
        return arr;
    }

    private String _getColumnStringName(byte[] arr) {
        byte[] stringArr = new byte[arr.length - headerTupleOffset]; // it is 4 + 4 for 2 ints
        System.arraycopy(arr, 0, stringArr, 0, stringArr.length);
        return _convertToStrings(stringArr);
    }

    /*
    The aim is to represent the tuple information in form of a byte arr so that it can be stored in a heap file
    and then retrieved
     */
    public byte[] serializeTuple(TID tid) {
        // 4 + 4 for numRIDs and Position. Then as we have 2 extra RID in each and every tuple. One for marking tuples
        //deleted and the second one for storing the serialized data. But as we are currently serializing the data
        // we don't have the last entry as that is what we are trying to create in this function
        byte[] serializedTuple = new byte[2 * INTSIZE + (tid.recordIDs.length - 1) * (2 * INTSIZE)];
        ValueIntClass numRIDArr = new ValueIntClass(tid.numRIDs);
        ValueIntClass positionArr = new ValueIntClass(tid.position);
        System.arraycopy(numRIDArr.getByteArr(), 0, serializedTuple, 0, INTSIZE);
        System.arraycopy(positionArr.getByteArr(), 0, serializedTuple, INTSIZE, INTSIZE);
        int curr_offset = 2 * INTSIZE;
        RID tempRID;
        ValueIntClass pageArr;
        ValueIntClass slotArr;
        for (int i = 0; i < tid.recordIDs.length - 1; i++) {
            tempRID = tid.recordIDs[i];
            pageArr = new ValueIntClass(tempRID.pageNo.pid);
            slotArr = new ValueIntClass(tempRID.slotNo);
            System.arraycopy(pageArr.getByteArr(), 0, serializedTuple, curr_offset, INTSIZE);
            curr_offset += INTSIZE;
            System.arraycopy(slotArr.getByteArr(), 0, serializedTuple, curr_offset, INTSIZE);
            curr_offset += INTSIZE;
        }
        return serializedTuple;
    }

    public TID deserializeTuple(byte[] arr) {
        byte[] numRIDArr = new byte[INTSIZE];
        byte[] posArr = new byte[INTSIZE];
        int curr_offset = 0;
        System.arraycopy(arr, curr_offset, numRIDArr, 0, INTSIZE);
        curr_offset += INTSIZE;
        System.arraycopy(arr, curr_offset, posArr, 0, INTSIZE);
        curr_offset += INTSIZE;
        ValueIntClass numRID = new ValueIntClass(numRIDArr);
        ValueIntClass position = new ValueIntClass(posArr);
        byte[] pageArr;
        byte[] slotArr;
        ValueIntClass slotNum;
        ValueIntClass pageNum;
        // For the additional deletion and TID encoding heap files
        TID tid = new TID(numRID.value + 2);
        tid.position = position.value;
        // + 1 as we would not have stored the TID encoding in the encoding itself
        for (int i = 0; i < numRID.value + 1; i++) {
            pageArr = new byte[INTSIZE];
            slotArr = new byte[INTSIZE];
            System.arraycopy(arr, curr_offset, pageArr, 0, INTSIZE);
            curr_offset += INTSIZE;
            System.arraycopy(arr, curr_offset, slotArr, 0, INTSIZE);
            curr_offset += INTSIZE;
            slotNum = new ValueIntClass(slotArr);
            pageNum = new ValueIntClass(pageArr);

            RID tempRID = new RID(new PageId(pageNum.value), slotNum.value);
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
            IOException {
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
        _fileName = getHeapFileName(name);
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
        for (int i = 0; i < totalColumns; i++) {

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

    public static String getHeapFileName(String name) {
        return name + "." + "hdr";
    }

    public Columnarfile(String name, int totalColumns, AttrType[] attrType, String[] colNames)
            throws HFException,
            HFBufMgrException,
            HFDiskMgrException,
            InvalidSlotNumberException,
            InvalidTupleSizeException,
            SpaceNotAvailableException,
            IOException {
        numColumns = totalColumns;
        columnNames = colNames;
        type = attrType;
        indexType = new IndexType[numColumns];

        _file_deleted = true;
        _fileName = null;


        _fileName = getHeapFileName(name);
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
        for (int i = 0; i < totalColumns; i++) {
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
            IOException {
        _fileName = getHeapFileName(name);
        HeaderFile = new Heapfile(_fileName);
        Scan headerFileScan = HeaderFile.openScan();
        RID emptyRID = new RID();
        Tuple colCountTuple = headerFileScan.getNext(emptyRID);
        if (colCountTuple == null){
            System.out.println("The header tuple is empty for table: " + name);
        }
        ValueIntClass columnCount = new ValueIntClass(colCountTuple.getTupleByteArray());
        numColumns = columnCount.value - 2;
        columnNames = new String[numColumns];
        columnFile = new Heapfile[columnCount.value];
        headerRIDs = new RID[columnCount.value];
        indexType = new IndexType[numColumns];
        offsets = new int[numColumns];
        type = new AttrType[numColumns];

        for (int i = 0; i < columnCount.value; i++){
            RID tupleRID = new RID();
            colCountTuple = headerFileScan.getNext(tupleRID);
            //Storing the RID that was filled in by getNext
            headerRIDs[i] = tupleRID;
            byte[] colData = colCountTuple.getTupleByteArray();

            byte[] StringArr = new byte[colData.length - 8];
            System.arraycopy(colData, 0, StringArr, 0, StringArr.length);
            String colName = new String(StringArr).trim();

            int colType = Convert.getIntValue(colData.length - 12, colData);
            int colOffset = Convert.getIntValue(colData.length - 8, colData);
            int index = Convert.getIntValue(colData.length - 4, colData);

            if (i < numColumns) {
                offsets[i] = colOffset;
                type[i] = new AttrType(colType);
                indexType[i] = new IndexType(index);
                columnNames[i] = colName.split("\\.")[1];
            }
            columnFile[i] = new Heapfile(colName);
        }
        headerFileScan.closescan();
    }

    public void deleteColumnarFile()
            throws InvalidSlotNumberException,
            FileAlreadyDeletedException,
            InvalidTupleSizeException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException {

        _file_deleted = true;

        for (int i = 0; i < numColumns; i++) {
            try {
                columnFile[i].deleteFile();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void setAttrOffset(int[] offsetArr)
            throws Exception {
        if (offsets.length != offsetArr.length) {
            throw new Exception("Offset array length is not correct");
        } else {
            offsets = offsetArr.clone();
        }
    }

    public int getTuplePosition(int pageNo, int slotNo)
        throws Exception
    {
        RID lastRID = new RID(new PageId(pageNo), slotNo - 1);
        TID lastEntryTID = deserializeTuple(columnFile[numColumns + 1].getRecord(lastRID).getTupleByteArray());
        return lastEntryTID.position;
    }

    public int getInsertPosition(TID tid, int batchInsertToken)
        throws Exception
    {
        int position = 0;
        if ((batchInsertToken == currBatchInsertToken) && (tid.recordIDs[numColumns + 1].pageNo.pid == currPageNo)){
            position = ((tid.recordIDs[numColumns + 1].slotNo - 1 == currSlotno) ? currPosition : getTuplePosition(currPageNo, currSlotno)) + 1;
        } else {
            position = columnFile[numColumns + 1].RidToPos(tid.recordIDs[numColumns + 1], this);
        }
        currBatchInsertToken = batchInsertToken;
        currPosition = position;
        currSlotno = tid.recordIDs[numColumns + 1].slotNo;
        currPageNo = tid.recordIDs[numColumns + 1].pageNo.pid;
        return currPosition;
    }

    public TID insertTuple(byte[] tupleptr)
            throws SpaceNotAvailableException,
            InvalidSlotNumberException,
            InvalidTupleSizeException,
            SpaceNotAvailableException,
            HFException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException,
            Exception{
        if (tupleptr.length >= MAX_SPACE) {
            throw new SpaceNotAvailableException(null, "Columnarfile: no available space");
        }
        KeyClass[] keyArr = new KeyClass[numColumns];
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
              if (indexType[i].indexType == 1){
                  //Btree Index
                  keyArr[i] = new IntegerKey(intAttr);
              }
          }
          if (attr.attrType == AttrType.attrString) {
            //insert type String
            String strAttr = Convert.getStrValue(offset,tupleptr,offsets[i]);
            offset = offset + offsets[i];

            byte[] strValue = new byte[offsets[i] + 2];
            Convert.setStrValue(strAttr, 0, strValue);
            tid.recordIDs[i] = columnFile[i].insertRecord(strValue);
              if (indexType[i].indexType == 1){
                  //Btree Index
                  keyArr[i] = new StringKey(strAttr);
              }
          }
          i++;
        }
        ValueIntClass newRow = new ValueIntClass(0);
        tid.recordIDs[numColumns] = columnFile[numColumns].insertRecord(newRow.getByteArr());
        tid.numRIDs = i;
        tid.recordIDs[numColumns + 1] = columnFile[numColumns + 1].insertRecord(serializeTuple(tid));

        for (int j = 0; j < numColumns; j++){
            if (indexType[j].indexType == 1){
                try {
                    String indexFileName = _fileName + "." + String.valueOf(j) + ".Btree";
                    //Setting the delete fashion to 1 which seems to be the default
                    BTreeFile btree = new BTreeFile(indexFileName);
                    btree.insert(keyArr[j], tid.recordIDs[numColumns + 1]);
                }
                catch (Exception ex){
                    ex.printStackTrace();
                }
            }
        }
        tid.position = columnFile[numColumns + 1].RidToPos(tid.recordIDs[numColumns + 1], this);
        byte[] arr = serializeTuple(tid);
        updateColumnofTuple(tid, new Tuple(arr, 0, arr.length), numColumns + 1);
        return tid;
    }

    public TID insertTuple(byte[] tupleptr, int batchInsertToken)
            throws SpaceNotAvailableException,
            InvalidSlotNumberException,
            InvalidTupleSizeException,
            SpaceNotAvailableException,
            HFException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException,
            Exception{
        if (tupleptr.length >= MAX_SPACE) {
            throw new SpaceNotAvailableException(null, "Columnarfile: no available space");
        }
        KeyClass[] keyArr = new KeyClass[numColumns];
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
                if (indexType[i].indexType == 1){
                    //Btree Index
                    keyArr[i] = new IntegerKey(intAttr);
                }
            }
            if (attr.attrType == AttrType.attrString) {
                //insert type String
                String strAttr = Convert.getStrValue(offset,tupleptr,offsets[i]);
                offset = offset + offsets[i];

                byte[] strValue = new byte[offsets[i] + 2];
                Convert.setStrValue(strAttr, 0, strValue);
                tid.recordIDs[i] = columnFile[i].insertRecord(strValue);
                if (indexType[i].indexType == 1){
                    //Btree Index
                    keyArr[i] = new StringKey(strAttr);
                }
            }
            i++;
        }
        ValueIntClass newRow = new ValueIntClass(0);
        tid.recordIDs[numColumns] = columnFile[numColumns].insertRecord(newRow.getByteArr());
        tid.numRIDs = i;
        tid.recordIDs[numColumns + 1] = columnFile[numColumns + 1].insertRecord(serializeTuple(tid));

        for (int j = 0; j < numColumns; j++){
            if (indexType[j].indexType == 1){
                try {
                    String indexFileName = _fileName + "." + String.valueOf(j) + ".Btree";
                    //Setting the delete fashion to 1 which seems to be the default
                    BTreeFile btree = new BTreeFile(indexFileName);
                    btree.insert(keyArr[j], tid.recordIDs[numColumns + 1]);
                }
                catch (Exception ex){
                    ex.printStackTrace();
                }
            }
        }
        tid.position = getInsertPosition(tid, batchInsertToken);//columnFile[numColumns + 1].RidToPos(tid.recordIDs[numColumns + 1], this);
        byte[] arr = serializeTuple(tid);
        updateColumnofTuple(tid, new Tuple(arr, 0, arr.length), numColumns + 1);
        return tid;
    }


    public Tuple getTuple(TID tid)
            throws InvalidSlotNumberException,
            InvalidTupleSizeException,
            HFException,
            HFDiskMgrException,
            HFBufMgrException,
            Exception {
        //Tuple[] tupleArr = new Tuple[numColumns];
        try {
            Tuple tupleArr;
            int totalLength = 0;
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            for (int i = 0; i < numColumns; i++) {

                tupleArr = columnFile[i].getRecord(tid.recordIDs[i]);
                if (tupleArr == null) {
                    System.out.printf("There is an issue");
                }
                totalLength += tupleArr.getLength();
                outputStream.write(tupleArr.getTupleByteArray());
            }
            return new Tuple(outputStream.toByteArray(), 0, totalLength);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;

    }

    public int getTupleCnt()
            throws InvalidSlotNumberException,
            InvalidTupleSizeException,
            HFDiskMgrException,
            HFBufMgrException,
            IOException {
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
            Exception {
        Tuple tupleArr = columnFile[column].getRecord(tid.recordIDs[column]);
        ValueClass colVal;
        switch (type[column].attrType) {
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
            IOException {
        return columnFile[columnNo].openScan();
    }

    public boolean updateTuple(TID tid, Tuple newtuple)
            throws InvalidSlotNumberException,
            InvalidUpdateException,
            InvalidTupleSizeException,
            HFException,
            HFDiskMgrException,
            HFBufMgrException,
            Exception {
        byte[] arr;
        ValueClass updateValue;
        boolean retValue = true;
        //int offset = 0; //The starting location of each column
        byte[] data;
        int totalOffset = 0;
        for (int i = 0; i < numColumns; i++) {
            AttrType attr = type[i];
            switch (attr.attrType) {
                case AttrType.attrString:
                    data = new byte[offsets[i] + 2];
                    System.arraycopy(newtuple.getTupleByteArray(), totalOffset, data, 0, offsets[i]);
                    totalOffset += (offsets[i] + 2);
                    //updateValue = new ValueStrClass(data);
                    break;
                case AttrType.attrInteger:
                    data = new byte[offsets[i]];
                    System.arraycopy(newtuple.getTupleByteArray(), totalOffset, data, 0, offsets[i]);
                    totalOffset += offsets[i];
                    //updateValue = new ValueIntClass(data);
                    break;
                default:
                    throw new Exception("Unexpected AttrType" + type[i].toString());
            }

            //arr = updateValue.getByteArr();
            retValue &= updateColumnofTuple(tid, new Tuple(data, 0, data.length), i);
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
            Exception {
        return columnFile[column].updateRecord(tid.recordIDs[column], newtuple);

    }

    /*
     Helper function to be called when an index is created or update on a column
    */
    public void updateIndexType(int column, int index)
            throws Exception {
        indexType[column] = new IndexType(index);
        String columnName = _fileName.replace(".hdr", "") + "." + columnNames[column]; //_fileName + "." + String.valueOf(column);
        byte[] headerUpdateArr = _getColumnHeaderInsertTuple(columnName, type[column].attrType, offsets[column], index);
        HeaderFile.updateRecord(headerRIDs[column], new Tuple(headerUpdateArr, 0, headerUpdateArr.length));
    }

    public boolean createBTreeIndex(int column)
            throws GetFileEntryException,
            ConstructPageException,
            IOException,
            AddFileEntryException {
        try {
            String indexFileName = _fileName + "." + String.valueOf(column) + ".Btree";

            //Setting the delete fashion to 1 which seems to be the default
            BTreeFile btree = new BTreeFile(indexFileName, type[column].attrType, offsets[column], 1);
            TupleScan cfs = new TupleScan(this);
            TID emptyTID = new TID(numColumns + 2);
            Tuple dataTuple = cfs.getNextInternal(emptyTID);
            while (dataTuple != null) {
                int offset = 0;
                KeyClass key;
                for (int i = 0; i < column; i++) {
                    offset += (type[i].attrType == AttrType.attrString) ? offsets[i] + 2 : offsets[i];
                }
                int tempOffset = (type[column].attrType == AttrType.attrString) ? offsets[column] + 2 : offsets[column];
                byte[] dataArr = new byte[tempOffset];
                System.arraycopy(dataTuple.getTupleByteArray(), offset, dataArr, 0, tempOffset);
                switch (type[column].attrType) {
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
                emptyTID = new TID(numColumns + 2);
                dataTuple = cfs.getNextInternal(emptyTID);
            }
            updateIndexType(column, 1);
            cfs.closeTupleScan();
            btree.close();
        }
        catch (Exception ex){
            ex.printStackTrace();
            return false;
        }
        return true;
    }

    public boolean createBitMapIndex(int column, ValueClass value) throws ConstructPageException, IOException, GetFileEntryException, AddFileEntryException {


        // get unique values
        // create column for each unique value
        // set each column value for the row
        //
        // a cursor begins with the first value it finds, pushes to a value list (head)
        // pushes 1 and value to bitmap vector 1
        // if a new value is found
        // pushes 0 and pushes value to value list queue
        // if same value as current push 1 and continue
        // once we have gone through all records
        // start over, and pop value from value list
        // continue until all values have been created

        BitMapFile bitMapFile = null;

        try {
            String indexFileName = getBitMapIndexFileName(_fileName, column);
            bitMapFile = new BitMapFile(indexFileName, this, column, value);

            bitMapFile.initCursor();
            LinkedList<Object> linkedList = new LinkedList<>();
            HashMap<Object, ValueClass> hashMap = new HashMap<>();

            do {
                TupleScan cfs = new TupleScan(this);
                TID emptyTID = new TID(numColumns + 2);
                Tuple dataTuple = cfs.getNextInternal(emptyTID);
                while (dataTuple != null) {
                    int offset = 0;
                    KeyClass key;
                    for (int i = 0; i < column; i++) {
                        offset += (type[i].attrType == AttrType.attrString) ? offsets[i] + 2 : offsets[i];
                    }
                    int tempOffset = (type[column].attrType == AttrType.attrString) ? offsets[column] + 2 : offsets[column];
                    byte[] dataArr = new byte[tempOffset];
                    System.arraycopy(dataTuple.getTupleByteArray(), offset, dataArr, 0, tempOffset);
                    switch (type[column].attrType) {
                        case AttrType.attrString:
                            ValueStrClass st = new ValueStrClass(dataArr);
                            key = new StringKey(st.value);

                            if (linkedList.isEmpty()) {
                                linkedList.add(st.value);
                                hashMap.put(st.value, st);
                                bitMapFile.setCursorUniqueValue(st);
                            }
                            // does the value, match the current value being iterated on?
                            // if same value as current push 1 and continue
                            if (linkedList.peek().equals(st.value)) {
                                bitMapFile.cursorInsert(true);
                            } else {
                                bitMapFile.cursorInsert(false);
                            }
                            if (!hashMap.containsKey(st.value)) {
                                linkedList.add(st.value);
                                hashMap.put(st.value, st);
                            }
                            // if value is not the same, see if it is already in the list
                            // if its already in the list, populate 0
                            // if it is not already in the list, add to list and populate 0


                            break;
                        case AttrType.attrInteger:
                            ValueIntClass it = new ValueIntClass(dataArr);
                            key = new IntegerKey(it.value);

                            // st.value
                            // insert string value here
                            if (linkedList.isEmpty()) {
                                linkedList.add(it.value);
                                hashMap.put(it.value, it);
                                bitMapFile.setCursorUniqueValue(it);
                            }
                            // does the value, match the current value being iterated on?
                            // if same value as current push 1 and continue
                            if (linkedList.peek().equals(it.value)) {
                                bitMapFile.cursorInsert(true);
                            } else {
                                bitMapFile.cursorInsert(false);
                            }
                            if (!hashMap.containsKey(it.value)) {
                                linkedList.add(it.value);
                                hashMap.put(it.value, it);
                            }
                            // if value is not the same, see if it is already in the list
                            // if its already in the list, populate 0
                            // if it is not already in the list, add to list and populate 0
                            break;
                        default:
                            throw new Exception("Unexpected AttrType" + type[column].toString());
                    }
                    emptyTID = new TID(numColumns + 2);
                    dataTuple = cfs.getNextInternal(emptyTID);
                }
                Object current = linkedList.removeFirst();// fifo queue https://stackoverflow.com/questions/9580457/fifo-class-in-java
                if (linkedList.size() != 0) {
                    bitMapFile.setCursorUniqueValue(hashMap.get(linkedList.peek()));
                }

                // iterate through all tuples for each unique value
                // link list maintains ordered list of unique values
                // hashmap ensures we do not insert duplicate values with O(1) time on the check
            } while (!linkedList.isEmpty());
            bitMapFile.cursorComplete();
            bitMapFile.close();
            updateIndexType(column, IndexType.BitMapIndex);

        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }

//        BM bm = new BM();
//        bm.printBitMap(bitMapFile.getHeaderPage());


        return true;
    }

    public static String getBitMapIndexFileName(String columnarFileName, int column) {
        String fileName = columnarFileName + "." + String.valueOf(column) + ".BitMap";
        return fileName;
    }

    public String getBitMapIndexFileName(int column) {
        return getBitMapIndexFileName(getFileName(), column);
    }

    public BitMapFile getBitMapIndexFile(int column){
        try {
            return new BitMapFile(getBitMapIndexFileName(column));
        } catch (Exception e) {
            throw new RuntimeException("Error getting bitmap file from columnar file by column: "+column);
        }
    }

    public boolean markTupleDeleted(TID tid)
            throws InvalidSlotNumberException,
            InvalidUpdateException,
            InvalidTupleSizeException,
            HFException,
            HFDiskMgrException,
            HFBufMgrException,
            Exception {
        ValueIntClass toDelete = new ValueIntClass(1);
        byte[] arr = toDelete.getByteArr();
        return columnFile[numColumns].updateRecord(tid.recordIDs[numColumns], new Tuple(arr, 0, arr.length));
    }

    public boolean purgeAllDeletedTuples()
            throws InvalidTupleSizeException, IOException, Exception {
        int deletionOffset = 0;
        for (int i = 0; i < offsets.length; i++) {
            deletionOffset += (type[i].attrType == AttrType.attrString) ? offsets[i] + 2 : offsets[i];
        }
        TupleScan tsc = new TupleScan(this);
        Tuple DataTuple;
        boolean succefullDeletion = true;
        TID tid = new TID(numColumns + 2);
        ArrayList<TID> toDelete = new ArrayList<TID>();
        DataTuple = tsc.getNextInternal(tid);
        while (DataTuple != null) {
            int deletitionBit = Convert.getIntValue(deletionOffset, DataTuple.getTupleByteArray());
            if (deletitionBit == 1) {
                int Totaloffset = 0;
                for (int j = 0; j < numColumns + 2; j++) {
                    if (j < numColumns && indexType[j].indexType != 0) {
                        //Delete the index
                        KeyClass key;
                        switch (indexType[j].indexType) {
                            case 1:
                                String indexFileName = _fileName + "." + String.valueOf(j) + ".Btree";
                                BTreeFile btree = new BTreeFile(indexFileName);
                                byte[] dataArr;
                                switch (type[j].attrType) {
                                    case AttrType.attrString:
                                        dataArr = new byte[offsets[j] + 2];
                                        System.arraycopy(DataTuple.getTupleByteArray(), Totaloffset, dataArr, 0, offsets[j]);
                                        ValueStrClass st = new ValueStrClass(dataArr);
                                        key = new StringKey(st.value);
                                        break;
                                    case AttrType.attrInteger:
                                        dataArr = new byte[offsets[j]];
                                        System.arraycopy(DataTuple.getTupleByteArray(), Totaloffset, dataArr, 0, offsets[j]);
                                        ValueIntClass it = new ValueIntClass(dataArr);
                                        key = new IntegerKey(it.value);
                                        break;
                                    default:
                                        throw new Exception("Unexpected AttrType" + type[j].toString());
                                }

                                succefullDeletion &= btree.Delete(key, tid.recordIDs[tid.recordIDs.length - 1]);
                                break;
                            default:
                                System.out.println("Index deletion not supported yet" + " " + indexType[j].indexType);
                        }
                    }
                    succefullDeletion &= columnFile[j].deleteRecord(tid.recordIDs[j]);
                    if (j < numColumns) {
                        Totaloffset += (type[j].attrType == AttrType.attrString) ? offsets[j] + 2 : offsets[j];
                    }

                }
                if (!succefullDeletion) {
                    System.out.println("Hello again from purge");
                    break;
                }
            }
            //Just to ensure that all the object reference don't map to the same last object
            tid = new TID(numColumns + 2);
            DataTuple = tsc.getNextInternal(tid);
        }
        //TODO: Remove index entries
        tsc.closeTupleScan();
        return succefullDeletion;
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
     *
     * @see bufmgr.pinPage
     */
    private void pinPage(PageId pageno, Page page, boolean emptyPage)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
        } catch (Exception e) {
            throw new HFBufMgrException(e, "Heapfile.java: pinPage() failed");
        }

    } // end of pinPage

    /**
     * short cut to access the unpinPage function in bufmgr package.
     *
     * @see bufmgr.unpinPage
     */
    private void unpinPage(PageId pageno, boolean dirty)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        } catch (Exception e) {
            throw new HFBufMgrException(e, "Heapfile.java: unpinPage() failed");
        }

    } // end of unpinPage

    private void freePage(PageId pageno)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.freePage(pageno);
        } catch (Exception e) {
            throw new HFBufMgrException(e, "Heapfile.java: freePage() failed");
        }

    } // end of freePage

    private PageId newPage(Page page, int num)
            throws HFBufMgrException {

        PageId tmpId = new PageId();

        try {
            tmpId = SystemDefs.JavabaseBM.newPage(page, num);
        } catch (Exception e) {
            throw new HFBufMgrException(e, "Heapfile.java: newPage() failed");
        }

        return tmpId;

    } // end of newPage

    private PageId get_file_entry(String filename)
            throws HFDiskMgrException {

        PageId tmpId = new PageId();

        try {
            tmpId = SystemDefs.JavabaseDB.get_file_entry(filename);
        } catch (Exception e) {
            throw new HFDiskMgrException(e, "Heapfile.java: get_file_entry() failed");
        }

        return tmpId;

    } // end of get_file_entry

    private void add_file_entry(String filename, PageId pageno)
            throws HFDiskMgrException {

        try {
            SystemDefs.JavabaseDB.add_file_entry(filename, pageno);
        } catch (Exception e) {
            throw new HFDiskMgrException(e, "Heapfile.java: add_file_entry() failed");
        }

    } // end of add_file_entry

    private void delete_file_entry(String filename)
            throws HFDiskMgrException {

        try {
            SystemDefs.JavabaseDB.delete_file_entry(filename);
        } catch (Exception e) {
            throw new HFDiskMgrException(e, "Heapfile.java: delete_file_entry() failed");
        }

    } // end of delete_file_entry


    public int getColumnIndexByName(String columnName) {
//        if(getColumnNames() != null){
//            return Arrays.asList(getColumnNames()).indexOf(columnName);
//        }
        for (int i = 0; i < columnNames.length; i++) {
            if (columnNames[i].trim().equals(columnName.trim())) {
                return i;
            }
        }
        return -1;
    }

    public ValueClass getColumnTypeByName(String columnName) {
        int columnIndexByName = getColumnIndexByName(columnName);
        if (columnIndexByName != -1) {
            AttrType attrType = type[columnIndexByName];
            if (attrType != null) {
                return attrType.getValueClass();
            }
        }
        return null;
    }

    public RID getRIDByPosition(int position, int column) throws InvalidTupleSizeException, IOException {
        int positionCur =0;

        Scan cfs = new Scan(columnFile[numColumns + 1]);
        TID emptyTID = new TID(numColumns + 2);
        RID emptyRID = new RID();
        Tuple dataTuple = cfs.getNext(emptyRID);
        while (dataTuple != null) {
            emptyTID = deserializeTuple(dataTuple.getTupleByteArray());
            if(position == emptyTID.position){
                return emptyTID.recordIDs[column];
            }
            dataTuple = cfs.getNext(emptyRID);
        }
        return null;
    }

    public TID getTIDByPosition(int position) throws InvalidTupleSizeException, IOException {
        Scan cfs = new Scan(columnFile[numColumns + 1]);
        TID emptyTID = new TID(numColumns + 2);
        RID emptyRID = new RID();
        Tuple dataTuple = cfs.getNext(emptyRID);
        while (dataTuple != null) {
            emptyTID = deserializeTuple(dataTuple.getTupleByteArray());
            if(position == emptyTID.position){
                return emptyTID;
            }
            dataTuple = cfs.getNext(emptyRID);
        }
        return null;
    }
}