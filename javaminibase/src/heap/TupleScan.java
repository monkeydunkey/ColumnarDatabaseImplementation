package heap;

import global.TID;
import columnar.Columnarfile;
import columnar.Scan;
import columnar.Tuple;
import java.io.*;

public class TupleScan {
    
	private Scan[] scanList;
	private ColumnarFile tempClmnFile;
	
    public TupleScan(Columnarfile cf) throws InvalidTupleSizeException, IOException
    {
    	tempClmnFile = cf;
        scanList = new Scan[numColumns];
        for (int i=0; i < numColumns; i++) {
            scanList[i] = cf.columnFile[i].openScan();
        }
    }

    /**
     * Closes the TupleScan object
     */
    void closeTupleScan(){
        for (int i = 0; i < numColumns; i++) {
            scanList[i].closeScan();
        }
    }

    /**
     * Retrieve the next tuple in a sequential scan
     * @param tid
     * @return
     */
    Tuple getNext(TID tid) throws InvalidTupleSizeException, IOException
    {
        Tuple tupleArr;
        int totalLength = 0;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (int i = 0; i < numColumns; i++) {
            tupleArr = tempClmnFile.columnFile[i].getNext(tid.recordIDs[i]);
            totalLength += tupleArr.getLength();
            outputStream.write( tupleArr.getTupleByteArray());
        }
        return new Tuple(outputStream.toByteArray(), 0, totalLength);
   }

    /**
     * Position all scan cursors to the records with the given rids
     * @param tid
     * @return
     */
    boolean position(TID tid){
	    for (int i = 0; i < numColumns; i++) {
	    	if (scanList[i].position(tid.recordIDs[i]) == false)
	            return false;
	    }
	    return true;
    }
}