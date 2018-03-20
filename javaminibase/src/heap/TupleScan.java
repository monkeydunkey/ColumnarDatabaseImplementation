package heap;

import global.TID;
import columnar.Columnarfile;
import heap.Scan;
import heap.Tuple;
import java.io.*;

public class TupleScan {
    
	Scan[] scanList;
	Columnarfile tempClmnFile;
	
    public TupleScan(Columnarfile cf) throws InvalidTupleSizeException, IOException
    {
    	tempClmnFile = cf;
        scanList = new Scan[Columnarfile.numColumns];
        for (int i=0; i < Columnarfile.numColumns; i++) {
            scanList[i] = cf.columnFile[i].openScan();
        }
    }

    /**
     * Closes the TupleScan object
     */
    void closeTupleScan(){
        for (int i = 0; i < Columnarfile.numColumns; i++) {
            scanList[i].closescan();
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
        for (int i = 0; i < Columnarfile.numColumns; i++) {
            tupleArr = scanList[i].getNext(tid.recordIDs[i]);
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
    boolean position(TID tid)throws InvalidTupleSizeException, IOException{
	    for (int i = 0; i < Columnarfile.numColumns; i++) {
	    	try {
	    		if (scanList[i].position(tid.recordIDs[i]) == false)
		            return false;
	    	}catch(Exception e) {
	    	 	e.printStackTrace();
	    	}
	    }
	    return true;
    }
}