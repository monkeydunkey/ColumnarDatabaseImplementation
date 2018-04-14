package heap;

import global.Convert;
import global.RID;
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
		//+2 for deletion file and TID Encoding file
        scanList = new Scan[Columnarfile.numColumns + 2];
        for (int i=0; i < Columnarfile.numColumns + 2; i++) {
        	try {
        		scanList[i] = cf.columnFile[i].openScan();
        	}catch(Exception e) {
        		e.printStackTrace();
        	}
        }
    }

    /**
     * Closes the TupleScan object
     */
    public void closeTupleScan(){
        for (int i = 0; i < Columnarfile.numColumns + 2; i++) {
        	try {
        		scanList[i].closescan();
        	}catch(Exception e) {
        		e.printStackTrace();
        	}
        }
    }

    /**
     * Retrieve the next tuple in a sequential scan
     * @param tid
     * @return
     */
    public Tuple getNext(TID tid) throws InvalidTupleSizeException, IOException
    {
    	Tuple recptrtuple = null;
        Tuple tupleArr;
		Tuple delTupleArr;
        int totalLength = 0;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

		//Rejecting the tuples marked for deletion
		RID deletionRowID = new RID();
		delTupleArr = scanList[Columnarfile.numColumns].getNext(deletionRowID);
		while (delTupleArr != null && Convert.getIntValue(0, delTupleArr.getTupleByteArray()) == 1){
			for (int i = 0; i < Columnarfile.numColumns; i++) {
				try {
					tupleArr = scanList[i].getNext(tid.recordIDs[i]);
				}catch(Exception e) {
					e.printStackTrace();
				}
			}
			delTupleArr = scanList[Columnarfile.numColumns].getNext(deletionRowID);
		}
		for (int i = 0; i < Columnarfile.numColumns; i++) {
			try {
				tupleArr = scanList[i].getNext(tid.recordIDs[i]);
				if (tupleArr == null) break;
				totalLength += tupleArr.getLength();
				outputStream.write(tupleArr.getTupleByteArray());
			}catch(Exception e) {
				e.printStackTrace();
			}
		}

        if (totalLength == 0) {
        	return recptrtuple;
        }else {
        	return new Tuple(outputStream.toByteArray(), 0, totalLength);
        }
   }

	public Tuple getNextInternal(TID tid) throws InvalidTupleSizeException, IOException
	{
		Tuple recptrtuple = null;
		Tuple tupleArr;
		int totalLength = 0;
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		for (int i = 0; i < Columnarfile.numColumns + 2; i++) { // loop through column indexes
			try {
				RID recordID = tid.recordIDs[i];
				int recordPosition = scanList[i].getRecordPosition();
				tupleArr = scanList[i].getNext(recordID);
				System.out.println("recordId: "+recordID.toString() + " " + recordPosition);
				if (tupleArr == null) break;
				totalLength += tupleArr.getLength();
				outputStream.write( tupleArr.getTupleByteArray());
			}catch(Exception e) {
				e.printStackTrace();
				break;
			}
		}
		if (totalLength == 0) {
			return recptrtuple;
		}else {
			return new Tuple(outputStream.toByteArray(), 0, totalLength);
		}

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