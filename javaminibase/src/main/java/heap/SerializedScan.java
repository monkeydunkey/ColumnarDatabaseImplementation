package heap;

import global.Convert;
import global.RID;
import global.TID;
import columnar.Columnarfile;
import heap.Scan;
import heap.Tuple;
import java.io.*;
/**
 * Created by shashankbhushan on 4/15/18.
 */
public class SerializedScan {
    Scan[] scanList;
    Columnarfile tempClmnFile;
    int currScanPos = 0;
    public int columnVal;
    public SerializedScan(Columnarfile cf) throws InvalidTupleSizeException, IOException
    {
        tempClmnFile = cf;
        //+2 for deletion file and TID Encoding file
        scanList = new Scan[2];
        for (int i=0; i < 2; i++) {
            try {
                scanList[i] = cf.columnFile[Columnarfile.numColumns + i].openScan();
            }catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    public SerializedScan(Columnarfile cf, int column) throws InvalidTupleSizeException, IOException {
        this.columnVal = column;
        tempClmnFile = cf;
        //+2 for deletion file and TID Encoding file
        scanList = new Scan[2];
        for (int i = 0; i < 2; i++) {
            try {
                scanList[i] = cf.columnFile[Columnarfile.numColumns + i].openScan();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Closes the TupleScan object
     */
    public void closeTupleScan(){
        for (int i = 0; i < 2; i++) {
            try {
                scanList[i].closescan();
            }catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Retrieve the next tuple in a sequential scan
     * @param rid
     * @return
     */

    public RID getNextSerialized(int position) throws InvalidTupleSizeException, IOException
    {
        Tuple recptrtuple = null;
        Tuple tupleArr;
        Tuple delTupleArr;
        Tuple PosTuple;
        int totalLength = 0;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        //Rejecting the tuples marked for deletion
        RID deletionRowID = new RID();
        RID positionRowID = new RID();
        delTupleArr = scanList[0].getNext(deletionRowID);
        PosTuple = scanList[1].getNext(positionRowID);
        while ((delTupleArr != null && Convert.getIntValue(0, delTupleArr.getTupleByteArray()) == 1) ||
                (PosTuple != null && Convert.getIntValue(4, PosTuple.getTupleByteArray()) < position))
        {
            delTupleArr = scanList[0].getNext(deletionRowID);
            PosTuple = scanList[1].getNext(positionRowID);
        }
        if (delTupleArr != null && PosTuple != null && Convert.getIntValue(4, PosTuple.getTupleByteArray()) == position){
            return positionRowID;//tempClmnFile.deserializeTuple(PosTuple.getTupleByteArray());
        }
        else{
            return null;
        }
    }

    public TID getNextSerialized() throws InvalidTupleSizeException, IOException
    {
        Tuple recptrtuple = null;
        Tuple tupleArr;
        Tuple delTupleArr;
        Tuple PosTuple;
        int totalLength = 0;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        //Rejecting the tuples marked for deletion
        RID deletionRowID = new RID();
        RID positionRowID = new RID();
        delTupleArr = scanList[0].getNext(deletionRowID);
        PosTuple = scanList[1].getNext(positionRowID);
        while ((delTupleArr != null && Convert.getIntValue(0, delTupleArr.getTupleByteArray()) == 1))
        {
            delTupleArr = scanList[0].getNext(deletionRowID);
            PosTuple = scanList[1].getNext(positionRowID);
        }
        if (delTupleArr != null && PosTuple != null){
            return tempClmnFile.deserializeTuple(PosTuple.getTupleByteArray());
        }
        else{
            return null;
        }
    }

}
