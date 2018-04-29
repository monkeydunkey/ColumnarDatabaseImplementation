package bitmap;

import btree.*;
import columnar.Columnarfile;
import global.RID;
import global.ValueIntClass;
import global.ValueStrClass;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.SerializedScan;
import heap.Tuple;

import java.io.IOException;

public class EmptyBitMapFileScan extends BitMapFileScan {

    public EmptyBitMapFileScan() {}

    @Override
    public KeyDataEntry get_next() throws ScanIteratorException {
        return null;
    }


}
