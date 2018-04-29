package bitmap;

import btree.*;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import columnar.Columnarfile;
import global.AttrType;
import global.ValueClass;
import global.ValueIntClass;
import global.ValueStrClass;
import heap.HFBufMgrException;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;

public class BitMapCreator {

    BitMapFile bitMapFile;
    LinkedList<Object> linkedList;
    HashMap<Object, ValueClass> hashMap;

    public BitMapCreator(String indexFileName, Columnarfile columnarfile, int column, ValueClass value) throws ConstructPageException, IOException, GetFileEntryException, AddFileEntryException {
        bitMapFile = new BitMapFile(indexFileName, columnarfile, column, value);
        bitMapFile.initCursor();
        linkedList = new LinkedList<>();
        hashMap = new HashMap<>();
    }

    public void push(int attrType, byte[] dataArr) throws Exception {
        KeyClass key;
        switch (attrType) {
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
                throw new Exception("Unexpected AttrType" + attrType);
        }
    }

    public boolean hasMore(){
        return !linkedList.isEmpty();
    }
    public void close() throws IOException, UnpinPageException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
        bitMapFile.cursorComplete();
        bitMapFile.close();
    }

    public void checkPoint() throws HFBufMgrException, UnpinPageException, IOException {
        Object current = linkedList.removeFirst();// fifo queue https://stackoverflow.com/questions/9580457/fifo-class-in-java
        if (linkedList.size() != 0) {
            bitMapFile.setCursorUniqueValue(hashMap.get(linkedList.peek()));
        }
    }

    public BitMapFile getBitMapFile() {
        return bitMapFile;
    }
}
