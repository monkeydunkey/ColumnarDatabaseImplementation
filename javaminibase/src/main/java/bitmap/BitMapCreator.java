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
    LinkedList<Integer> typeLinkedList;
    HashMap<Object, String> hashMap;
    int maxPosition;

    public BitMapCreator(String indexFileName, Columnarfile columnarfile, int column, ValueClass value) throws ConstructPageException, IOException, GetFileEntryException, AddFileEntryException {
        bitMapFile = new BitMapFile(indexFileName, columnarfile, column, value);
        bitMapFile.initCursor();
        linkedList = new LinkedList<>();
        hashMap = new HashMap<>();
        typeLinkedList = new LinkedList<>();
        maxPosition = 0;
    }

    public void push(int attrType, byte[] dataArr, int postion) throws Exception {
        KeyClass key;
        maxPosition = (postion > maxPosition) ? postion : maxPosition;
        switch (attrType) {
            case AttrType.attrString:
                ValueStrClass st = new ValueStrClass(dataArr);
                key = new StringKey(st.value);
                /*
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
                */
                if (!hashMap.containsKey(st.value)) {
                    linkedList.add(st);
                    typeLinkedList.add(AttrType.attrString);
                    hashMap.put(st.value, String.valueOf(postion));

                } else {
                    hashMap.put(st.value, hashMap.get(st.value) + " " + String.valueOf(postion));
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
                /*
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
                */
                if (!hashMap.containsKey(it.value)) {
                    linkedList.add(it);
                    typeLinkedList.add(AttrType.attrInteger);
                    hashMap.put(it.value, String.valueOf(postion));
                } else {
                    hashMap.put(it.value, hashMap.get(it.value) + " " + String.valueOf(postion));
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
        //bitMapFile.cursorComplete();
        bitMapFile.close();
    }

    public void checkPoint() throws HFBufMgrException, UnpinPageException, IOException {
        while (linkedList.size() != 0){
            Object current = linkedList.removeFirst();// fifo queue https://stackoverflow.com/questions/9580457/fifo-class-in-java
            Integer type = typeLinkedList.removeFirst();
            if (AttrType.attrString == type){
                System.out.println("The encoding is:" + hashMap.get( ((ValueStrClass)current).value ) + " max Value: " + maxPosition);
                bitMapFile.setCursorUniqueValue((ValueClass)current, hashMap.get(((ValueStrClass)current).value), maxPosition);
            } else {
                System.out.println("The encoding is:" + hashMap.get( ((ValueIntClass)current).value ) + " max Value: " + maxPosition);
                bitMapFile.setCursorUniqueValue((ValueClass)current, hashMap.get(((ValueIntClass)current).value), maxPosition);
            }

        }

    }

    public BitMapFile getBitMapFile() {
        return bitMapFile;
    }
}
