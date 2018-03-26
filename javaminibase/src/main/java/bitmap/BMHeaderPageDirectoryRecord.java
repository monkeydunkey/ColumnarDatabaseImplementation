package bitmap;

import global.*;

import java.io.IOException;

class BMHeaderPageDirectoryRecord {
    PageId bmPageId;
    ValueClass valueClass;

    public BMHeaderPageDirectoryRecord(PageId bmPageId, ValueClass valueClass) {
        this.bmPageId = bmPageId;
        this.valueClass = valueClass;
    }

    public PageId getBmPageId() {
        return bmPageId;
    }

    public void setBmPageId(PageId bmPageId) {
        this.bmPageId = bmPageId;
    }

    public ValueClass getValueClass() {
        return valueClass;
    }

    public void setValueClass(ValueClass valueClass) {
        this.valueClass = valueClass;
    }

    public byte[] getByteArray() throws IOException {
        int recordLength = 0;
        byte[] valueClassByteArr = valueClass.getByteArr();
        recordLength+= valueClassByteArr.length;
        recordLength+= 4; // bmPageId.pid size == 4

        byte[] data = new byte[recordLength];

        int offset = 0;
        Convert.setIntValue(bmPageId.pid, offset, data);

        if(valueClass instanceof ValueStrClass){
            Convert.setStrValue(((ValueStrClass) valueClass).value, offset+4, data);
        }else if (valueClass instanceof ValueIntClass){
            Convert.setIntValue(((ValueIntClass) valueClass).value, offset+4, data);
        }else{
            throw new RuntimeException("Value class was expected to be of type ValueStrClass, or ValueIntClass, but was not either");
        }




        return new byte[0];
    }
}
