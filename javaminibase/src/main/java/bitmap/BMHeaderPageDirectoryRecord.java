package bitmap;

import global.*;

import java.io.IOException;

class BMHeaderPageDirectoryRecord {
    PageId bmPageId;
    ValueClass valueClass;
    int arraySize;

    public BMHeaderPageDirectoryRecord(PageId bmPageId, ValueClass valueClass, int arraySize) {
        this.bmPageId = bmPageId;
        this.valueClass = valueClass;
        this.arraySize = arraySize;
    }

    public BMHeaderPageDirectoryRecord(byte[] data){
        try {
            int pid = Convert.getIntValue(0, data);
            bmPageId = new PageId(pid);
            arraySize = Convert.getIntValue(4, data);

            int strIntFlag = Convert.getIntValue(8, data);
            if(strIntFlag ==1){
                String strValue = Convert.getStrValue(12, data, data.length - 12);
                valueClass = new ValueStrClass(strValue);
            }else{
                int intValue = Convert.getIntValue(12, data);
                valueClass = new ValueIntClass(intValue);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
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
        recordLength+= 4; //
        recordLength+= 4; //

        byte[] data = new byte[recordLength];

        int offset = 0;
        Convert.setIntValue(bmPageId.pid, offset, data);
        Convert.setIntValue(arraySize, offset+4, data);

        if(valueClass instanceof ValueStrClass){
            Convert.setIntValue(1, offset+8, data);
            Convert.setStrValue(((ValueStrClass) valueClass).value, offset+12, data);
        }else if (valueClass instanceof ValueIntClass){
            Convert.setIntValue(0, offset+8, data);
            Convert.setIntValue(((ValueIntClass) valueClass).value, offset+12, data);
        }else{
            throw new RuntimeException("Value class was expected to be of type ValueStrClass, or ValueIntClass, but was not either");
        }

        return data;
    }
}
