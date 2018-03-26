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

    public BMHeaderPageDirectoryRecord(byte[] data){
        try {
            int pid = Convert.getIntValue(0, data);
            bmPageId = new PageId(pid);

            int strIntFlag = Convert.getIntValue(4, data);
            if(strIntFlag ==1){
                String strValue = Convert.getStrValue(8, data, data.length - 8);
                valueClass = new ValueStrClass(strValue);
            }else{
                int intValue = Convert.getIntValue(8, data);
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
        recordLength+= 4; // bmPageId.pid size == 4

        byte[] data = new byte[recordLength];

        int offset = 0;
        Convert.setIntValue(bmPageId.pid, offset, data);

        if(valueClass instanceof ValueStrClass){
            Convert.setIntValue(1, offset+4, data);
            Convert.setStrValue(((ValueStrClass) valueClass).value, offset+8, data);
        }else if (valueClass instanceof ValueIntClass){
            Convert.setIntValue(0, offset+4, data);
            Convert.setIntValue(((ValueIntClass) valueClass).value, offset+8, data);
        }else{
            throw new RuntimeException("Value class was expected to be of type ValueStrClass, or ValueIntClass, but was not either");
        }

        return data;
    }
}
