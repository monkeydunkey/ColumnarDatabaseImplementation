package columnar;
import java.nio.ByteBuffer;

public abstract class ValueClass extends java.lang.Object {
    public ValueClass() {
    }
    public byte[] getByteArr(){

    }
}

public class ValueIntClass extends ValueClass {
    public int value = 0;
    public ValueIntClass(int val) {
        value = val;
    }

    public ValueIntClass(byte[] arr){
        ByteBuffer wrapped = ByteBuffer.wrap(arr); // big-endian by default
        value = wrapped.getInt(); // 1
    }

    public byte[] getByteArr(){
        //buffer Size for storing Int
        ByteBuffer dbuf = ByteBuffer.allocate(4);
        dbuf.putInt(value);
        return dbuf.array();
    }
}

public class ValueStrClass extends ValueClass {
    public String value = "";
    public ValueIntClass(String val) {
        value = val;
    }

    public ValueStrClass(byte[] arr){
        value = String(arr);
    }

    public byte[] getByteArr(){
        return value.getBytes();
    }
}

public class ValueRealClass extends ValueClass {
    public float value = "";
    public ValueRealClass(float val) {
        value = val;
    }

    public ValueRealClass(byte[] arr){
        ByteBuffer wrapped = ByteBuffer.wrap(arr); // big-endian by default
        value = wrapped.getFloat();
    }

    public byte[] getByteArr(){
        //buffer Size for storing Float
        ByteBuffer dbuf = ByteBuffer.allocate(4);
        dbuf.putFloat(value);
        return dbuf.array();
    }
}

//Assuming null so we have to value to work with so the byteArray is just empty
public class ValueNullClass extends ValueClass {
    public String value = null;
    public ValueNullClass(object val) {
    }

    public ValueNullClass(byte[] arr) {
    }

    public byte[] getByteArr(){
        return new byte[0];
    }
}