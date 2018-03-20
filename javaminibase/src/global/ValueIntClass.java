package global;
import java.nio.ByteBuffer;

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
