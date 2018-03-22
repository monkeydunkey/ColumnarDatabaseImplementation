package global;

public class ValueStrClass extends ValueClass {
	public String value = "";
	public ValueStrClass(String val) {
		value = val;
	}

	public ValueStrClass(byte[] arr){
		value = new String(arr);
	}

	public byte[] getByteArr(){
		return value.getBytes();
	}
}