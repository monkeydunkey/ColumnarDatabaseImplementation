package global;

import java.util.Objects;

public class ValueStrClass extends ValueClass {
	public String value = "";
	public ValueStrClass(String val) {
		value = val;
	}

	public ValueStrClass(byte[] arr){
		value = new String(arr).trim();
	}

	public byte[] getByteArr(){
		return value.getBytes();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		ValueStrClass that = (ValueStrClass) o;
		return Objects.equals(value, that.value);
	}

	@Override
	public int hashCode() {

		return Objects.hash(value);
	}

	@Override
	public String toString() {
		return String.valueOf(value);
	}
}