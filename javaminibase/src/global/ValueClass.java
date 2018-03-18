package global;

public abstract class ValueClass extends java.lang.Object {
    public ValueClass() {
    }
}

public class ValueIntClass extends ValueClass {
	public int value = 0;
	public ValueIntClass(int val) {
		value = val;
	}
}

public class ValueStrClass extends ValueClass {
	public String value = "";
	public ValueIntClass(String val) {
		value = val;
	}
}
