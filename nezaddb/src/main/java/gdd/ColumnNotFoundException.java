package gdd;

public class ColumnNotFoundException extends Exception {

	public ColumnNotFoundException(Throwable throwable) {
		super(throwable);
	}

	public ColumnNotFoundException(String string) {
		super(string);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

}
