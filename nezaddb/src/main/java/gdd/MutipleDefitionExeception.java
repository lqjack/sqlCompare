package gdd;

public class MutipleDefitionExeception extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public MutipleDefitionExeception(Throwable throwable) {
		super(throwable);
	}

	public MutipleDefitionExeception(String string) {
		super(string);
	}

}
