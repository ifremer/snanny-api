package fr.ifremer.sensornanny.getdata.serverrestful.exception;

public class TooManyObservationsException extends Exception {
	private static final long serialVersionUID = 8831691552533242911L;
	
	public static final int LIMIT = 10000;

	private int rows;
	
	public TooManyObservationsException(int rows) {
		this.rows = rows;
	}
	
	public int getRows() {
		return rows;
	}
	
}
