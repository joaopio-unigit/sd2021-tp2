package tp1.google.replies;

public class GoogleSheetsReturn {
	@SuppressWarnings("unused")
	private String range;
	@SuppressWarnings("unused")
	private String majorDimension;
	private String[][] values;
	
	public GoogleSheetsReturn() {
		
	}
	
	public String[][] getValues(){
		return values;
	}
}
