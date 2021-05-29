package tp1.google.replies;

public class GoogleSheetsReturn {
	private String range;
	private String majorDimension;
	private String[][] values;
	
	public GoogleSheetsReturn() {
		
	}
	
	public String[][] getValues(){
		return values;
	}
}
