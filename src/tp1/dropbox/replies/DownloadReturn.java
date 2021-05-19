package tp1.dropbox.replies;

import tp1.api.Spreadsheet;

public class DownloadReturn {

	private Spreadsheet sheet;
	
	public DownloadReturn() {
		sheet = null;
	}
	
	public void setSpreadsheet(Spreadsheet sheet) {
		this.sheet = sheet;
	}
	
	public Spreadsheet getSpreadsheet() {
		return sheet;
	}
}
