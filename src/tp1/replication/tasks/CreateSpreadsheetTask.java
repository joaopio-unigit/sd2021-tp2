package tp1.replication.tasks;

import tp1.api.Spreadsheet;

public class CreateSpreadsheetTask extends Task{

	private Spreadsheet sheet;
	
	
	public CreateSpreadsheetTask(Spreadsheet sheet) {
		this.sheet = sheet;
	}

	public Spreadsheet getSpreadsheet() {
		return sheet;
	}
}
