package tp1.replication.tasks;

import tp1.api.Spreadsheet;

public class CreateSpreadsheetTask extends Task{

	private Spreadsheet sheet;
	
	
	public CreateSpreadsheetTask(int sequenceNumber, Spreadsheet sheet) {
		super(sequenceNumber);
		this.sheet = sheet;
	}

	public Spreadsheet getSpreadsheet() {
		return sheet;
	}
}
