package tp1.replication.tasks;

public class DeleteSpreadsheetTask extends Task {

	private String sheetId;
	
	public DeleteSpreadsheetTask(String sheetId) {
		this.sheetId = sheetId;
	}

	public String getSheetId() {
		return sheetId;
	}
}
