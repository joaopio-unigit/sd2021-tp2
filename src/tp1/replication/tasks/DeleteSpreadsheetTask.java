package tp1.replication.tasks;

public class DeleteSpreadsheetTask extends Task {

	private String sheetId;
	
	public DeleteSpreadsheetTask(int sequenceNumber, String sheetId) {
		super(sequenceNumber);
		this.sheetId = sheetId;
	}

	public String getSheetId() {
		return sheetId;
	}
}
