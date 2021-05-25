package tp1.replication.tasks;

public class ShareSpreadsheetTask extends Task {

	private String sheetId;
	private String userId;
	
	public ShareSpreadsheetTask(int sequenceNumber, String sheetId, String userId) {
		super(sequenceNumber);
		this.sheetId = sheetId;
		this.userId = userId;
	}
	
	public String getSheetId() {
		return sheetId;
	}
	
	public String getUserId() {
		return userId;
	}
}
