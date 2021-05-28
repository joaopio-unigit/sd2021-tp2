package tp1.replication.tasks;

public class ShareSpreadsheetTask extends Task {

	private String sheetId;
	private String userId;
	
	public ShareSpreadsheetTask(String sheetId, String userId) {
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
