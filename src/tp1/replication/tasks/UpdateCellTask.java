package tp1.replication.tasks;

public class UpdateCellTask extends Task{

	private String sheetId, cell, rawValue;
	
	public UpdateCellTask(String sheetId, String cell, String rawValue) {
		this.sheetId = sheetId;
		this.cell = cell;
		this.rawValue = rawValue;
	}
	
	public String getSheetId() {
		return sheetId;
	}
	
	public String getCell() {
		return cell;
	}
	
	public String getRawValue() {
		return rawValue;
	}
}
