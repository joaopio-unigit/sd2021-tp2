package tp1.replication.tasks;

public class DeleteUserSpreadsheetsTask extends Task{

	private String userId;

	public DeleteUserSpreadsheetsTask(int sequenceNumber, String userId) {
		super(sequenceNumber);
		this.userId = userId;
	}
	
	public String getUserId() {
		return userId;
	}
}
