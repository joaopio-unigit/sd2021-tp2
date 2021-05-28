package tp1.replication.tasks;

public class DeleteUserSpreadsheetsTask extends Task{

	private String userId;
	

	public DeleteUserSpreadsheetsTask(String userId) {
		this.userId = userId;
	}
	
	public String getUserId() {
		return userId;
	}
}
