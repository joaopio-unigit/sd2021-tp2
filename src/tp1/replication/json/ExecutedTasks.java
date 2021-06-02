package tp1.replication.json;

import java.util.List;

public class ExecutedTasks {
	private List<String[]> executedTasks;
	
	public ExecutedTasks(List<String[]> executedTasks) {
		this.executedTasks = executedTasks;
	}
	
	public List<String[]> getExecutedTasks(){
		return executedTasks;
	}
}
