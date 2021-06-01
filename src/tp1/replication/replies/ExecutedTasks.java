package tp1.replication.replies;

import java.util.List;

import tp1.replication.tasks.Task;

public class ExecutedTasks {
	private List<Task> executedTasks;
	
	public ExecutedTasks(List<Task> executedTasks) {
		this.executedTasks = executedTasks;
	}
	
	public List<Task> getExecutedTasks(){
		return executedTasks;
	}
}
