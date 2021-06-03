package tp1.replication;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;
import java.util.concurrent.atomic.*;

import javax.net.ssl.HttpsURLConnection;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;

import com.google.gson.Gson;

import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import tp1.api.Spreadsheet;
import tp1.api.service.rest.ReplicationRestSpreadsheets;
import tp1.api.service.rest.RestSpreadsheets;
import tp1.replication.json.ExecutedTasks;
import tp1.replication.tasks.Task;
import tp1.server.rest.replication.ReplicationSpreadsheetsServer;
import tp1.util.InsecureHostnameVerifier;
import tp1.util.ZookeeperProcessor;

public class ReplicationManager {

	private final static int CONNECTION_TIMEOUT = 10000;
	private final static int REPLY_TIMEOUT = 10000;
	private static final String ZOO_ERROR = "Error on instantiating Zookeeper.";
	private static final String ZOOKEEPER_HOSTPORT = "kafka:2181";
	private static final String WAITING_FOR_SECONDARY = "Waiting for a secondary server acknowledge.";
	private static final String THREAD_NULL = "THREAD: response is null.";
	private static final String THREAD_STATUS = "THREAD: response status ";
	private static final String THREAD_CONNECTION_TIMEOUT = "THREAD: connection timed out.";
	private static final String PRIMARY_UPDATE_SUCCESS = "New primary server notified.";
	private static final String PRIMARY_UPDATE_FAILURE = " Failed to notify new primary server.";
	private static final String PRIMARY_SERVER = "PRIMARY SERVER URL: ";
	private static final String CREATED_NODE = "Created znode: ";
	private static final String CREATED_CHILD_NODE = "Created child znode: ";
	private static final String REPLICA_NODE_NAME = "replica";
	// APAGAR
	private static final String ACK_OUTPUT = "One of the secundary servers acknowledged.";

	private static Logger Log = Logger.getLogger(ReplicationManager.class.getName());

	private static ReplicationManager instance;
	private ZookeeperProcessor zk;

	private Client client;
	private String primaryServerURL;
	private List<String> existingServers;

	private List<Task> tasks;
	private AtomicLong globalVersionNumber;
	private Gson json;

	// APAGAR
	// String znodePATHTOIGNORE;

	synchronized public static ReplicationManager getInstance() {
		if (instance == null)
			instance = new ReplicationManager();

		return instance;
	}

	private ReplicationManager() {
		primaryServerURL = null;

		tasks = new ArrayList<Task>();
		globalVersionNumber = new AtomicLong(0L);

		client = createClient();

		json = new Gson();
	}

	public boolean isPrimary(String serverURL) {
		return primaryServerURL.equals(serverURL);
	}

	public String getPrimaryServerURL() {
		return primaryServerURL;
	}

	// EXECUCAO DOS PEDIDOS

	public void createSpreadsheet(Spreadsheet sheet, Long version) {
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());
		AtomicInteger numberOfAcks = new AtomicInteger(0);

		for (String serverURL : existingServers) {
			if (!serverURL.equals(primaryServerURL)) {
				// if (!serverURL.equals(primaryServerURL) &&
				// !znodePATHTOIGNORE.equals(serverURL)) {
				String createSpreadsheetURL = serverURL + RestSpreadsheets.PATH + ReplicationRestSpreadsheets.OPERATION;
				WebTarget target = client.target(createSpreadsheetURL).queryParam("repSecret", ReplicationSpreadsheetsServer.replicationSecret);
				new Thread(() -> {
					try {
						Response r = target.request().header(RestSpreadsheets.HEADER_VERSION, version)
								.accept(MediaType.APPLICATION_JSON)
								.post(Entity.entity(sheet, MediaType.APPLICATION_JSON));

						if (r != null && isSuccessful(r)) {
							numberOfAcks.incrementAndGet();
						} else {
							if (r == null)
								System.out.println(THREAD_NULL);
							else {
								System.out.println(THREAD_STATUS + r.getStatus());
							}
						}
					} catch (ProcessingException pe) {
						numberOfAcks.getAndIncrement();
						System.out.println(THREAD_CONNECTION_TIMEOUT);
					}
				}).start();
			}
		}

		while (numberOfAcks.get() == 0 && existingServers.size() > 1) { // ESPERAR ATE RECEBER UM ACK
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			System.out.println(WAITING_FOR_SECONDARY);
		}

		System.out.println(ACK_OUTPUT);
	}

	public void deleteSpreadsheet(String sheetId, Long version) {
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());
		AtomicInteger numberOfAcks = new AtomicInteger(0);

		for (String serverURL : existingServers) {

			if (!serverURL.equals(primaryServerURL)) {
				String deleteSpreadsheetURL = serverURL + RestSpreadsheets.PATH;
				WebTarget target = client.target(deleteSpreadsheetURL).path(sheetId)
						.path(ReplicationRestSpreadsheets.OPERATION).queryParam("repSecret", ReplicationSpreadsheetsServer.replicationSecret);
				new Thread(() -> {
					try {
						Response r = target.request().header(RestSpreadsheets.HEADER_VERSION, version)
								.accept(MediaType.APPLICATION_JSON).delete();

						if (r != null && isSuccessful(r)) {
							numberOfAcks.incrementAndGet();
						} else {
							if (r == null)
								System.out.println(THREAD_NULL);
							else {
								System.out.println(THREAD_STATUS + r.getStatus());
							}
						}
					} catch (ProcessingException pe) {
						numberOfAcks.getAndIncrement();
						System.out.println(THREAD_CONNECTION_TIMEOUT);
					}
				}).start();
			}
		}

		while (numberOfAcks.get() == 0 && existingServers.size() > 1) { // ESPERAR ATE RECEBER UM ACK
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			System.out.println(WAITING_FOR_SECONDARY);
		}
		System.out.println(ACK_OUTPUT);

	}

	public void updateCell(String sheetId, String cell, String rawValue, Long version) {
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());
		AtomicInteger numberOfAcks = new AtomicInteger(0);

		for (String serverURL : existingServers) {

			if (!serverURL.equals(primaryServerURL)) {
				String updateCellURL = serverURL + RestSpreadsheets.PATH;
				WebTarget target = client.target(updateCellURL).path(sheetId).path(cell)
						.path(ReplicationRestSpreadsheets.OPERATION).queryParam("repSecret", ReplicationSpreadsheetsServer.replicationSecret);
				new Thread(() -> {
					try {
						Response r = target.request().header(RestSpreadsheets.HEADER_VERSION, version)
								.accept(MediaType.APPLICATION_JSON)
								.put(Entity.entity(rawValue, MediaType.APPLICATION_JSON));

						if (r != null && isSuccessful(r)) {
							numberOfAcks.incrementAndGet();
						} else {
							if (r == null)
								System.out.println(THREAD_NULL);
							else {
								System.out.println(THREAD_STATUS + r.getStatus());
							}
						}
					} catch (ProcessingException pe) {
						numberOfAcks.getAndIncrement();
						System.out.println(THREAD_CONNECTION_TIMEOUT);
					}
				}).start();
			}
		}

		while (numberOfAcks.get() == 0 && existingServers.size() > 1) { // ESPERAR ATE RECEBER UM ACK
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			System.out.println(WAITING_FOR_SECONDARY);
		}
		System.out.println(ACK_OUTPUT);

	}

	public void shareSpreadsheet(String sheetId, String userId, Long version) {

		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());
		AtomicInteger numberOfAcks = new AtomicInteger(0);

		for (String serverURL : existingServers) {

			if (!serverURL.equals(primaryServerURL)) {
				String shareSpreadsheetURL = serverURL + RestSpreadsheets.PATH;
				WebTarget target = client.target(shareSpreadsheetURL).path(sheetId).path("share").path(userId)
						.path(ReplicationRestSpreadsheets.OPERATION).queryParam("repSecret", ReplicationSpreadsheetsServer.replicationSecret);
				new Thread(() -> {
					try {
						Response r = target.request().header(RestSpreadsheets.HEADER_VERSION, version)
								.accept(MediaType.APPLICATION_JSON).post(null);

						if (r != null && isSuccessful(r)) {
							numberOfAcks.incrementAndGet();
						} else {
							if (r == null)
								System.out.println(THREAD_NULL);
							else {
								System.out.println(THREAD_STATUS + r.getStatus());
							}
						}
					} catch (ProcessingException pe) {
						numberOfAcks.getAndIncrement();
						System.out.println(THREAD_CONNECTION_TIMEOUT);
					}
				}).start();
			}
		}

		while (numberOfAcks.get() == 0 && existingServers.size() > 1) { // ESPERAR ATE RECEBER UM ACK
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			System.out.println(WAITING_FOR_SECONDARY);
		}
		System.out.println(ACK_OUTPUT);

	}

	public void unshareSpreadsheet(String sheetId, String userId, Long version) {
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());
		AtomicInteger numberOfAcks = new AtomicInteger(0);

		for (String serverURL : existingServers) {

			if (!serverURL.equals(primaryServerURL)) {
				String unshareSpreadsheetURL = serverURL + RestSpreadsheets.PATH;
				WebTarget target = client.target(unshareSpreadsheetURL).path(sheetId).path("share").path(userId)
						.path(ReplicationRestSpreadsheets.OPERATION).queryParam("repSecret", ReplicationSpreadsheetsServer.replicationSecret);
				new Thread(() -> {
					try {
						Response r = target.request().header(RestSpreadsheets.HEADER_VERSION, version)
								.accept(MediaType.APPLICATION_JSON).delete();

						if (r != null && isSuccessful(r)) {
							numberOfAcks.incrementAndGet();
						} else {
							if (r == null)
								System.out.println(THREAD_NULL);
							else {
								System.out.println(THREAD_STATUS + r.getStatus());
							}
						}
					} catch (ProcessingException pe) {
						numberOfAcks.getAndIncrement();
						System.out.println(THREAD_CONNECTION_TIMEOUT);
					}
				}).start();
			}
		}

		while (numberOfAcks.get() == 0 && existingServers.size() > 1) { // ESPERAR ATE RECEBER UM ACK
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			System.out.println(WAITING_FOR_SECONDARY);
		}
		System.out.println(ACK_OUTPUT);

	}

	public void deleteUserSpreadsheets(String userId, Long version) {
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());
		AtomicInteger numberOfAcks = new AtomicInteger(0);

		for (String serverURL : existingServers) {

			if (!serverURL.equals(primaryServerURL)) {
				String deleteUserSpreadsheetsURL = serverURL + RestSpreadsheets.PATH;
				WebTarget target = client.target(deleteUserSpreadsheetsURL)
						.path(ReplicationRestSpreadsheets.DELETESHEETS).path(userId)
						.path(ReplicationRestSpreadsheets.OPERATION).queryParam("repSecret", ReplicationSpreadsheetsServer.replicationSecret);
				new Thread(() -> {
					try {
						Response r = target.request().header(RestSpreadsheets.HEADER_VERSION, version)
								.accept(MediaType.APPLICATION_JSON).delete();

						if (r != null && isSuccessful(r)) {
							numberOfAcks.incrementAndGet();
						} else {
							if (r == null)
								System.out.println(THREAD_NULL);
							else {
								System.out.println(THREAD_STATUS + r.getStatus());
							}
						}
					} catch (ProcessingException pe) {
						numberOfAcks.getAndIncrement();
						System.out.println(THREAD_CONNECTION_TIMEOUT);
					}
				}).start();
			}
		}

		while (numberOfAcks.get() == 0 && existingServers.size() > 1) { // ESPERAR ATE RECEBER UM ACK
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			System.out.println(WAITING_FOR_SECONDARY);
		}
		System.out.println(ACK_OUTPUT);

	}

	public List<String[]> getExecutedTasks(int startingPos) {

		List<String[]> tasksJsonRepresentation = new ArrayList<String[]>(tasks.size());

		for (Task task : tasks) {
			Tasks taskType = Tasks.valueOf(task.getClass().getSimpleName());
			switch (taskType) {
			case CreateSpreadsheetTask:
				String[] createTask = new String[] { Tasks.CreateSpreadsheetTask.toString(), json.toJson(task) };
				tasksJsonRepresentation.add(createTask);
				break;
			case DeleteSpreadsheetTask:
				String[] deleteTask = new String[] { Tasks.DeleteSpreadsheetTask.toString(), json.toJson(task) };
				tasksJsonRepresentation.add(deleteTask);
				break;
			case DeleteUserSpreadsheetsTask:
				String[] deleteUserTask = new String[] { Tasks.DeleteUserSpreadsheetsTask.toString(),
						json.toJson(task) };
				tasksJsonRepresentation.add(deleteUserTask);
				break;
			case ShareSpreadsheetTask:
				String[] shareTask = new String[] { Tasks.ShareSpreadsheetTask.toString(), json.toJson(task) };
				tasksJsonRepresentation.add(shareTask);
				break;
			case UnshareSpreadsheetTask:
				String[] unshareTask = new String[] { Tasks.UnshareSpreadsheetTask.toString(), json.toJson(task) };
				tasksJsonRepresentation.add(unshareTask);
				break;
			case UpdateCellTask:
				String[] updateTask = new String[] { Tasks.UpdateCellTask.toString(), json.toJson(task) };
				tasksJsonRepresentation.add(updateTask);
				break;
			default:
				System.out.println("Type of task not recognized when converting to JSON");
				break;
			}

		}

		return tasksJsonRepresentation.subList(startingPos, tasksJsonRepresentation.size());
	}

	// GESTAO DE VERSAO

	synchronized public Long newTask(Task newTask) {
		tasks.add(newTask);

		Long taskAssignedVersion = globalVersionNumber.getAndIncrement();

		return taskAssignedVersion;
	}

	public Long getGlobalSequenceNumber() {
		return globalVersionNumber.get();
	}

	public List<String[]> getMissingTasks(int startingPos) {
		AtomicInteger numberOfAcks = new AtomicInteger(0);
		List<List<String[]>> updatedTasks = new LinkedList<List<String[]>>();

		for (String serverURL : existingServers) {
			if (!serverURL.equals(ReplicationSpreadsheetsServer.serverURL)) {
				String getTasksURL = serverURL + RestSpreadsheets.PATH + ReplicationRestSpreadsheets.TASKS;
				WebTarget target = client.target(getTasksURL).queryParam("startingPos", startingPos).queryParam("repSecret", ReplicationSpreadsheetsServer.replicationSecret);

				System.out.println("MAKING A TASK REQUEST TO " + getTasksURL);

				new Thread(() -> {
					try {
						Response r = target.request().accept(MediaType.APPLICATION_JSON).get();
												
						int ackNumber = numberOfAcks.getAndIncrement();

						System.out.println("THREAD: RECEBI RESPOSTA " + ackNumber);
						
						if (r != null && isSuccessful(r)) {
							System.out.println("RESPOSTA FOI SUCESSO");
							
							List<String[]> receivedTasks = json
									.fromJson(r.readEntity(String.class), ExecutedTasks.class).getExecutedTasks();
							//updatedTasks.add(ackNumber, receivedTasks);
							updatedTasks.add(receivedTasks);
						} else {
							if (r == null)
								System.out.println(THREAD_NULL);
							else {
								System.out.println(THREAD_STATUS + r.getStatus());
							}
						}

					} catch (ProcessingException pe) {
						numberOfAcks.getAndIncrement();
						System.out.println(THREAD_CONNECTION_TIMEOUT);
					}
				}).start();
			}
		}

		while (numberOfAcks.get() != existingServers.size() - 1) { // ESPERAR ATE RECEBER OS ACKS DE TODOS MENOS ELE
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			System.out.println(WAITING_FOR_SECONDARY);
		}

		List<String[]> mostUpdatedTasks = null;
		int mostUpdatedTasksNumber = 0;

		for (List<String[]> taskList : updatedTasks) {
			if (taskList.get(1).length > mostUpdatedTasksNumber) {
				mostUpdatedTasks = taskList;
				mostUpdatedTasksNumber = taskList.size();
			}
		}
		return mostUpdatedTasks;
	}

	// ZOOKEEPER

	public void startZookeeper() {
		try {
			zk = ZookeeperProcessor.getInstance(ZOOKEEPER_HOSTPORT);
		} catch (Exception e) {
			Log.info(ZOO_ERROR);
		}

		String domainZNode = "/" + ReplicationSpreadsheetsServer.spreadsheetsDomain;

		if (zk.write(domainZNode, CreateMode.PERSISTENT) != null) {
			System.out.println(CREATED_NODE + domainZNode);
		}

		String serverZNode = String.format("%s/%s_", domainZNode, REPLICA_NODE_NAME);

		// PASSAR O URL DO SERVIDOR NO NOME DO ZNODE
		String znodePath = zk.write(serverZNode, ReplicationSpreadsheetsServer.serverURL,
				CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println(CREATED_CHILD_NODE + znodePath);

		// LISTA COM ELEMENTOS COMO replica_00000000000000
		List<String> existingZnodes = zk.getChildren(domainZNode, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				List<String> existingZNodes = zk.getChildren(domainZNode, this);
				// ELEGER O PRIMARIO QUANDO HOUVER ALTERACOES
				primaryServerElection(domainZNode, znodePath, existingZNodes);
			}

		});

		// ELEGER O PRIMARIO QUANDO A CLASSE E INICIALIZADA
		primaryServerElection(domainZNode, znodePath, existingZnodes);
	}

	private void primaryServerElection(String domainZNode, String serverZNodePath, List<String> existingZNodes) {
		existingServers = new LinkedList<String>();

		String previousPrimaryServerURL = primaryServerURL;

		String primaryServerNode = existingZNodes.get(0);

		for (String znode : existingZNodes) {

			String znodePath = domainZNode + "/" + znode;
			String znodeURL = zk.getValue(znodePath);

			if (primaryServerNode.compareTo(znode) >= 0) {
				primaryServerNode = znode;
				primaryServerURL = znodeURL;
			}

			/*
			 * // APAGAR if (znode.contains("1")) { znodePATHTOIGNORE = znodeURL;
			 * System.out.println("ADICIONEI O NODE PARA IGNORAR: " + znodePATHTOIGNORE); }
			 */

			// OBTER OS URLS QUANDO OCORREM ALTERACOES OFERECE MELHOR DESEMPENHO
			existingServers.add(znodeURL);
		}

		// NOTIFICAR O SERVIDOR QUE E O PRIMARIO QUANDO EXISTEM OUTROS, HOUVE UMA
		// ALTERACAO E SO ELE FAZER A NOTIFICACAO
		if (previousPrimaryServerURL != null && !previousPrimaryServerURL.equals(primaryServerURL)
				&& primaryServerURL.equals(ReplicationSpreadsheetsServer.serverURL))
			primaryServerNotification();
	}

	private void primaryServerNotification() {
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());

		ClientConfig config = new ClientConfig();
		config.property(ClientProperties.CONNECT_TIMEOUT, CONNECTION_TIMEOUT);
		config.property(ClientProperties.READ_TIMEOUT, REPLY_TIMEOUT);
		Client client = ClientBuilder.newClient(config);
		WebTarget target;

		String serverNotificationURL = primaryServerURL + RestSpreadsheets.PATH + ReplicationRestSpreadsheets.PRIMARY;
		target = client.target(serverNotificationURL).queryParam("repSecret", ReplicationSpreadsheetsServer.replicationSecret);
		Response r = target.request().accept(MediaType.APPLICATION_JSON).post(null);

		if (r != null && r.getStatus() >= 200 && 300 > r.getStatus()) {
			Log.info(PRIMARY_UPDATE_SUCCESS);
		} else {
			Log.info(PRIMARY_UPDATE_FAILURE);
			/*
			 * System.out.println("URI A TENTAR CONTACTAR " + target.getUri().toString());
			 * if (r == null) System.out.println("R IS NULL"); else
			 * System.out.println("STATUS DO R " + r.getStatus());
			 */
		}
		System.out.println(PRIMARY_SERVER + primaryServerURL);
	}

	// METODOS PRIVADOS

	private Client createClient() {
		ClientConfig config = new ClientConfig();
		config.property(ClientProperties.CONNECT_TIMEOUT, CONNECTION_TIMEOUT);
		config.property(ClientProperties.READ_TIMEOUT, REPLY_TIMEOUT);
		return ClientBuilder.newClient(config);
	}

	private boolean isSuccessful(Response r) {
		if (r.getStatus() >= 200 && r.getStatus() < 300)
			return true;
		else
			return false;
	}
}
