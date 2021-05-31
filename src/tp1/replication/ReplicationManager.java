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

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import tp1.api.Spreadsheet;
import tp1.api.service.rest.ReplicationRestSpreadsheets;
import tp1.api.service.rest.RestSpreadsheets;
import tp1.replication.tasks.Task;
import tp1.server.rest.replication.ReplicationSpreadsheetsServer;
import tp1.util.InsecureHostnameVerifier;
import tp1.util.ZookeeperProcessor;

public class ReplicationManager {

	private final static int CONNECTION_TIMEOUT = 10000;
	private final static int REPLY_TIMEOUT = 1000;
	private static final String ZOO_ERROR = "Error on instantiating Zookeeper.";
	private static final String ZOOKEEPER_HOSTPORT = "kafka:2181";
	private static final String WAITING_FOR_SECONDARY = "Waiting for a secondary server acknowledge.";
	private static final String THREAD_NULL = "THREAD: response is null.";
	private static final String THREAD_STATUS = "THREAD: response status ";
	private static final String PRIMARY_UPDATE_SUCCESS = "New primary server notified.";
	private static final String PRIMARY_UPDATE_FAILURE =" Failed to notify new primary server.";
	private static final String PRIMARY_SERVER = "PRIMARY SERVER URL: ";
	private static final String CREATED_NODE = "Created znode: ";
	private static final String CREATED_CHILD_NODE = "Created child znode: ";
	private static final String REPLICA_NODE_NAME = "replica";
	
	private static Logger Log = Logger.getLogger(ReplicationManager.class.getName());

	private static ReplicationManager instance;
	private ZookeeperProcessor zk;

	private Client client;
	private String primaryServerURL;
	private List<String> existingServers;

	private List<Task> tasks;
	private AtomicLong globalVersionNumber;

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

		// for (int i = 0; i < existingServers.size(); i++) {
		for (String serverURL : existingServers) {
			// String serverURL = existingServers.get(i);

			if (!serverURL.equals(primaryServerURL)) {
				String createSpreadsheetURL = serverURL + RestSpreadsheets.PATH + ReplicationRestSpreadsheets.OPERATION;
				WebTarget target = client.target(createSpreadsheetURL);

				new Thread(() -> {
					Response r = target.request().header(RestSpreadsheets.HEADER_VERSION, version)
							.accept(MediaType.APPLICATION_JSON).post(Entity.entity(sheet, MediaType.APPLICATION_JSON));

					if (r != null && r.getStatus() == Status.OK.getStatusCode()) {
						numberOfAcks.incrementAndGet();
					} else {
						if (r == null)
							System.out.println(THREAD_NULL);
						else {
							System.out.println(THREAD_STATUS + r.getStatus());
						}
					}
				}).start();
				;
			}
		}

		while (numberOfAcks.get() == 0 && existingServers.size() > 1) { // ESPERAR ATE RECEBER UM ACK
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			System.out.println(WAITING_FOR_SECONDARY);
		}
	}

	public void deleteSpreadsheet(String sheetId, Long version) {
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());
		AtomicInteger numberOfAcks = new AtomicInteger(0);

		for (String serverURL : existingServers) {

			if (!serverURL.equals(primaryServerURL)) {
				String deleteSpreadsheetURL = serverURL + RestSpreadsheets.PATH;
				WebTarget target = client.target(deleteSpreadsheetURL).path(sheetId)
						.path(ReplicationRestSpreadsheets.OPERATION);

				new Thread(() -> {
					Response r = target.request().header(RestSpreadsheets.HEADER_VERSION, version)
							.accept(MediaType.APPLICATION_JSON).delete();

					if (r != null && r.getStatus() == Status.OK.getStatusCode()) {
						numberOfAcks.incrementAndGet();
					} else {
						if (r == null)
							System.out.println(THREAD_NULL);
						else {
							System.out.println(THREAD_STATUS + r.getStatus());
						}
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
	}

	public void updateCell(String sheetId, String cell, String rawValue, Long version) {
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());
		AtomicInteger numberOfAcks = new AtomicInteger(0);

		for (String serverURL : existingServers) {

			if (!serverURL.equals(primaryServerURL)) {
				String updateCellURL = serverURL + RestSpreadsheets.PATH;
				WebTarget target = client.target(updateCellURL).path(sheetId).path(cell)
						.path(ReplicationRestSpreadsheets.OPERATION);

				new Thread(() -> {
					Response r = target.request().header(RestSpreadsheets.HEADER_VERSION, version)
							.accept(MediaType.APPLICATION_JSON)
							.put(Entity.entity(rawValue, MediaType.APPLICATION_JSON));

					if (r != null && r.getStatus() == Status.OK.getStatusCode()) {
						numberOfAcks.incrementAndGet();
					} else {
						if (r == null)
							System.out.println(THREAD_NULL);
						else {
							System.out.println(THREAD_STATUS + r.getStatus());
						}
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
	}

	public void shareSpreadsheet(String sheetId, String userId, Long version) {
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());
		AtomicInteger numberOfAcks = new AtomicInteger(0);

		for (String serverURL : existingServers) {

			if (!serverURL.equals(primaryServerURL)) {
				String shareSpreadsheetURL = serverURL + RestSpreadsheets.PATH;
				WebTarget target = client.target(shareSpreadsheetURL).path(sheetId).path("share").path(userId)
						.path(ReplicationRestSpreadsheets.OPERATION);

				new Thread(() -> {
					Response r = target.request().header(RestSpreadsheets.HEADER_VERSION, version)
							.accept(MediaType.APPLICATION_JSON).post(null);

					if (r != null && r.getStatus() == Status.OK.getStatusCode()) {
						numberOfAcks.incrementAndGet();
					} else {
						if (r == null)
							System.out.println(THREAD_NULL);
						else {
							System.out.println(THREAD_STATUS + r.getStatus());
						}
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
	}

	public void unshareSpreadsheet(String sheetId, String userId, Long version) {
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());
		AtomicInteger numberOfAcks = new AtomicInteger(0);

		for (String serverURL : existingServers) {

			if (!serverURL.equals(primaryServerURL)) {
				String unshareSpreadsheetURL = serverURL + RestSpreadsheets.PATH;
				WebTarget target = client.target(unshareSpreadsheetURL).path(sheetId).path("share").path(userId)
						.path(ReplicationRestSpreadsheets.OPERATION);

				new Thread(() -> {
					Response r = target.request().header(RestSpreadsheets.HEADER_VERSION, version)
							.accept(MediaType.APPLICATION_JSON).delete();

					if (r != null && r.getStatus() == Status.OK.getStatusCode()) {
						numberOfAcks.incrementAndGet();
					} else {
						if (r == null)
							System.out.println(THREAD_NULL);
						else {
							System.out.println(THREAD_STATUS + r.getStatus());
						}
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
	}

	public void deleteUserSpreadsheets(String userId, Long version) {
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());
		AtomicInteger numberOfAcks = new AtomicInteger(0);

		for (String serverURL : existingServers) {

			if (!serverURL.equals(primaryServerURL)) {
				String deleteUserSpreadsheetsURL = serverURL + RestSpreadsheets.PATH;
				WebTarget target = client.target(deleteUserSpreadsheetsURL)
						.path(ReplicationRestSpreadsheets.DELETESHEETS).path(userId)
						.path(ReplicationRestSpreadsheets.OPERATION);

				new Thread(() -> {
					Response r = target.request().header(RestSpreadsheets.HEADER_VERSION, version)
							.accept(MediaType.APPLICATION_JSON).delete();

					if (r != null && r.getStatus() == Status.OK.getStatusCode()) {
						numberOfAcks.incrementAndGet();
					} else {
						if (r == null)
							System.out.println(THREAD_NULL);
						else {
							System.out.println(THREAD_STATUS + r.getStatus());
						}
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
	}

	// GESTAO DE VERSAO

	synchronized public Long newTask(Task newTask) {
		tasks.add(newTask);

		Long taskAssignedVersion = globalVersionNumber.getAndIncrement();

		return taskAssignedVersion;

		/*
		 * Tasks taskType = Tasks.valueOf(newTask.getClass().getSimpleName()); switch
		 * (taskType) { case CreateSpreadsheetTask:
		 * System.out.println("A NOVA TASK E CRIAR UMA FOLHA"); CreateSpreadsheetTask
		 * cTask = (CreateSpreadsheetTask) newTask; exec.execute(() ->
		 * createSpreadsheet(cTask.getSpreadsheet(), taskVersion)); break; case
		 * DeleteSpreadsheetTask: DeleteSpreadsheetTask dTask = (DeleteSpreadsheetTask)
		 * newTask; exec.execute(() -> deleteSpreadsheet(dTask.getSheetId(),
		 * taskVersion)); break; case DeleteUserSpreadsheetsTask:
		 * DeleteUserSpreadsheetsTask dUTask = (DeleteUserSpreadsheetsTask) newTask;
		 * exec.execute(() -> deleteUserSpreadsheets(dUTask.getUserId(), taskVersion));
		 * break; case ShareSpreadsheetTask: ShareSpreadsheetTask sTask =
		 * (ShareSpreadsheetTask) newTask; exec.execute(() ->
		 * shareSpreadsheet(sTask.getSheetId(), sTask.getUserId(), taskVersion)); break;
		 * case UnshareSpreadsheetTask: UnshareSpreadsheetTask uTask =
		 * (UnshareSpreadsheetTask) newTask; exec.execute(() ->
		 * unshareSpreadsheet(uTask.getSheetId(), uTask.getUserId(), taskVersion));
		 * break; case UpdateCellTask: UpdateCellTask upTask = (UpdateCellTask) newTask;
		 * exec.execute(() -> updateCell(upTask.getSheetId(), upTask.getCell(),
		 * upTask.getRawValue(), taskVersion)); break; default:
		 * System.out.println("Type of task not recognized"); break; }
		 */
	}

	public Long getGlobalSequenceNumber() {
		return globalVersionNumber.get();
	}

	public List<Task> getMissingTasks(Long localVersionNumber) {
		int startingPos = localVersionNumber.intValue();
		return tasks.subList(startingPos, globalVersionNumber.intValue() - 1);
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

			// OBTER OS URLS QUANDO OCORREM ALTERACOES OFERECE MELHOR DESEMPENHO
			existingServers.add(znodeURL);
		}

		if (previousPrimaryServerURL != null && !previousPrimaryServerURL.equals(primaryServerURL))
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
		target = client.target(serverNotificationURL);
		Response r = target.request().header(RestSpreadsheets.HEADER_VERSION, globalVersionNumber)
				.accept(MediaType.APPLICATION_JSON).post(null);

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
}
