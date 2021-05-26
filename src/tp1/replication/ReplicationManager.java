package tp1.replication;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

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
import tp1.replication.tasks.CreateSpreadsheetTask;
import tp1.replication.tasks.DeleteSpreadsheetTask;
import tp1.replication.tasks.DeleteUserSpreadsheetsTask;
import tp1.replication.tasks.ShareSpreadsheetTask;
import tp1.replication.tasks.Task;
import tp1.replication.tasks.UnshareSpreadsheetTask;
import tp1.replication.tasks.UpdateCellTask;
import tp1.server.rest.replication.ReplicationSpreadsheetsServer;
import tp1.util.InsecureHostnameVerifier;
import tp1.util.ZookeeperProcessor;

public class ReplicationManager {

	private final static int CONNECTION_TIMEOUT = 10000;
	private final static int REPLY_TIMEOUT = 1000;
	private static final String ZOO_ERROR = "Error on instantiating Zookeeper.";
	private static final String ZOOKEEPER_HOSTPORT = "localhost:2181,kafka:2181";

	private static Logger Log = Logger.getLogger(ReplicationManager.class.getName());

	private static ReplicationManager instance;
	private ZookeeperProcessor zk;

	private String primaryServerURL;
	private List<String> existingServers;
	private ExecutorService exec;

	private List<Task> tasks;
	private int globalVersionNumber;

	synchronized public static ReplicationManager getInstance() {
		if (instance == null)
			instance = new ReplicationManager();

		return instance;
	}

	private ReplicationManager() {
		primaryServerURL = null;
		exec = Executors.newCachedThreadPool();

		tasks = new ArrayList<Task>();
		globalVersionNumber = 0;

		startZookeeper();
	}

	public boolean isPrimary(String serverURL) {
		return primaryServerURL.equals(serverURL);
	}

	public String getPrimaryServerURL() {
		return primaryServerURL;
	}

	// EXECUCAO DOS PEDIDOS

	public void createSpreadsheet(Spreadsheet sheet) {
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());

		ClientConfig config = new ClientConfig();
		config.property(ClientProperties.CONNECT_TIMEOUT, CONNECTION_TIMEOUT);
		config.property(ClientProperties.READ_TIMEOUT, REPLY_TIMEOUT);
		Client client = ClientBuilder.newClient(config);
		WebTarget target;

		for (int i = 0; i < existingServers.size(); i++) {
			String serverURL = existingServers.get(i);

			if (!serverURL.equals(primaryServerURL)) {
				String createSpreadsheetURL = serverURL + RestSpreadsheets.PATH + ReplicationRestSpreadsheets.OPERATION;
				target = client.target(createSpreadsheetURL);

				Response r = target.request().accept(MediaType.APPLICATION_JSON)
						.post(Entity.entity(sheet, MediaType.APPLICATION_JSON));

				if (r != null && r.getStatus() == Status.OK.getStatusCode()) {
					// SUCESSO PODE SEGUIR EM FRENTE
					int successPos = i;
					// UMA THREAD CONTINUA O TRABALHO
					exec.execute(() -> continueCreateSpreadsheet(successPos + 1, client, sheet));
					// ADICIONA A OPERACAO AO CONJUNTO DE OPERACOES REALIZADAS
					int taskVersionNumber = attributeVersionNumber();
					tasks.add(taskVersionNumber, new CreateSpreadsheetTask(taskVersionNumber, sheet));
					break;
				} else {
					// PERGUNTAR
				}
			}
		}
	}

	private void continueCreateSpreadsheet(int startingPos, Client client, Spreadsheet sheet) {
		WebTarget target;

		for (int i = startingPos; i < existingServers.size(); i++) {
			String serverURL = existingServers.get(i);

			if (!serverURL.equals(primaryServerURL)) {
				String createSpreadsheetURL = serverURL + RestSpreadsheets.PATH + ReplicationRestSpreadsheets.OPERATION;
				target = client.target(createSpreadsheetURL);
				target.request().accept(MediaType.APPLICATION_JSON)
						.post(Entity.entity(sheet, MediaType.APPLICATION_JSON));
			}
		}
	}

	public void deleteSpreadsheet(String sheetId) {
		ClientConfig config = new ClientConfig();
		config.property(ClientProperties.CONNECT_TIMEOUT, CONNECTION_TIMEOUT);
		config.property(ClientProperties.READ_TIMEOUT, REPLY_TIMEOUT);
		Client client = ClientBuilder.newClient(config);
		WebTarget target;

		for (int i = 0; i < existingServers.size(); i++) {
			String serverURL = existingServers.get(i);

			if (!serverURL.equals(primaryServerURL)) {
				String deleteSpreadsheetURL = serverURL + RestSpreadsheets.PATH;
				target = client.target(deleteSpreadsheetURL).path(sheetId).path(ReplicationRestSpreadsheets.OPERATION);

				Response r = target.request().accept(MediaType.APPLICATION_JSON).delete();

				if (r != null && r.getStatus() == Status.OK.getStatusCode()) {
					// SUCESSO PODE SEGUIR EM FRENTE
					int successPos = i;
					// UMA THREAD CONTINUA O TRABALHO
					exec.execute(() -> continueDeleteSpreadsheet(successPos + 1, client, sheetId));
					// ADICIONA A OPERACAO AO CONJUNTO DE OPERACOES REALIZADAS
					int taskVersionNumber = attributeVersionNumber();
					tasks.add(taskVersionNumber, new DeleteSpreadsheetTask(taskVersionNumber, sheetId));
					break;
				} else {
					// PERGUNTAR
				}
			}
		}
	}

	private void continueDeleteSpreadsheet(int startingPos, Client client, String sheetId) {
		WebTarget target;

		for (int i = startingPos; i < existingServers.size(); i++) {
			String serverURL = existingServers.get(i);

			if (!serverURL.equals(primaryServerURL)) {
				String deleteSpreadsheetURL = serverURL + RestSpreadsheets.PATH;
				target = client.target(deleteSpreadsheetURL).path(sheetId).path(ReplicationRestSpreadsheets.OPERATION);
				target.request().accept(MediaType.APPLICATION_JSON).delete();
			}
		}
	}

	public void updateCell(String sheetId, String cell, String rawValue) {
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());

		ClientConfig config = new ClientConfig();
		config.property(ClientProperties.CONNECT_TIMEOUT, CONNECTION_TIMEOUT);
		config.property(ClientProperties.READ_TIMEOUT, REPLY_TIMEOUT);
		Client client = ClientBuilder.newClient(config);
		WebTarget target;

		for (int i = 0; i < existingServers.size(); i++) {
			String serverURL = existingServers.get(i);

			if (!serverURL.equals(primaryServerURL)) {
				String updateCellURL = serverURL + RestSpreadsheets.PATH;
				target = client.target(updateCellURL).path(sheetId).path(cell)
						.path(ReplicationRestSpreadsheets.OPERATION);

				Response r = target.request().accept(MediaType.APPLICATION_JSON)
						.put(Entity.entity(rawValue, MediaType.APPLICATION_JSON));

				if (r != null && r.getStatus() == Status.OK.getStatusCode()) {
					// SUCESSO PODE SEGUIR EM FRENTE
					int successPos = i;
					// UMA THREAD CONTINUA O TRABALHO
					exec.execute(() -> continueUpdateCell(successPos + 1, client, sheetId, cell, rawValue));
					// ADICIONA A OPERACAO AO CONJUNTO DE OPERACOES REALIZADAS
					int taskVersionNumber = attributeVersionNumber();
					tasks.add(taskVersionNumber, new UpdateCellTask(taskVersionNumber, sheetId, cell, rawValue));
					break;
				} else {
					// PERGUNTAR
				}
			}
		}
	}

	private void continueUpdateCell(int startingPos, Client client, String sheetId, String cell, String rawValue) {
		WebTarget target;

		for (int i = startingPos; i < existingServers.size(); i++) {
			String serverURL = existingServers.get(i);

			if (!serverURL.equals(primaryServerURL)) {
				String updateCellURL = serverURL + RestSpreadsheets.PATH;
				target = client.target(updateCellURL).path(sheetId).path(cell)
						.path(ReplicationRestSpreadsheets.OPERATION);
				target.request().accept(MediaType.APPLICATION_JSON)
						.put(Entity.entity(rawValue, MediaType.APPLICATION_JSON));
			}
		}
	}

	public void shareSpreadsheet(String sheetId, String userId) {
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());

		ClientConfig config = new ClientConfig();
		config.property(ClientProperties.CONNECT_TIMEOUT, CONNECTION_TIMEOUT);
		config.property(ClientProperties.READ_TIMEOUT, REPLY_TIMEOUT);
		Client client = ClientBuilder.newClient(config);
		WebTarget target;

		for (int i = 0; i < existingServers.size(); i++) {
			String serverURL = existingServers.get(i);

			if (!serverURL.equals(primaryServerURL)) {
				String shareSpreadsheetURL = serverURL + RestSpreadsheets.PATH;
				target = client.target(shareSpreadsheetURL).path(sheetId).path("share").path(userId)
						.path(ReplicationRestSpreadsheets.OPERATION);

				Response r = target.request().accept(MediaType.APPLICATION_JSON).post(null);

				if (r != null && r.getStatus() == Status.OK.getStatusCode()) {
					// SUCESSO PODE SEGUIR EM FRENTE
					int successPos = i;
					// UMA THREAD CONTINUA O TRABALHO
					exec.execute(() -> continueShareSpreadsheet(successPos + 1, client, sheetId, userId));
					// ADICIONA A OPERACAO AO CONJUNTO DE OPERACOES REALIZADAS
					int taskVersionNumber = attributeVersionNumber();
					tasks.add(taskVersionNumber, new ShareSpreadsheetTask(taskVersionNumber, sheetId, userId));
					break;
				} else {
					// PERGUNTAR
				}
			}
		}
	}

	private void continueShareSpreadsheet(int startingPos, Client client, String sheetId, String userId) {
		WebTarget target;

		for (int i = startingPos; i < existingServers.size(); i++) {
			String serverURL = existingServers.get(i);

			if (!serverURL.equals(primaryServerURL)) {
				String shareSpreadsheetURL = serverURL + RestSpreadsheets.PATH;
				target = client.target(shareSpreadsheetURL).path(sheetId).path("share").path(userId)
						.path(ReplicationRestSpreadsheets.OPERATION);
				target.request().accept(MediaType.APPLICATION_JSON).post(null);
			}
		}
	}

	public void unshareSpreadsheet(String sheetId, String userId) {
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());

		ClientConfig config = new ClientConfig();
		config.property(ClientProperties.CONNECT_TIMEOUT, CONNECTION_TIMEOUT);
		config.property(ClientProperties.READ_TIMEOUT, REPLY_TIMEOUT);
		Client client = ClientBuilder.newClient(config);
		WebTarget target;

		for (int i = 0; i < existingServers.size(); i++) {
			String serverURL = existingServers.get(i);

			if (!serverURL.equals(primaryServerURL)) {
				String unshareSpreadsheetURL = serverURL + RestSpreadsheets.PATH;
				target = client.target(unshareSpreadsheetURL).path(sheetId).path("share").path(userId)
						.path(ReplicationRestSpreadsheets.OPERATION);

				Response r = target.request().accept(MediaType.APPLICATION_JSON).delete();

				if (r != null && r.getStatus() == Status.OK.getStatusCode()) {
					// SUCESSO PODE SEGUIR EM FRENTE
					int successPos = i;
					// UMA THREAD CONTINUA O TRABALHO
					exec.execute(() -> continueUnshareSpreadsheet(successPos + 1, client, sheetId, userId));
					// ADICIONA A OPERACAO AO CONJUNTO DE OPERACOES REALIZADAS
					int taskVersionNumber = attributeVersionNumber();
					tasks.add(taskVersionNumber, new UnshareSpreadsheetTask(taskVersionNumber, sheetId, userId));
					break;
				} else {
					// PERGUNTAR
				}
			}
		}
	}

	private void continueUnshareSpreadsheet(int startingPos, Client client, String sheetId, String userId) {
		WebTarget target;

		for (int i = startingPos; i < existingServers.size(); i++) {
			String serverURL = existingServers.get(i);

			if (!serverURL.equals(primaryServerURL)) {
				String unshareSpreadsheetURL = serverURL + RestSpreadsheets.PATH;
				target = client.target(unshareSpreadsheetURL).path(sheetId).path("share").path(userId)
						.path(ReplicationRestSpreadsheets.OPERATION);
				target.request().accept(MediaType.APPLICATION_JSON).delete();
			}
		}
	}

	public void deleteUserSpreadsheets(String userId, String secret) {
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());

		ClientConfig config = new ClientConfig();
		config.property(ClientProperties.CONNECT_TIMEOUT, CONNECTION_TIMEOUT);
		config.property(ClientProperties.READ_TIMEOUT, REPLY_TIMEOUT);
		Client client = ClientBuilder.newClient(config);
		WebTarget target;

		for (int i = 0; i < existingServers.size(); i++) {
			String serverURL = existingServers.get(i);

			if (!serverURL.equals(primaryServerURL)) {
				String deleteUserSpreadsheetsURL = serverURL + RestSpreadsheets.PATH;
				target = client.target(deleteUserSpreadsheetsURL).path(ReplicationRestSpreadsheets.DELETESHEETS)
						.path(userId).path(ReplicationRestSpreadsheets.OPERATION).queryParam("secret", secret);

				Response r = target.request().accept(MediaType.APPLICATION_JSON).delete();

				if (r != null && r.getStatus() == Status.OK.getStatusCode()) {
					// SUCESSO PODE SEGUIR EM FRENTE
					int successPos = i;
					// UMA THREAD CONTINUA O TRABALHO
					exec.execute(() -> continueDeleteUserSpreadsheets(successPos + 1, client, userId, secret));
					// ADICIONA A OPERACAO AO CONJUNTO DE OPERACOES REALIZADAS
					int taskVersionNumber = attributeVersionNumber();
					tasks.add(taskVersionNumber, new DeleteUserSpreadsheetsTask(taskVersionNumber, userId));
					break;
				} else {
					// PERGUNTAR
				}
			}
		}
	}

	private void continueDeleteUserSpreadsheets(int startingPos, Client client, String userId, String secret) {
		WebTarget target;

		for (int i = startingPos; i < existingServers.size(); i++) {
			String serverURL = existingServers.get(i);

			if (!serverURL.equals(primaryServerURL)) {
				String deleteUserSpreadsheetsURL = serverURL + RestSpreadsheets.PATH;
				target = client.target(deleteUserSpreadsheetsURL).path(ReplicationRestSpreadsheets.DELETESHEETS)
						.path(userId).path(ReplicationRestSpreadsheets.OPERATION).queryParam("secret", secret);
				target.request().accept(MediaType.APPLICATION_JSON).delete();
			}
		}
	}

	// GESTAO DE VERSAO

	synchronized private int attributeVersionNumber() {
		int currentVersionNumber = globalVersionNumber;
		globalVersionNumber++;
		return currentVersionNumber;
	}

	public int getGlobalSequenceNumber() {
		return globalVersionNumber;
	}

	public List<Task> getMissingTasks(int localVersionNumber) {
		return tasks.subList(localVersionNumber, tasks.size());
	}

	// ZOOKEEPER

	private void startZookeeper() {
		try {
			zk = ZookeeperProcessor.getInstance(ZOOKEEPER_HOSTPORT);
		} catch (Exception e) {
			Log.info(ZOO_ERROR);
		}

		String domainZNode = "/" + ReplicationSpreadsheetsServer.spreadsheetsDomain;

		if (zk.write(domainZNode, CreateMode.PERSISTENT) != null) {
			System.out.println("Created znode: " + domainZNode);
		}

		String serverZNode = String.format("%s/%s_", domainZNode, "replica");

		// PASSAR O URL DO SERVIDOR NO NOME DO ZNODE
		String znodePath = zk.write(serverZNode, ReplicationSpreadsheetsServer.serverURL,
				CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println("Created child znode: " + znodePath);

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

		String primaryServerNode = existingZNodes.get(0);

		for (String znode : existingZNodes) {
			String znodePath = domainZNode + "/" + znode;
			String znodeURL = zk.getValue(znodePath);

			if (primaryServerNode.compareTo(znode) > 0) {
				primaryServerNode = znode;
				primaryServerURL = znodeURL;
			}

			// OBTER OS URLS QUANDO OCORREM ALTERACOES OFERECE MELHOR DESEMPENHO
			existingServers.add(znodeURL);
		}
	}

}
