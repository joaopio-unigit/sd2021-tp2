package tp1.clients.replication;

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

public class ReplicationMiddleman {

	private final static int CONNECTION_TIMEOUT = 10000;
	private final static int REPLY_TIMEOUT = 1000;
	private static final String ZOO_ERROR = "Error on instantiating Zookeeper.";
	private static final String ZOOKEEPER_HOSTPORT = "localhost:2181,kafka:2181";
	
	private static Logger Log = Logger.getLogger(ReplicationMiddleman.class.getName());

	private static ReplicationMiddleman instance;
	private ZookeeperProcessor zk;

	private String primaryServerURL;
	private boolean primaryServer;
	private List<String> existingServers;
	private ExecutorService exec;

	private List<Task> tasks;
	private int taskSequenceNumber;
	
	synchronized public static ReplicationMiddleman getInstance() {
		if(instance == null)
			instance = new ReplicationMiddleman();
		
		return instance;
	}
	
	private ReplicationMiddleman() {
		primaryServer = false;
		primaryServerURL = null;
		exec = Executors.newCachedThreadPool();
		
		tasks = new ArrayList<Task>();
		taskSequenceNumber = 0;
		
		startZookeeper();
	}

	public boolean isPrimary() {
		return primaryServer;
	}

	public String getPrimaryServerURL() {
		return primaryServerURL;
	}

	// EXECUCAO DOS PEDIDOS

	public void createSpreadsheet(Spreadsheet sheet, String password) {
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());

		ClientConfig config = new ClientConfig();
		config.property(ClientProperties.CONNECT_TIMEOUT, CONNECTION_TIMEOUT);
		config.property(ClientProperties.READ_TIMEOUT, REPLY_TIMEOUT);
		Client client = ClientBuilder.newClient(config);
		WebTarget target;
		
		for (int i = 0; i < existingServers.size(); i++) {
			String createSpreadsheetURL = existingServers.get(i) + RestSpreadsheets.PATH + ReplicationRestSpreadsheets.OPERATION;
			target = client.target(createSpreadsheetURL);

			Response r = target.request().accept(MediaType.APPLICATION_JSON).post(Entity.entity(sheet, MediaType.APPLICATION_JSON));

			if (r != null && r.getStatus() == Status.OK.getStatusCode()) {
				// SUCESSO PODE SEGUIR EM FRENTE
				int successPos = i;
				//UMA THREAD CONTINUA O TRABALHO
				exec.execute(() -> continueCreateSpreadsheet(successPos+1, client, sheet));
				//ADICIONA A OPERACAO AO CONJUNTO DE OPERACOES REALIZADAS
				tasks.add(new CreateSpreadsheetTask(getTaskSequenceNumber(), sheet));
				break;
			} else {
				// PERGUNTAR
			}
		}
	}

	private void continueCreateSpreadsheet(int startingPos, Client client, Spreadsheet sheet) {
		WebTarget target;
		
		for (int i = startingPos; i < existingServers.size(); i++) {
			String createSpreadsheetURL = existingServers.get(i) + RestSpreadsheets.PATH + ReplicationRestSpreadsheets.OPERATION;
			target = client.target(createSpreadsheetURL);
			target.request().accept(MediaType.APPLICATION_JSON).post(Entity.entity(sheet, MediaType.APPLICATION_JSON));
		}
	}

	public void deleteSpreadsheet(String sheetId) {
		ClientConfig config = new ClientConfig();
		config.property(ClientProperties.CONNECT_TIMEOUT, CONNECTION_TIMEOUT);
		config.property(ClientProperties.READ_TIMEOUT, REPLY_TIMEOUT);
		Client client = ClientBuilder.newClient(config);
		WebTarget target;
		
		for (int i = 0; i < existingServers.size(); i++) {
			String deleteSpreadsheetURL = existingServers.get(i) + RestSpreadsheets.PATH;
			target = client.target(deleteSpreadsheetURL).path(sheetId).path(ReplicationRestSpreadsheets.OPERATION);

			Response r = target.request().accept(MediaType.APPLICATION_JSON).delete();

			if (r != null && r.getStatus() == Status.OK.getStatusCode()) {
				// SUCESSO PODE SEGUIR EM FRENTE
				int successPos = i;
				//UMA THREAD CONTINUA O TRABALHO
				exec.execute(() -> continueDeleteSpreadsheet(successPos+1, client, sheetId));
				//ADICIONA A OPERACAO AO CONJUNTO DE OPERACOES REALIZADAS
				tasks.add(new DeleteSpreadsheetTask(getTaskSequenceNumber(), sheetId));
				break;
			} else {
				// PERGUNTAR
			}
		}
	}
	
	private void continueDeleteSpreadsheet(int startingPos, Client client, String sheetId) {
		WebTarget target;
		
		for (int i = startingPos; i < existingServers.size(); i++) {
			String deleteSpreadsheetURL = existingServers.get(i) + RestSpreadsheets.PATH;
			target = client.target(deleteSpreadsheetURL).path(sheetId).path(ReplicationRestSpreadsheets.OPERATION);
			target.request().accept(MediaType.APPLICATION_JSON).delete();

		}
	}
	
	public void updateCell(String sheetId, String cell, String rawValue) {
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());

		ClientConfig config = new ClientConfig();
		config.property(ClientProperties.CONNECT_TIMEOUT, CONNECTION_TIMEOUT);
		config.property(ClientProperties.READ_TIMEOUT, REPLY_TIMEOUT);
		Client client = ClientBuilder.newClient(config);
		WebTarget target;
		
		for(int i = 0 ; i < existingServers.size(); i++) {
			String updateCellURL = existingServers.get(i) + RestSpreadsheets.PATH;
			target = client.target(updateCellURL).path(sheetId).path(cell).path(ReplicationRestSpreadsheets.OPERATION);
			
			Response r = target.request().accept(MediaType.APPLICATION_JSON).put(Entity.entity(rawValue, MediaType.APPLICATION_JSON));

			if (r != null && r.getStatus() == Status.OK.getStatusCode()) {
				// SUCESSO PODE SEGUIR EM FRENTE
				int successPos = i;
				//UMA THREAD CONTINUA O TRABALHO
				exec.execute(() -> continueUpdateCell(successPos+1, client, sheetId, cell, rawValue));
				//ADICIONA A OPERACAO AO CONJUNTO DE OPERACOES REALIZADAS
				tasks.add(new UpdateCellTask(getTaskSequenceNumber(), sheetId, cell, rawValue));
				break;
			} else {
				// PERGUNTAR
			}
		}
	}
	
	private void continueUpdateCell(int startingPos, Client client, String sheetId, String cell, String rawValue) {
		WebTarget target;
		
		for (int i = startingPos; i < existingServers.size(); i++) {
			String updateCellURL = existingServers.get(i) + RestSpreadsheets.PATH;
			target = client.target(updateCellURL).path(sheetId).path(cell).path(ReplicationRestSpreadsheets.OPERATION);
			target.request().accept(MediaType.APPLICATION_JSON).put(Entity.entity(rawValue, MediaType.APPLICATION_JSON));
		}
	}

	public void shareSpreadsheet(String sheetId, String userId) {
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());

		ClientConfig config = new ClientConfig();
		config.property(ClientProperties.CONNECT_TIMEOUT, CONNECTION_TIMEOUT);
		config.property(ClientProperties.READ_TIMEOUT, REPLY_TIMEOUT);
		Client client = ClientBuilder.newClient(config);
		WebTarget target;
		
		for(int i = 0 ; i < existingServers.size(); i++) {
			String shareSpreadsheetURL = existingServers.get(i) + RestSpreadsheets.PATH;
			target = client.target(shareSpreadsheetURL).path(sheetId).path("share").path(userId).path(ReplicationRestSpreadsheets.OPERATION);
			
			Response r = target.request().accept(MediaType.APPLICATION_JSON).post(null);

			if (r != null && r.getStatus() == Status.OK.getStatusCode()) {
				// SUCESSO PODE SEGUIR EM FRENTE
				int successPos = i;
				//UMA THREAD CONTINUA O TRABALHO
				exec.execute(() -> continueShareSpreadsheet(successPos+1, client, sheetId, userId));
				//ADICIONA A OPERACAO AO CONJUNTO DE OPERACOES REALIZADAS
				tasks.add(new ShareSpreadsheetTask(getTaskSequenceNumber(), sheetId, userId));
				break;
			} else {
				// PERGUNTAR
			}
		}	
	}
	
	private void continueShareSpreadsheet(int startingPos, Client client, String sheetId, String userId) {
		WebTarget target;
		
		for (int i = startingPos; i < existingServers.size(); i++) {
			String shareSpreadsheetURL = existingServers.get(i) + RestSpreadsheets.PATH;
			target = client.target(shareSpreadsheetURL).path(sheetId).path("share").path(userId).path(ReplicationRestSpreadsheets.OPERATION);
			target.request().accept(MediaType.APPLICATION_JSON).post(null);
		}
	}
	
	public void unshareSpreadsheet(String sheetId, String userId) {
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());

		ClientConfig config = new ClientConfig();
		config.property(ClientProperties.CONNECT_TIMEOUT, CONNECTION_TIMEOUT);
		config.property(ClientProperties.READ_TIMEOUT, REPLY_TIMEOUT);
		Client client = ClientBuilder.newClient(config);
		WebTarget target;
		
		for(int i = 0 ; i < existingServers.size(); i++) {
			String unshareSpreadsheetURL = existingServers.get(i) + RestSpreadsheets.PATH;
			target = client.target(unshareSpreadsheetURL).path(sheetId).path("share").path(userId).path(ReplicationRestSpreadsheets.OPERATION);
			
			Response r = target.request().accept(MediaType.APPLICATION_JSON).delete();

			if (r != null && r.getStatus() == Status.OK.getStatusCode()) {
				// SUCESSO PODE SEGUIR EM FRENTE
				int successPos = i;
				//UMA THREAD CONTINUA O TRABALHO
				exec.execute(() -> continueUnshareSpreadsheet(successPos+1, client, sheetId, userId));
				//ADICIONA A OPERACAO AO CONJUNTO DE OPERACOES REALIZADAS
				tasks.add(new UnshareSpreadsheetTask(getTaskSequenceNumber(), sheetId, userId));
				break;
			} else {
				// PERGUNTAR
			}
		}
	}

	private void continueUnshareSpreadsheet(int startingPos, Client client, String sheetId, String userId) {
		WebTarget target;
		
		for (int i = startingPos; i < existingServers.size(); i++) {
			String unshareSpreadsheetURL = existingServers.get(i) + RestSpreadsheets.PATH;
			target = client.target(unshareSpreadsheetURL).path(sheetId).path("share").path(userId).path(ReplicationRestSpreadsheets.OPERATION);
			target.request().accept(MediaType.APPLICATION_JSON).delete();
		}
	}
	
	public void deleteUserSpreadsheets(String userId, String secret) {
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());

		ClientConfig config = new ClientConfig();
		config.property(ClientProperties.CONNECT_TIMEOUT, CONNECTION_TIMEOUT);
		config.property(ClientProperties.READ_TIMEOUT, REPLY_TIMEOUT);
		Client client = ClientBuilder.newClient(config);
		WebTarget target;
		
		for(int i = 0 ; i < existingServers.size(); i++) {
			String deleteUserSpreadsheetsURL = existingServers.get(i) + RestSpreadsheets.PATH;
			target = client.target(deleteUserSpreadsheetsURL).path(ReplicationRestSpreadsheets.DELETESHEETS).path(userId).path(ReplicationRestSpreadsheets.OPERATION).queryParam("secret", secret);
			
			Response r = target.request().accept(MediaType.APPLICATION_JSON).delete();

			if (r != null && r.getStatus() == Status.OK.getStatusCode()) {
				// SUCESSO PODE SEGUIR EM FRENTE
				int successPos = i;
				//UMA THREAD CONTINUA O TRABALHO
				exec.execute(() -> continueDeleteUserSpreadsheets(successPos+1, client, userId, secret));
				//ADICIONA A OPERACAO AO CONJUNTO DE OPERACOES REALIZADAS
				tasks.add(new DeleteUserSpreadsheetsTask(getTaskSequenceNumber(),userId));
				break;
			} else {
				// PERGUNTAR
			}
		}
	}
	
	private void continueDeleteUserSpreadsheets(int startingPos, Client client, String userId, String secret) {
		WebTarget target;
		
		for (int i = startingPos; i < existingServers.size(); i++) {
			String deleteUserSpreadsheetsURL = existingServers.get(i) + RestSpreadsheets.PATH;
			target = client.target(deleteUserSpreadsheetsURL).path(ReplicationRestSpreadsheets.DELETESHEETS).path(userId).path(ReplicationRestSpreadsheets.OPERATION).queryParam("secret", secret);
			target.request().accept(MediaType.APPLICATION_JSON).delete();
		}
	}
	
	synchronized private int getTaskSequenceNumber() {
		int currentSequenceNumber = taskSequenceNumber;
		taskSequenceNumber++;
		return currentSequenceNumber;
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
		
		//LISTA COM ELEMENTOS COMO replica_00000000000000
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
		String primaryServerNodePath = domainZNode + "/" + primaryServerNode;

		for (String znode : existingZNodes) {
			String znodePath = domainZNode + "/" + znode;
			String znodeURL = zk.getValue(znodePath);

			if (primaryServerNode.compareTo(znode) > 0) {
				primaryServerNode = znode;
				primaryServerURL = znodeURL;
				primaryServerNodePath = znodePath;
			}

			// OBTER OS URLS QUANDO OCORREM ALTERACOES OFERECE MELHOR DESEMPENHO
			existingServers.add(znodeURL);
		}

		if (primaryServerNodePath.equals(serverZNodePath)) {
			primaryServer = true;
		} else {
			primaryServer = false;
		}
	}






}
