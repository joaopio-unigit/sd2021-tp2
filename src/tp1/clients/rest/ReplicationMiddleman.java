package tp1.clients.rest;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import javax.net.ssl.HttpsURLConnection;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import tp1.api.Spreadsheet;
import tp1.server.rest.replication.ReplicationSpreadsheetsServer;
import tp1.util.InsecureHostnameVerifier;
import tp1.util.ZookeeperProcessor;

public class ReplicationMiddleman {

	private final static int CONNECTION_TIMEOUT = 10000;
	private final static int REPLY_TIMEOUT = 1000;
	
	private static Logger Log = Logger.getLogger(ReplicationMiddleman.class.getName());
	private static final String ZOO_ERROR = "Error on instantiating Zookeeper.";

	private ZookeeperProcessor zk;

	private String primaryServerURL;
	private boolean primaryServer;
	private List<String> existingServers;

	public ReplicationMiddleman() {
		primaryServer = false;
		primaryServerURL = null;
		startZookeeper();
	}
	
	public boolean isPrimary() {
		return primaryServer;
	}
	
	public String getPrimaryServerURL() {
		return primaryServerURL;
	}
	
	//EXECUCAO DOS PEDIDOS
	
	public void createSpreadsheet(Spreadsheet sheet, String password) {
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());
		
		ClientConfig config = new ClientConfig();
		config.property(ClientProperties.CONNECT_TIMEOUT, CONNECTION_TIMEOUT);
		config.property(ClientProperties.READ_TIMEOUT, REPLY_TIMEOUT);
		Client client = ClientBuilder.newClient(config);
		
		
	}
	
	// ZOOKEEPER

	private void startZookeeper() {
		try {
			zk = new ZookeeperProcessor("localhost:2181,kafka:2181");
		} catch (Exception e) {
			Log.info(ZOO_ERROR);
		}

		String domainZNode = "/" + ReplicationSpreadsheetsServer.spreadsheetsDomain;

		if (zk.write(domainZNode, CreateMode.PERSISTENT) != null) {
			System.out.println("Created znode: " + domainZNode);
		}

		String serverZNode = String.format("%s/%s_", domainZNode, "replica");

		// PASSAR O URL DO SERVIDOR NO NOME DO ZNODE
		String znodePath = zk.write(serverZNode, ReplicationSpreadsheetsServer.serverURL, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println("Created child znode: " + znodePath);

		List<String> existingZnodes = zk.getChildren(domainZNode, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				List<String> existingZNodes = zk.getChildren(domainZNode, this);			
				//ELEGER O PRIMARIO QUANDO HOUVER ALTERACOES
				primaryServerElection(znodePath, existingZNodes);
			}

		});

		//ELEGER O PRIMARIO QUANDO A CLASSE E INICIALIZADA
		primaryServerElection(znodePath, existingZnodes);
	}

	private void primaryServerElection(String serverZNode, List<String> existingZNodes) {
		existingServers = new LinkedList<String>();
		
		String primaryServerNode = existingZNodes.get(0);

		for (String znode : existingZNodes) {
			String znodeURL = zk.getValue(znode);
			
			if (primaryServerNode.compareTo(znode) > 0) {
				primaryServerNode = znode;
				primaryServerURL = znodeURL;
			}
				
			//OBTER OS URLS QUANDO OCORREM ALTERACOES OFERECE MELHOR DESEMPENHO
			existingServers.add(znodeURL);
		}

		if (primaryServerNode.equals(serverZNode)) {
			primaryServer = true;	
		} else {
			primaryServer = false;
		}
	}
}
