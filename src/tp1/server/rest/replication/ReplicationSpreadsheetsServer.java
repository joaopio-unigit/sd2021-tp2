package tp1.server.rest.replication;

import java.net.InetAddress;
import java.net.URI;
import java.util.logging.Logger;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import tp1.impl.GenericExceptionMapper;
import tp1.replication.ReplicationManager;
import tp1.replication.VersionFilter;
import tp1.server.resource.replication.ReplicationSpreadsheetsResource;
import tp1.util.Discovery;
import tp1.util.InsecureHostnameVerifier;

public class ReplicationSpreadsheetsServer {

	private static Logger Log = Logger.getLogger(ReplicationSpreadsheetsServer.class.getName());

	static {
		System.setProperty("java.net.preferIPv4Stack", "true");
		System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s: %5$s\n");
	}
	
	public static final int PORT = 8080;
	public static final String SERVICE = "sheets";
	public static String spreadsheetsDomain;
	public static Discovery sheetsDiscovery;
	public static String serverURL;
	public static String serverSecret;
	
	public static void main(String[] args) {
		
		spreadsheetsDomain =  args.length > 0 ? args[0] : "?";
		serverSecret =  args.length > 0 ? args[1] : "?";
				
		try {		
		String ip = InetAddress.getLocalHost().getHostAddress();
				
		//HTTPS
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());
				
		//HTTPS
		serverURL = String.format("https://%s:%s/rest", ip, PORT);
		
		ReplicationManager replicationM = ReplicationManager.getInstance();										//REPLICATION
		replicationM.startZookeeper();
		
		String serviceName = spreadsheetsDomain + ":" + SERVICE;
		sheetsDiscovery = new Discovery(serviceName, serverURL, Discovery.DEFAULT);								//CRIAR O OBJETO DISCOVERY
		
		sheetsDiscovery.start();																				//COMECAR A ANUNCIAR O SERVICO

		ResourceConfig config = new ResourceConfig();
		config.register(ReplicationSpreadsheetsResource.class);
		config.register(new GenericExceptionMapper());
		config.register(new VersionFilter(replicationM));														//REPLICATION
		
		JdkHttpServerFactory.createHttpServer( URI.create(serverURL), config, SSLContext.getDefault());
	
		Log.info(String.format("%s Server ready @ %s\n",  SERVICE, serverURL));	
				
		//More code can be executed here...
		} catch( Exception e) {
			Log.severe(e.getMessage());
		}
	}
	
}
