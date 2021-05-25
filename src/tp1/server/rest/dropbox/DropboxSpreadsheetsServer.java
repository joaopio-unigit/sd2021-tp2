package tp1.server.rest.dropbox;

import java.net.InetAddress;
import java.net.URI;
import java.util.logging.Logger;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import tp1.impl.GenericExceptionMapper;
import tp1.server.resource.dropbox.DropboxSpreadsheetsResource;
import tp1.util.Discovery;
import tp1.util.InsecureHostnameVerifier;;

public class DropboxSpreadsheetsServer {

	private static Logger Log = Logger.getLogger(DropboxSpreadsheetsServer.class.getName());

	static {
		System.setProperty("java.net.preferIPv4Stack", "true");
		System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s: %5$s\n");
	}
	
	private static final String TRUE = "true";
	public static final int PORT = 8080;
	public static final String SERVICE = "sheets";
	
	
	public static String spreadsheetsDomain;
	public static boolean stateReset;
	public static Discovery sheetsDiscovery;
	public static String serverSecret;
	
	public static String apiKey;
	public static String apiSecret;
	public static String accessTokenStr;
	
	public static void main(String[] args) {
		
		System.out.println("DROPBOX SERVER A ARRANCAR");
		
		spreadsheetsDomain =  args.length > 0 ? args[0] : "?";
		serverSecret =  args.length > 0 ? args[2] : "?";
		
		apiKey = args.length > 0 ? args[3] : "?";
		apiSecret = args.length > 0 ? args[4] : "?";
		accessTokenStr = args.length > 0 ? args[5] : "?";		
		
		if(args[1].equals(TRUE))
			stateReset = true;
		else
			stateReset = false;
		
		try {		
		String ip = InetAddress.getLocalHost().getHostAddress();
				
		//HTTPS
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());
				
		ResourceConfig config = new ResourceConfig();
		config.register(DropboxSpreadsheetsResource.class);
		config.register(new GenericExceptionMapper());
		
		//HTTPS
		String serverURI = String.format("https://%s:%s/rest", ip, PORT);
		JdkHttpServerFactory.createHttpServer( URI.create(serverURI), config, SSLContext.getDefault());
	
		Log.info(String.format("%s Server ready @ %s\n",  SERVICE, serverURI));
				
		String serviceName = spreadsheetsDomain + ":" + SERVICE;
		
		sheetsDiscovery = new Discovery(serviceName, serverURI, Discovery.DEFAULT);								//CRIAR O OBJETO DISCOVERY

		sheetsDiscovery.start();																				//COMECAR A ANUNCIAR O SERVICO
				
		//More code can be executed here...
		} catch( Exception e) {
			Log.severe(e.getMessage());
		}
	}
	
}
