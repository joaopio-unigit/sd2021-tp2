package tp1.server.rest;

import java.net.InetAddress;
import java.net.URI;
import java.util.logging.Logger;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import tp1.impl.GenericExceptionMapper;
import tp1.server.resource.SpreadsheetsResource;
import tp1.util.Discovery;
import tp1.util.InsecureHostnameVerifier;;

public class SpreadsheetsServer {

	private static Logger Log = Logger.getLogger(SpreadsheetsServer.class.getName());

	static {
		System.setProperty("java.net.preferIPv4Stack", "true");
		System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s: %5$s\n");
	}
	
	public static final int PORT = 8080;
	public static final String SERVICE = "sheets";
	public static String spreadsheetsDomain;
	public static Discovery sheetsDiscovery;
	
	public static void main(String[] args) {
		
		spreadsheetsDomain =  args.length > 0 ? args[0] : "?";
				
		try {		
		String ip = InetAddress.getLocalHost().getHostAddress();
				
		//HTTPS
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());
				
		ResourceConfig config = new ResourceConfig();
		config.register(SpreadsheetsResource.class);
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
