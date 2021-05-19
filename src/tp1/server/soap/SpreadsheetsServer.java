package tp1.server.soap;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import jakarta.xml.ws.Endpoint;
import tp1.server.ws.SpreadsheetsWS;
import tp1.util.Discovery;
import tp1.util.InsecureHostnameVerifier;

public class SpreadsheetsServer {
	
	private static Logger Log = Logger.getLogger(SpreadsheetsServer.class.getName());

	static {
		System.setProperty("java.net.preferIPv4Stack", "true");
		System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s: %5$s\n");
	}

	public static final int PORT = 8080;
	public static final String SERVICE = "sheets";												
	public static final String SOAP_SPREADSHEETS_PATH = "/soap/spreadsheets";
	public static String spreadsheetsDomain;
	public static Discovery sheetsDiscovery;
	
	public static void main(String[] args) {
		
		spreadsheetsDomain = args.length > 0 ? args[0] : "?";
		
		try {
			String ip = InetAddress.getLocalHost().getHostAddress();
			
			//HTTPS
			String serverURI = String.format("https://%s:%s/soap", ip, PORT);
			
			HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());
			
			HttpsConfigurator configurator = new HttpsConfigurator(SSLContext.getDefault());
			
			HttpsServer server = HttpsServer.create(new InetSocketAddress(ip, PORT), 0);
			
			server.setHttpsConfigurator(configurator);
			//
			
			server.setExecutor(Executors.newCachedThreadPool());								//TRATAMENTO DE CONCORRENCIA DO LADO SERVIDOR
			
			String serviceName = spreadsheetsDomain + ":" + SERVICE;
			
			sheetsDiscovery = new Discovery(serviceName, serverURI, Discovery.DEFAULT);
			

			Endpoint soapSpreadsheetsEndpoint = Endpoint.create(new SpreadsheetsWS());
			
			soapSpreadsheetsEndpoint.publish(server.createContext(SOAP_SPREADSHEETS_PATH));
			
			server.start();

			Log.info(String.format("%s Server ready @ %s\n",  SERVICE, serverURI));
			sheetsDiscovery.start();
			

			//More code can be executed here...
		} catch( Exception e) {
			Log.severe(e.getMessage());
		}
	}
}
