package tp1.clients.soap;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

import javax.net.ssl.HttpsURLConnection;
import javax.xml.namespace.QName;
import jakarta.xml.ws.BindingProvider;
import com.sun.xml.ws.client.BindingProviderProperties;

import jakarta.xml.ws.Service;
import jakarta.xml.ws.WebServiceException;
import tp1.api.service.soap.SoapUsers;
import tp1.api.service.soap.UsersException;
import tp1.server.ws.UsersWS;
import tp1.util.InsecureHostnameVerifier;

public class UsersMiddleman {

	public final static String USERS_WSDL = "/users/?wsdl";

	private final static int MAX_RETRIES = 10;
	private final static long RETRY_PERIOD = 1010;
	private final static int CONNECTION_TIMEOUT = 10000;
	private final static int REPLY_TIMEOUT = 10000;
	private final static String TIMEOUT = "UsersM - Connection timeout!!";
	private final static String RETRY_CONNECTION = "Retrying to connect.";

	private SoapUsers users;
	private String url;

	public UsersMiddleman() {
		users = null;
		url = null;
	}

	public String getUsersServerURL() {
		return url;
	}

	public void setUpConnection() {
		
		//HTTPS
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());

		boolean success = false;
		while (!success) {
			try {
				QName QNAME = new QName(SoapUsers.NAMESPACE, SoapUsers.NAME);
				Service service = Service.create(new URL(url + USERS_WSDL), QNAME);
				users = service.getPort(tp1.api.service.soap.SoapUsers.class);
				success = true;
			} catch (WebServiceException | MalformedURLException e) {
				System.err.println("Could not contact the server: " + e.getMessage());
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
				}
			}
		}
		((BindingProvider) users).getRequestContext().put(BindingProviderProperties.CONNECT_TIMEOUT,CONNECTION_TIMEOUT);
		((BindingProvider) users).getRequestContext().put(BindingProviderProperties.REQUEST_TIMEOUT, REPLY_TIMEOUT);

	}

	public boolean hasUser(String userId) {
		boolean found = true;

		boolean success = false;
		int numberOfTries = 0;

		while (!success && numberOfTries < MAX_RETRIES) {
			try {
				users.hasUser(userId);
				success = true;
			} catch (UsersException e) {
				if (e.getMessage().equals(UsersWS.NOT_FOUND))
					found = false;
				
				success = true;
			} catch (WebServiceException wse) {
				numberOfTries++;
				connectionFailure(wse);
			}
		}
		return found;
	}

	public boolean checkPassword(String userId, String password) {
		boolean checked = true;

		boolean success = false;
		int numberOfTries = 0;

		while (!success && numberOfTries < MAX_RETRIES) {
			try {
				users.getUser(userId, password);
				success = true;
			} catch (UsersException e) {
				if (e.getMessage().equals(UsersWS.FORBIDDEN))
					checked = false; 
					
				success = true;
			} catch (WebServiceException wse) {
				numberOfTries++;
				connectionFailure(wse);
			}
		}
		
		if(numberOfTries == MAX_RETRIES)
			checked = false;
		
		return checked;

	}

	public void setUsersServerURI(URI uri) {
		url = uri.toString();
		setUpConnection();
	}

	private void connectionFailure(WebServiceException wse) {
		System.out.println(TIMEOUT);
		wse.printStackTrace();
		try {
			Thread.sleep(RETRY_PERIOD);
		} catch (InterruptedException ie) {
		}
		System.out.println(RETRY_CONNECTION);
	}
}
