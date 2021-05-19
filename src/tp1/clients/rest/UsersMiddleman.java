package tp1.clients.rest;

import java.net.URI;

import javax.net.ssl.HttpsURLConnection;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;

import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import tp1.api.service.rest.RestUsers;
import tp1.util.InsecureHostnameVerifier;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

public class UsersMiddleman {

	private final static int MAX_RETRIES = 10;
	private final static long RETRY_PERIOD = 1010;
	private final static int CONNECTION_TIMEOUT = 10000;
	private final static int REPLY_TIMEOUT = 10000;
	private final static String TIMEOUT = "Connection timeout!!";
	private final static String RETRY_CONNECTION = "Retrying to connect.";

	private URI usersServerURI;
	private WebTarget target;

	public UsersMiddleman() {
		usersServerURI = null;
	}

	public void setUsersServerURI(URI uri) {
		usersServerURI = uri;
		setUpConnection();
	}

	public URI getUsersUserverURI() {
		return usersServerURI;
	}

	public boolean hasUser(String userID) {

		Response r = getMessage(target.path(userID).path(RestUsers.VERIFYUSER));

		if (r.getStatus() == Status.NOT_FOUND.getStatusCode()) {
			return false;
		} else
			return true;
	}

	public boolean checkPassword(String sheetOwner, String password) {

		Response r = getMessage(target.path(sheetOwner).queryParam("password", password));

		if (r.getStatus() == Status.OK.getStatusCode() && r.hasEntity()) {
			return true;
		} else {
			if (r.getStatus() != Status.FORBIDDEN.getStatusCode())
				System.out.println("Error, HTTP error status: " + r.getStatus());

			return false;
		}
	}

	private void setUpConnection() {
		//HTTPS
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());
		
		ClientConfig config = new ClientConfig();
		config.property(ClientProperties.CONNECT_TIMEOUT, CONNECTION_TIMEOUT);
		config.property(ClientProperties.READ_TIMEOUT, REPLY_TIMEOUT);
		Client client = ClientBuilder.newClient(config);
		target = client.target(usersServerURI.toString()).path(RestUsers.PATH);
	}

	private Response getMessage(WebTarget target) {
		Response r = null;
		int retries = 0;
		boolean success = false;

		while (!success && retries < MAX_RETRIES) {
			try {
				r = target.request().accept(MediaType.APPLICATION_JSON).get();
				success = true;
			} catch (ProcessingException pe) {
				System.out.println(TIMEOUT);
				pe.printStackTrace();
				retries++;
				try {Thread.sleep(RETRY_PERIOD);} catch (InterruptedException ie) {ie.printStackTrace();}
				System.out.println(RETRY_CONNECTION);
			}
		}
		return r;
	}
}
