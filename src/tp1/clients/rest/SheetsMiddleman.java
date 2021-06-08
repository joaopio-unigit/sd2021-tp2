package tp1.clients.rest;

import java.net.URI;
import java.sql.Timestamp;

import javax.net.ssl.HttpsURLConnection;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;

import com.google.gson.Gson;

import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import tp1.api.service.rest.RestSpreadsheets;
import tp1.google.replies.GoogleSheetsReturn;
import tp1.util.InsecureHostnameVerifier;

public class SheetsMiddleman {

	private final static int MAX_RETRIES = 3;
	private final static long RETRY_PERIOD = 10000;
	private final static int CONNECTION_TIMEOUT = 10000;
	private final static int REPLY_TIMEOUT = 1000;
	private final static String REQUEST = "Making a request to ";
	private final static String TIMEOUT = "Connection timeout!!";
	private final static String RETRY_CONNECTION = "Retrying to connect.";
	private static final String SUCCESS = "Success";
	private static final String ERROR_STATUS = "Error, HTTP error status: ";

	private static final String GOOGLE_SHEETS_API_KEY = "AIzaSyDan0PpAHPQh0eEQ2NDc6qf1QxdzOzWVsg";
	private static final String GOOGLE_SHEETS_PATH = "https://sheets.googleapis.com/v4/spreadsheets/";
	private static final String VALUES = "/values";
	private static final String KEY_PARAM = "key";
	private static final String GOOGLE_APIS = "googleapis";
	private static final int SHEETID_POS = 3;

	private URI sheetsServerURI;
	private WebTarget target;
	private Gson json;

	public SheetsMiddleman() {
		sheetsServerURI = null;
		json = new Gson();
	}

	public void setSheetsServerURI(URI uri) {
		sheetsServerURI = uri;
		setUpConnection();
	}

	public URI getSheetsServerURI() {
		return sheetsServerURI;
	}

	public String[][] getSpreadsheetValues(String sheetURL, String userId, String range, boolean rangeStoredInCache,
			Timestamp twClient, String serverSecret) {
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());

		Client client = createClient();

		WebTarget localTarget;
		if (sheetURL.contains(GOOGLE_APIS)) {
			localTarget = getGoogleTarget(sheetURL, client, range);
		}
		else
			localTarget = client.target(sheetURL).path(userId).path(range).queryParam("twclient", twClient).queryParam("secret", serverSecret); // BUILDING THE PATH

		System.out.println(REQUEST + localTarget.getUri().toString());

		int retries = 0;
		boolean success = false;
		String[][] rangeValues = null;

		while (!success && retries < MAX_RETRIES) {
			try {

				Response r = localTarget.request().accept(MediaType.APPLICATION_JSON).get(); // MAKING THE REQUEST

				if (r.getStatus() == Status.OK.getStatusCode() && r.hasEntity()) {
					System.out.println(SUCCESS);
					if (sheetURL.contains(GOOGLE_APIS)) {
						GoogleSheetsReturn googleSheet = json.fromJson(r.readEntity(String.class), GoogleSheetsReturn.class);
						rangeValues = googleSheet.getValues();
					}
					else	
						rangeValues = r.readEntity(String[][].class);
				} else
					System.out.println(ERROR_STATUS + r.getStatus());

				success = true;

			} catch (ProcessingException pe) {
				if (rangeStoredInCache)
					return null;

				retries++;
				connetionFailure(pe);
			}
		}
		return rangeValues;
	}
	
	public Timestamp getTWServer(String sheetURL, String serverSecret) {
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());

		Client client = createClient();

		WebTarget localTarget = client.target(sheetURL).path("cache").queryParam("secret", serverSecret); // BUILDING THE PATH
		
		System.out.println(REQUEST + localTarget.getUri().toString());

		int retries = 0;
		boolean success = false;
		Timestamp twServer = null;
		
		while (!success && retries < MAX_RETRIES) {
			try {

				Response r = localTarget.request().accept(MediaType.APPLICATION_JSON).get(); // MAKING THE REQUEST

				if (r.getStatus() == Status.OK.getStatusCode() && r.hasEntity()) {
					System.out.println(SUCCESS);
						twServer = r.readEntity(Timestamp.class);
				} else
					System.out.println(ERROR_STATUS + r.getStatus());

				success = true;

			} catch (ProcessingException pe) {
				retries++;
				connetionFailure(pe);
			}
		}
		
		return twServer;
	}
	
	public void deleteUserSpreadsheets(String userId, String serverSecret) {

		int retries = 0;
		boolean success = false;

		while (!success && retries < MAX_RETRIES) {
			try {
				Response r = target.path(RestSpreadsheets.DELETESHEETS).path(userId).queryParam("secret", serverSecret)
						.request().accept(MediaType.APPLICATION_JSON).delete();

				success = true;

				if (r.getStatus() == Status.OK.getStatusCode() && r.hasEntity()) {
					System.out.println(SUCCESS);
				} else
					System.out.println(ERROR_STATUS + r.getStatus());

			} catch (ProcessingException pe) {
				retries++;
				connetionFailure(pe);
			}
		}
	}

	private void setUpConnection() {
		Client client = createClient();
		target = client.target(sheetsServerURI.toString()).path(RestSpreadsheets.PATH);
	}

	private Client createClient() {
		// HTTPS
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());

		ClientConfig config = new ClientConfig();
		config.property(ClientProperties.CONNECT_TIMEOUT, CONNECTION_TIMEOUT);
		config.property(ClientProperties.READ_TIMEOUT, REPLY_TIMEOUT);
		return ClientBuilder.newClient(config);
	}

	private void connetionFailure(ProcessingException pe) {
		System.out.println(TIMEOUT);
		pe.printStackTrace();
		try {
			Thread.sleep(RETRY_PERIOD);
		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}
		System.out.println(RETRY_CONNECTION);
	}

	// GOOGLE

	private WebTarget getGoogleTarget(String sheetURL, Client client, String range) {
		String sheetId = sheetURL.split("/")[SHEETID_POS];

		return client.target(GOOGLE_SHEETS_PATH).path(sheetId).path(VALUES).path(range).queryParam(KEY_PARAM,
				GOOGLE_SHEETS_API_KEY);

	}
}
