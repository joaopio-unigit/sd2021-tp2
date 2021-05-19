package tp1.clients.soap;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

import javax.net.ssl.HttpsURLConnection;
import javax.xml.namespace.QName;

import com.sun.xml.ws.client.BindingProviderProperties;

import jakarta.xml.ws.BindingProvider;
import jakarta.xml.ws.Service;
import jakarta.xml.ws.WebServiceException;
import tp1.api.service.soap.SoapSpreadsheets;
import tp1.util.InsecureHostnameVerifier;
import tp1.api.service.soap.SheetsException;

public class SheetsMiddleman {

	private final static String SPREADSHEET_WSDL = "/spreadsheets/?wsdl";

	private final static int MAX_RETRIES = 3;
	private final static long RETRY_PERIOD = 10000;
	private final static int CONNECTION_TIMEOUT = 10000;
	private final static int REPLY_TIMEOUT = 1000;
	private final static int INFINITE_RETRYS = -1;
	private final static int WITH_CACHE_NUMBER_OF_RETRYS = 3;
	private final static String REQUEST = "Making a request to ";
	private final static String TIMEOUT = "Connection timeout!!";
	private final static String RETRY_CONNECTION = "Retrying to connect.";

	public SoapSpreadsheets sheets;
	public String serverURL;

	public SheetsMiddleman() {
		sheets = null;
		serverURL = null;
	}

	public void setSheetsServerURI(URI uri) {
		serverURL = uri.toString();
		setupConnection(serverURL, INFINITE_RETRYS);
	}

	public String[][] getSpreadsheetValues(String sheetURL, String userId, String range, int numberOfTries) {

		String[] urlInfo = sheetURL.split("/");
		int sheetIdIndex = urlInfo.length - 1;

		String sheetId = urlInfo[sheetIdIndex];

		String url = sheetURL.replace(sheetId, "");

		String[][] rangeValues = null;
		if(!setupConnection(url, WITH_CACHE_NUMBER_OF_RETRYS))
			return rangeValues;

		int maxTries = MAX_RETRIES;
		int retries = 0;
		boolean success = false;

		if(numberOfTries > 0)
			maxTries = numberOfTries;

		while (!success && retries < maxTries) {

			try {
				System.out.println(REQUEST + url);
				rangeValues = sheets.importRange(sheetId, userId, range);
				success = true;
			} catch (SheetsException e) {
				e.printStackTrace();
				setupConnection(serverURL, INFINITE_RETRYS);
				return null;
			} catch (WebServiceException wse) {
				retries++;
				connectionFailure(wse);
			}
		}

		setupConnection(serverURL, WITH_CACHE_NUMBER_OF_RETRYS);
		return rangeValues;
	}

	public String getSheetsServerURL() {
		return serverURL;
	}

	public void deleteUserSpreadsheets(String userId) {

		setupConnection(serverURL, INFINITE_RETRYS);

		boolean success = false;
		int numberOfTries = 0;

		while (!success && numberOfTries < MAX_RETRIES) {
			try {
				sheets.deleteUserSpreadsheets(userId);
				success = true;
			} catch (SheetsException e) {
				e.printStackTrace();
				success = true;
			} catch (WebServiceException wse) {
				numberOfTries++;
				connectionFailure(wse);
			}
		}
	}

	private boolean setupConnection(String serverUrl, int numberOfTries) {
		
		//HTTPS
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());
	
		boolean success = false;
		int tryCounter = 0;
		while (!success) {
			try {
				QName QNAME = new QName(SoapSpreadsheets.NAMESPACE, SoapSpreadsheets.NAME);
				Service service = Service.create(new URL(serverUrl + SPREADSHEET_WSDL), QNAME);
				sheets = service.getPort(tp1.api.service.soap.SoapSpreadsheets.class);
				success = true;
			} catch (WebServiceException | MalformedURLException e) {
				System.err.println("Could not contact the server: " + e.getMessage());
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
			tryCounter++;
			if(tryCounter == numberOfTries) 
				return false;
		}
	
		((BindingProvider) sheets).getRequestContext().put(BindingProviderProperties.CONNECT_TIMEOUT,CONNECTION_TIMEOUT);
		((BindingProvider) sheets).getRequestContext().put(BindingProviderProperties.REQUEST_TIMEOUT, REPLY_TIMEOUT);
		return true;
	}

	private void connectionFailure(WebServiceException wse) {
		System.out.println(TIMEOUT);
		wse.printStackTrace();
		try {
			Thread.sleep(RETRY_PERIOD);
		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}
		System.out.println(RETRY_CONNECTION);
	}
}
