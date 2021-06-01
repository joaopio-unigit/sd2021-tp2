import java.util.Scanner;

import javax.net.ssl.HttpsURLConnection;

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
import tp1.api.User;
import tp1.api.service.rest.RestSpreadsheets;
import tp1.api.service.rest.RestUsers;
import tp1.util.InsecureHostnameVerifier;

public class AllClient {

	private final static int CONNECTION_TIMEOUT = 10000;
	private final static int REPLY_TIMEOUT = 10000;

	public static void main(String[] args) {
		/*
		 * UriBuilder uriB = UriBuilder.newInstance();
		 * uriB.uri("https://127.0.1.1:8080/rest").path(RestSpreadsheets.PATH).
		 * queryParam("query", "queryParam"); System.out.println(uriB.toTemplate()); URI
		 * uri = uriB.build(); System.out.println(uri.toString());
		 */

		Scanner in = new Scanner(System.in);
		System.out.println(
				"Please insert the mode you want to execute \n0-CreateUser 1-CreateSpreadsheet 2-GetSpreadsheet");
		int mode = Integer.parseInt(in.nextLine().trim());

		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());
		ClientConfig config = new ClientConfig();
		config.property(ClientProperties.CONNECT_TIMEOUT, CONNECTION_TIMEOUT);
		config.property(ClientProperties.READ_TIMEOUT, REPLY_TIMEOUT);
		Client client = ClientBuilder.newClient(config);
		WebTarget target;

		Response r = null;
		String fullUserURL = null;
		switch (mode) {
		case 0:
			System.out.println("Please insert the userService URL:");
			String userURL = in.nextLine();
			fullUserURL = userURL + RestUsers.PATH;
			System.out.println("Creating user using " + fullUserURL);
			target = client.target(fullUserURL);
			r = target.request().accept(MediaType.APPLICATION_JSON)
					.post(Entity.entity(new User("id", "nome", "email", "pass"), MediaType.APPLICATION_JSON));
			break;
		case 1:
			System.out.println("Please insert the spreadsheetsService URL:");
			String spreadsheetURL = in.nextLine();
			String fullSheetsURL = spreadsheetURL + RestSpreadsheets.PATH;

			// CREATESPREADSHEET
			target = client.target(fullSheetsURL).queryParam("password", "pass");
			System.out.println("Creating spreadsheet using " + target.getUri().toString());
			r = target.request().accept(MediaType.APPLICATION_JSON).post(
					Entity.entity(new Spreadsheet(null, "id", null, 1, 1, null, null), MediaType.APPLICATION_JSON));

			break;
		case 2:
			System.out.println("Please insert the spreadsheetsService URL:");
			spreadsheetURL = in.nextLine();
			fullSheetsURL = spreadsheetURL + RestSpreadsheets.PATH;
			// GETSPREADSHEET
			String searchSheetId = "4447519d-216b-47a4-b5bd-cdc969018ec6";
			target = client.target(fullSheetsURL).path(searchSheetId).queryParam("userId", "id").queryParam("password",
					"pass");
			System.out.println("Getting spreadsheet using " + target.getUri().toString());
			r = target.request().accept(MediaType.APPLICATION_JSON).get();
			break;
		}

		if (r.getStatus() == Status.OK.getStatusCode()) {
			switch (mode) {
			case 0:
				System.out.println("Created User nome!!");
				break;
			case 1:
				// CREATESPREADSHEET System.out.println("Created spreadsheet!!"); String
				String sheetId = r.readEntity(String.class);
				System.out.println("SHEETID: " + sheetId);
				break;
			case 2:

				// GETSPREADSHEET
				System.out.println("Got spreadsheet");
				Spreadsheet sheet = r.readEntity(Spreadsheet.class);
				System.out.println("SHEETID: " + sheet.getSheetId() + " SHEETOWNER: " + sheet.getOwner());
				break;
			}

		} else {
			if (r.hasEntity()) {
				System.out.println("TEM ENTIDADE " + r.getStatus());
				System.out.println("TIPO DE ENTIDADE " + r.getEntity().getClass().getSimpleName());
			} else {
				System.out.println("PROBLEMA EM CONTACTAR O SERVIDOR: STATUS " + r.getStatus());
			}
		}
		in.close();

	}
}
