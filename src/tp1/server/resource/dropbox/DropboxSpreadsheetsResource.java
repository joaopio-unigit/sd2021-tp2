package tp1.server.resource.dropbox;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import jakarta.inject.Singleton;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response.Status;
import tp1.api.Spreadsheet;
import tp1.api.engine.AbstractSpreadsheet;
import tp1.api.service.rest.RestSpreadsheets;
import tp1.clients.dropbox.DropboxMiddleman;
import tp1.clients.rest.SheetsMiddleman;
import tp1.clients.rest.UsersMiddleman;
import tp1.impl.engine.SpreadsheetEngineImpl;
import tp1.server.rest.UsersServer;
import tp1.server.rest.dropbox.DropboxSpreadsheetsServer;
import tp1.util.CellRange;
import tp1.util.Discovery;

@Singleton
public class DropboxSpreadsheetsResource implements RestSpreadsheets {

	private static final boolean CLIENT_DEFAULT_RETRIES = false;

	private static final String SHEET_ID_DELIMITER = "--";

	private final Map<String, Map<String, String[][]>> cache;
	private ExecutorService exec;

	private static Logger Log = Logger.getLogger(DropboxSpreadsheetsResource.class.getName());

	private Discovery discovery;
	private UsersMiddleman usersM;
	private SheetsMiddleman sheetsM;
	private DropboxMiddleman dropboxM;

	public DropboxSpreadsheetsResource() {
		cache = new HashMap<String, Map<String, String[][]>>();
		exec = Executors.newCachedThreadPool();

		discovery = DropboxSpreadsheetsServer.sheetsDiscovery;
		usersM = new UsersMiddleman();
		setUsersMiddlemanURI(DropboxSpreadsheetsServer.spreadsheetsDomain);
		sheetsM = new SheetsMiddleman();
		setSheetsMiddlemanURI(DropboxSpreadsheetsServer.spreadsheetsDomain);

		dropboxM = new DropboxMiddleman(DropboxSpreadsheetsServer.spreadsheetsDomain, DropboxSpreadsheetsServer.apiKey, DropboxSpreadsheetsServer.apiSecret, DropboxSpreadsheetsServer.accessTokenStr);
		if (DropboxSpreadsheetsServer.stateReset)
			dropboxM.resetState();

		if (!dropboxM.createDomain(null)) {
			Log.info("Existing domain or failed to create domain's folder.");
		}
	}

	@Override
	public String createSpreadsheet(Spreadsheet sheet, String password) {
		Log.info("createSpreadsheet : " + sheet + "; pwd = " + password);

		if (sheet == null || password == null)
			throw new WebApplicationException(Status.BAD_REQUEST);

		String sheetOwner = sheet.getOwner();

		if (sheetOwner == null || sheet.getRows() < 0 || sheet.getColumns() < 0) {
			Log.info("Sheet object invalid.");
			throw new WebApplicationException(Status.BAD_REQUEST);
		}

		if (!usersM.hasUser(sheetOwner, DropboxSpreadsheetsServer.serverSecret)) {
			Log.info("User does not exist.");
			throw new WebApplicationException(Status.BAD_REQUEST);
		}

		boolean correctPassword = usersM.checkPassword(sheetOwner, password);

		if (correctPassword) {
			String sheetID = sheet.getOwner() + SHEET_ID_DELIMITER + UUID.randomUUID().toString();
			String sheetURL = sheetsM.getSheetsServerURI().toString() + RestSpreadsheets.PATH + "/" + sheetID;

			sheet.setSheetId(sheetID);
			sheet.setSheetURL(sheetURL);

			dropboxM.createDomain(sheetOwner);

			if (!dropboxM.uploadSpreadsheet(sheet)) {
				Log.info("Failed to create spreadsheet in Dropbox.");
				throw new WebApplicationException(Status.BAD_REQUEST);
			}

			return sheetID;
		} else {
			Log.info("Password is incorrect.");
			throw new WebApplicationException(Status.BAD_REQUEST);
		}
	}

	@Override
	public void deleteSpreadsheet(String sheetId, String password) {
		Log.info("deleteSpreadsheet : sheet = " + sheetId + "; pwd = " + password);

		if (sheetId == null || password == null)
			throw new WebApplicationException(Status.BAD_REQUEST);

		Spreadsheet sheet;

		String sheetOwner = sheetId.split(SHEET_ID_DELIMITER)[0];
		sheet = dropboxM.getSpreadsheet(sheetOwner, sheetId);

		checkIfSheetExists(sheet);

		checkUserPassword(sheet.getOwner(), password);

		if (!dropboxM.deleteSpreadsheet(sheetOwner, sheetId)) {
			Log.info("Failed to delete spreadsheet in Dropbox.");
			throw new WebApplicationException(Status.BAD_REQUEST);
		}
	}

	@Override
	public Spreadsheet getSpreadsheet(String sheetId, String userId, String password, Long version) {
		Log.info("getSpreadsheet : " + sheetId + "; userId = " + userId + "; pwd = " + password);

		if (sheetId == null || userId == null)
			throw new WebApplicationException(Status.BAD_REQUEST);

		checkValidUserId(userId);

		System.out.println("USERID VALIDO");
		
		checkUserPassword(userId, password);

		System.out.println("PASSWORD CORRETA");
		
		Spreadsheet sheet;

		String sheetOwner = sheetId.split(SHEET_ID_DELIMITER)[0];
		sheet = dropboxM.getSpreadsheet(sheetOwner, sheetId);

		checkIfSheetExists(sheet);

		Set<String> sharedUsers = sheet.getSharedWith();

		String searchingUser = userId + "@" + DropboxSpreadsheetsServer.spreadsheetsDomain;

		boolean hasAccess = true;

		if (!sheet.getOwner().equals(userId)) {
			if (sharedUsers == null || sharedUsers.isEmpty()) {
				hasAccess = false;
			} else {
				if (!sharedUsers.contains(searchingUser)) {
					hasAccess = false;
				}
			}
		}

		if (!hasAccess) {
			Log.info("User does not have access to the spreadsheet.");
			throw new WebApplicationException(Status.FORBIDDEN);
		}

		return sheet;
	}

	@Override
	public String[][] getSpreadsheetValues(String sheetId, String userId, String password, Long version) {
		Log.info("getSpreadsheetValues : " + sheetId + "; userId = " + userId + "; pwd = " + password);

		if (sheetId == null || userId == null || password == null)
			throw new WebApplicationException(Status.BAD_REQUEST);

		checkValidUserId(userId);

		String[][] sheetValues;

		Spreadsheet sheet;
		Set<String> sharedUsers;

		String sheetOwner = sheetId.split(SHEET_ID_DELIMITER)[0];
		sheet = dropboxM.getSpreadsheet(sheetOwner, sheetId);

		checkIfSheetExists(sheet);

		sharedUsers = sheet.getSharedWith();

		checkUserPassword(userId, password);

		String userIdDomain = userId + "@" + DropboxSpreadsheetsServer.spreadsheetsDomain;

		if (!(sharedUsers.contains(userIdDomain) || sheetOwner.equals(userId))) {
			Log.info("UserId without access.");
			throw new WebApplicationException(Status.FORBIDDEN);
		}

		sheetValues = SpreadsheetEngineImpl.getInstance().computeSpreadsheetValues(new AbstractSpreadsheet() {

			@Override
			public String sheetId() {
				return sheet.getSheetId();
			}

			@Override
			public int rows() {
				return sheet.getRows();
			}

			@Override
			public int columns() {
				return sheet.getColumns();
			}

			@Override
			public String cellRawValue(int row, int col) {
				return sheet.getCellRawValue(row, col);
			}

			@Override
			public String[][] getRangeValues(String sheetURL, String range) {

				String userIdDomain = sheetOwner + "@" + DropboxSpreadsheetsServer.spreadsheetsDomain;

				boolean rangeStoredInCache = false;

				if (cache.get(sheetURL) != null) {
					if (cache.get(sheetURL).get(range) != null) {
						rangeStoredInCache = true;
					}
				}

				String[][] sheetValues = sheetsM.getSpreadsheetValues(sheetURL, userIdDomain, range,
						rangeStoredInCache, DropboxSpreadsheetsServer.serverSecret);

				if (sheetValues != null) {

					exec.execute(() -> {
						insertNewValuesInCache(sheetURL, range, sheetValues);
					});
				} else {
					Map<String, String[][]> cachedSheetValues = cache.get(sheetURL);
					if (cachedSheetValues != null)
						return cachedSheetValues.get(range);
				}

				return sheetValues;
			}
		});

		return sheetValues;
	}

	@Override
	public void updateCell(String sheetId, String cell, String rawValue, String userId, String password) {
		Log.info("updateCell : " + cell + "; value = " + rawValue + "; sheet = " + sheetId + "; userId = " + userId
				+ "; pwd = " + password);

		if (sheetId == null || cell == null || rawValue == null || userId == null || password == null)
			throw new WebApplicationException(Status.BAD_REQUEST);

		checkUserPassword(userId, password);

		String sheetOwner = sheetId.split(SHEET_ID_DELIMITER)[0];
		Spreadsheet sheet = dropboxM.getSpreadsheet(sheetOwner, sheetId);

		checkIfSheetExists(sheet);

		sheet.setCellRawValue(cell, rawValue);

		if (!dropboxM.uploadSpreadsheet(sheet)) {
			Log.info("Failed to upload spreadsheet to Dropbox.");
			throw new WebApplicationException(Status.BAD_REQUEST);
		}
	}

	@Override
	public void shareSpreadsheet(String sheetId, String userId, String password) {
		Log.info("shareSpreadsheet : " + sheetId + "; userId = " + userId + "; pwd = " + password);

		if (sheetId == null || userId == null || password == null)
			throw new WebApplicationException(Status.BAD_REQUEST);

		String[] userInfo = userId.split("@");

		String userIdNoDomain = userInfo[0];
		String domain = userInfo[1];

		setUsersMiddlemanURI(domain); // MUDAR PARA O DOMINIO DO CLIENTE

		checkValidUserId(userIdNoDomain);

		setUsersMiddlemanURI(DropboxSpreadsheetsServer.spreadsheetsDomain); // VOLTAR PARA O DOMINIO DO SERVICO DE
																			// FOLHAS

		Spreadsheet sheet;
		Set<String> sharedUsers;

		String sheetOwner = sheetId.split(SHEET_ID_DELIMITER)[0];
		sheet = dropboxM.getSpreadsheet(sheetOwner, sheetId);

		checkIfSheetExists(sheet);

		sharedUsers = sheet.getSharedWith();

		checkUserPassword(sheet.getOwner(), password);

		if (sharedUsers.contains(userId)) {
			Log.info("Already shared with the user.");
			throw new WebApplicationException(Status.CONFLICT);
		}

		sharedUsers.add(userId); // ADICIONA O UTILIZADOR X OU ENTAO X@DOMAIN SE PERTENCER A OUTRO DOMINIO

		if (!dropboxM.uploadSpreadsheet(sheet)) {
			Log.info("Failed to upload spreadsheet to Dropbox.");
			throw new WebApplicationException(Status.BAD_REQUEST);
		}
	}

	@Override
	public void unshareSpreadsheet(String sheetId, String userId, String password) {
		Log.info("unshareSpreadsheet : " + sheetId + "; userId = " + userId + "; pwd = " + password);

		String[] userInfo = userId.split("@");

		String userIdNoDomain = userInfo[0];
		String domain = userInfo[1];

		setUsersMiddlemanURI(domain); // MUDAR PARA O DOMINIO DO CLIENTE

		checkValidUserId(userIdNoDomain);

		setUsersMiddlemanURI(DropboxSpreadsheetsServer.spreadsheetsDomain); // VOLTAR PARA O DOMINIO DO SERVICO DE
																			// FOLHAS

		if (sheetId == null || userId == null || password == null)
			throw new WebApplicationException(Status.BAD_REQUEST);

		Spreadsheet sheet;
		Set<String> sharedUsers;

		String sheetOwner = sheetId.split(SHEET_ID_DELIMITER)[0];
		sheet = dropboxM.getSpreadsheet(sheetOwner, sheetId);

		checkIfSheetExists(sheet);

		sharedUsers = sheet.getSharedWith();

		if (!sharedUsers.contains(userId)) {
			Log.info("Share not existing.");
			throw new WebApplicationException(Status.NOT_FOUND);
		}

		checkUserPassword(sheet.getOwner(), password);

		sharedUsers.remove(userId);

		if (!dropboxM.uploadSpreadsheet(sheet)) {
			Log.info("Failed to upload spreadsheet to Dropbox.");
			throw new WebApplicationException(Status.BAD_REQUEST);
		}
	}

	@Override
	public void deleteUserSpreadsheets(String userId, String secret) {
		Log.info("deleteUserSpreadsheets : " + userId);

		if (userId == null || !secret.equals(DropboxSpreadsheetsServer.serverSecret))
			throw new WebApplicationException(Status.BAD_REQUEST);

		if (!dropboxM.deleteSpreadsheet(userId, null)) {
			Log.info("Failed to delete spreadsheet in Dropbox.");
			throw new WebApplicationException(Status.BAD_REQUEST);
		}
	}

	@Override
	public String[][] importRange(String sheetId, String userId, String range, String secret, Long version) {
		Log.info("importRange : " + sheetId + "; userId = " + userId + "; range = " + range);

		if(!secret.equals(DropboxSpreadsheetsServer.serverSecret))
			throw new WebApplicationException(Status.BAD_REQUEST);
		
		String sheetOwner = sheetId.split(SHEET_ID_DELIMITER)[0];
		Spreadsheet sheet = dropboxM.getSpreadsheet(sheetOwner, sheetId);

		checkIfSheetExists(sheet);

		if (!sheet.getSharedWith().contains(userId))
			throw new WebApplicationException(Status.FORBIDDEN);

		CellRange cellR = new CellRange(range);

		String[][] rangeValues = SpreadsheetEngineImpl.getInstance()
				.computeSpreadsheetValues(new AbstractSpreadsheet() {

					@Override
					public String sheetId() {
						return sheet.getSheetId();
					}

					@Override
					public int rows() {
						return sheet.getRows();
					}

					@Override
					public int columns() {
						return sheet.getColumns();
					}

					@Override
					public String cellRawValue(int row, int col) {
						return sheet.getCellRawValue(row, col);
					}

					@Override
					public String[][] getRangeValues(String sheetURL, String range) {
						String userIdDomain = sheet.getOwner() + "@" + DropboxSpreadsheetsServer.spreadsheetsDomain;

						String[][] sheetValues = sheetsM.getSpreadsheetValues(sheetURL, userIdDomain, range,
								CLIENT_DEFAULT_RETRIES, DropboxSpreadsheetsServer.serverSecret);

						return sheetValues;
					}
				});

		return cellR.extractRangeValuesFrom(rangeValues);
	}

	// METODOS PRIVADOS

	private void setUsersMiddlemanURI(String domain) {

		String service = domain + ":" + UsersServer.SERVICE;

		URI[] uris = discovery.knownUrisOf(service);

		while (uris == null) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			uris = discovery.knownUrisOf(service);
		}

		usersM.setUsersServerURI(uris[0]);
	}

	private void setSheetsMiddlemanURI(String domain) {
		String service = domain + ":" + DropboxSpreadsheetsServer.SERVICE;

		URI[] uris = discovery.knownUrisOf(service);

		while (uris == null) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			uris = discovery.knownUrisOf(service);
		}

		sheetsM.setSheetsServerURI(uris[0]);
	}

	private void insertNewValuesInCache(String sheetId, String range, String[][] newRangeValues) {

		Map<String, String[][]> sheetCachedRanges = cache.get(sheetId);
		if (sheetCachedRanges == null) {
			sheetCachedRanges = new HashMap<String, String[][]>();
		}

		sheetCachedRanges.put(range, newRangeValues);

		cache.put(sheetId, sheetCachedRanges);
	}

	private void checkValidUserId(String userId) {
		if (!usersM.hasUser(userId, DropboxSpreadsheetsServer.serverSecret)) {
			Log.info("UserId invalid.");
			throw new WebApplicationException(Status.NOT_FOUND);
		}
	}

	private void checkUserPassword(String userId, String password) {

		boolean correctPassword = usersM.checkPassword(userId, password);

		if (!correctPassword) {
			Log.info("Password is incorrect.");
			throw new WebApplicationException(Status.FORBIDDEN);
		}
	}

	private void checkIfSheetExists(Spreadsheet sheet) {
		if (sheet == null) {
			Log.info("SheetId invalid.");
			throw new WebApplicationException(Status.NOT_FOUND);
		}
	}

}
