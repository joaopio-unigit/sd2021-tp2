package tp1.server.ws;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import jakarta.jws.WebService;
import tp1.api.Spreadsheet;
import tp1.api.engine.AbstractSpreadsheet;
import tp1.api.service.soap.SoapSpreadsheets;
import tp1.api.service.soap.SheetsException;
import tp1.clients.soap.SheetsMiddleman;
import tp1.clients.soap.UsersMiddleman;
import tp1.impl.engine.SpreadsheetEngineImpl;
import tp1.server.resource.SpreadsheetsResource;
import tp1.server.soap.SpreadsheetsServer;
import tp1.server.soap.UsersServer;
import tp1.util.CellRange;
import tp1.util.Discovery;

@WebService(serviceName = SoapSpreadsheets.NAME, targetNamespace = SoapSpreadsheets.NAMESPACE, endpointInterface = SoapSpreadsheets.INTERFACE)
public class SpreadsheetsWS implements SoapSpreadsheets {

	private static final int CACHE_MAX_CONNECTION_RETRIES = 1;
	private static final int CLIENT_DEFAULT_CONECTION_RETRIES = -1;
	public static final String BAD_REQUEST = "Bad Request";
	public static final String CONFLICT = "Bad Request";
	public static final String FORBIDDEN = "Forbidden";
	public static final String NOT_FOUND = "Not Found";

	private final Map<String, Spreadsheet> spreadsheets;
	private final Map<String, List<String>> owners;

	private final Map<String, Map<String, String[][]>> cache;
	private ExecutorService exec;

	private static Logger Log = Logger.getLogger(SpreadsheetsResource.class.getName());

	private Discovery discovery;
	private UsersMiddleman usersM;
	private SheetsMiddleman sheetsM;

	public SpreadsheetsWS() {
		spreadsheets = new HashMap<String, Spreadsheet>();
		owners = new HashMap<String, List<String>>();
		cache = new HashMap<String, Map<String, String[][]>>();
		discovery = SpreadsheetsServer.sheetsDiscovery;
		usersM = new UsersMiddleman();
		sheetsM = new SheetsMiddleman();

		exec = Executors.newSingleThreadExecutor();
	}

	@Override
	public String createSpreadsheet(Spreadsheet sheet, String password) throws SheetsException {
		Log.info("createSpreadsheet : " + sheet + "; pwd = " + password);

		checkClientsConnection();

		if (sheet == null || password == null)
			throw new SheetsException(BAD_REQUEST);

		String sheetOwner = sheet.getOwner();

		if (sheetOwner == null || sheet.getRows() < 0 || sheet.getColumns() < 0) {
			Log.info("Sheet object invalid.");
			throw new SheetsException(BAD_REQUEST);
		}

		if (!usersM.hasUser(sheetOwner)) {
			Log.info("User does not exist.");
			throw new SheetsException(BAD_REQUEST);
		}

		checkUserPassword(sheetOwner, password);

		String sheetID = UUID.randomUUID().toString();

		String spreadsheetPath = SpreadsheetsServer.SOAP_SPREADSHEETS_PATH.replace("/soap", "");

		String sheetURL = sheetsM.getSheetsServerURL().toString() + spreadsheetPath + "/" + sheetID;

		sheet.setSheetId(sheetID);
		sheet.setSheetURL(sheetURL);

		spreadsheets.put(sheetID, sheet);

		List<String> sheetOwnerSheets = owners.get(sheetOwner);

		if (sheetOwnerSheets == null) {
			sheetOwnerSheets = new ArrayList<String>();
			owners.put(sheetOwner, sheetOwnerSheets);
		}

		sheetOwnerSheets.add(sheetID);

		return sheetID;
	}

	@Override
	public void deleteSpreadsheet(String sheetId, String password) throws SheetsException {
		Log.info("deleteSpreadsheet : sheet = " + sheetId + "; pwd = " + password);

		checkClientsConnection();
		
		if (sheetId == null || password == null)
			throw new SheetsException(BAD_REQUEST);

		synchronized (this) {
			Spreadsheet sheet = spreadsheets.get(sheetId);

			if (sheet == null) {
				Log.info("SheetId invalid.");
				throw new SheetsException(NOT_FOUND);
			}
			boolean correctPassword = usersM.checkPassword(sheet.getOwner(), password);
			if (correctPassword) {
				spreadsheets.remove(sheetId);
			} else { // 403
				Log.info("Password is incorrect.");
				throw new SheetsException(FORBIDDEN);
			}
		}

	}

	@Override
	public Spreadsheet getSpreadsheet(String sheetId, String userId, String password) throws SheetsException {
		Log.info("getSpreadsheet : " + sheetId + "; userId = " + userId + "; pwd = " + password);

		checkClientsConnection();

		if (sheetId == null || userId == null || password == null || password.isEmpty())
			throw new SheetsException(BAD_REQUEST);

		checkIfUserExists(userId);

		checkUserPassword(userId, password);

		Spreadsheet sheet;

		synchronized (this) {

			sheet = spreadsheets.get(sheetId);

			checkIfSheetExists(sheet);

			Set<String> sharedUsers = sheet.getSharedWith();

			String searchingUser = userId + "@" + SpreadsheetsServer.spreadsheetsDomain;

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
				throw new SheetsException(FORBIDDEN);
			}
		}

		return sheet;
	}

	@Override
	public String[][] getSpreadsheetValues(String sheetId, String userId, String password) throws SheetsException {
		Log.info("getSpreadsheetValues : " + sheetId + "; userId = " + userId + "; pwd = " + password);

		checkClientsConnection();

		if (sheetId == null || userId == null || password == null)
			throw new SheetsException(BAD_REQUEST);

		checkIfUserExists(userId);

		String[][] sheetValues;

		synchronized (this) {

			Spreadsheet sheet = spreadsheets.get(sheetId);

			checkIfSheetExists(sheet);

			Set<String> sharedUsers = sheet.getSharedWith();

			String sheetOwner = sheet.getOwner();

			checkUserPassword(userId, password);

			boolean hasAccess = true;
			String userIdDomain = userId + "@" + SpreadsheetsServer.spreadsheetsDomain;

			if (!sheet.getOwner().equals(userId)) {
				if (sharedUsers == null || sharedUsers.isEmpty()) {
					hasAccess = false;
				} else {
					if (!sharedUsers.contains(userIdDomain)) {
						hasAccess = false;
					}
				}
			}

			if (!hasAccess) {
				Log.info("User does not have access to the spreadsheet.");
				throw new SheetsException(FORBIDDEN);
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

					String userIdDomain = sheetOwner + "@" + SpreadsheetsServer.spreadsheetsDomain;

					String[][] rangeValues = null;

					if (cache.get(sheetURL) != null && cache.get(sheetURL).get(range) != null) {
						rangeValues = sheetsM.getSpreadsheetValues(sheetURL, userIdDomain, range,
								CACHE_MAX_CONNECTION_RETRIES);
					} else {
						rangeValues = sheetsM.getSpreadsheetValues(sheetURL, userIdDomain, range,
								CLIENT_DEFAULT_CONECTION_RETRIES);
					}

					if (rangeValues != null) {
						String[][] newCachedValues = rangeValues;
						exec.execute(() -> {
							insertNewValuesInCache(sheetURL, range, newCachedValues);
						});
					} else {
						Map<String, String[][]> cachedSheetValues = cache.get(sheetURL);
						return cachedSheetValues.get(range);
					}

					return rangeValues;
				}
			});
			return sheetValues;
		}
	}

	@Override
	public void updateCell(String sheetId, String cell, String rawValue, String userId, String password)
			throws SheetsException {
		Log.info("updateCell : " + cell + "; value = " + rawValue + "; sheet = " + sheetId + "; userId = " + userId
				+ "; pwd = " + password);

		checkClientsConnection();

		if (sheetId == null || cell == null || rawValue == null || userId == null || password == null)
			throw new SheetsException(BAD_REQUEST);

		checkUserPassword(userId, password);

		synchronized (this) {

			Spreadsheet sheet = spreadsheets.get(sheetId);

			checkIfSheetExists(sheet);

			sheet.setCellRawValue(cell, rawValue);
		}

	}

	@Override
	public void shareSpreadsheet(String sheetId, String userId, String password) throws SheetsException {
		Log.info("shareSpreadsheet : " + sheetId + "; userId = " + userId + "; pwd = " + password);

		checkClientsConnection();

		if (sheetId == null || userId == null || password == null)
			throw new SheetsException(BAD_REQUEST);

		String[] userInfo = userId.split("@");

		String userIdNoDomain = userInfo[0];
		String domain = userInfo[1];

		setUsersMiddlemanURI(domain);

		checkIfUserExists(userIdNoDomain);

		setUsersMiddlemanURI(SpreadsheetsServer.spreadsheetsDomain);

		synchronized (this) {

			Spreadsheet sheet = spreadsheets.get(sheetId);

			checkIfSheetExists(sheet);

			checkUserPassword(sheet.getOwner(), password);

			Set<String> sharedUsers = sheet.getSharedWith();

			if (sharedUsers != null) {
				if (sharedUsers.contains(userId)) {
					Log.info("Already shared with the user.");
					throw new SheetsException(CONFLICT);
				}
			} else {
				sharedUsers = new HashSet<String>();
				sheet.setSharedWith(sharedUsers);
			}
			sharedUsers.add(userId);
		}

	}

	@Override
	public void unshareSpreadsheet(String sheetId, String userId, String password) throws SheetsException {
		Log.info("unshareSpreadsheet : " + sheetId + "; userId = " + userId + "; pwd = " + password);

		checkClientsConnection();

		String[] userInfo = userId.split("@");

		String userIdNoDomain = userInfo[0];
		String domain = userInfo[1];

		setUsersMiddlemanURI(domain);

		checkIfUserExists(userIdNoDomain);

		setUsersMiddlemanURI(SpreadsheetsServer.spreadsheetsDomain);

		if (sheetId == null || userId == null || password == null)
			throw new SheetsException(BAD_REQUEST);

		synchronized (this) {
			Spreadsheet sheet = spreadsheets.get(sheetId);
			checkIfSheetExists(sheet);

			Set<String> sharedUsers = sheet.getSharedWith();

			if (!sharedUsers.contains(userId)) {
				Log.info("Share not existing.");
				throw new SheetsException(NOT_FOUND);
			}

			checkUserPassword(sheet.getOwner(), password);

			sharedUsers.remove(userId);
		}

	}

	@Override
	public String[][] importRange(String sheetId, String userId, String range) throws SheetsException {
		Log.info("importRange : " + sheetId + "; userId = " + userId + "; range = " + range);

		Spreadsheet sheet = spreadsheets.get(sheetId);

		if (sheet == null)
			throw new SheetsException(NOT_FOUND);

		if (!sheet.getSharedWith().contains(userId))
			throw new SheetsException(FORBIDDEN);

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

						String userIdDomain = sheet.getOwner() + "@" + SpreadsheetsServer.spreadsheetsDomain;

						String[][] sheetValues = sheetsM.getSpreadsheetValues(sheetURL, userIdDomain, range,
								CLIENT_DEFAULT_CONECTION_RETRIES);

						return sheetValues;
					}
				});

		return cellR.extractRangeValuesFrom(rangeValues);
	}

	@Override
	public void deleteUserSpreadsheets(String userId) throws SheetsException {
		Log.info("deleteUserSpreadsheets : " + userId);

		if (userId == null)
			throw new SheetsException(BAD_REQUEST);

		List<String> userIdSheets = owners.remove(userId);

		for (String sheetId : userIdSheets) {
			spreadsheets.remove(sheetId);
		}
	}

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

	private void setSheetsMiddlemanURI() {
		String service = SpreadsheetsServer.spreadsheetsDomain + ":" + SpreadsheetsServer.SERVICE;

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

	private void checkClientsConnection() {
		if (usersM.getUsersServerURL() == null)
			setUsersMiddlemanURI(SpreadsheetsServer.spreadsheetsDomain);
		if (sheetsM.getSheetsServerURL() == null)
			setSheetsMiddlemanURI();
	}

	private void insertNewValuesInCache(String sheetURL, String range, String[][] newRangeValues) {
		synchronized (this) {
			Map<String, String[][]> sheetCachedRanges = cache.get(sheetURL);
			if (sheetCachedRanges == null) {
				sheetCachedRanges = new HashMap<String, String[][]>();
			}

			sheetCachedRanges.put(range, newRangeValues);

			cache.put(sheetURL, sheetCachedRanges);
		}
	}

	private void checkIfUserExists(String userId) throws SheetsException {
		if (!usersM.hasUser(userId)) {
			Log.info("UserId invalid.");
			throw new SheetsException(NOT_FOUND);
		}
	}

	private void checkIfSheetExists(Spreadsheet sheet) throws SheetsException {
		if (sheet == null) {
			Log.info("SheetId invalid.");
			throw new SheetsException(NOT_FOUND);
		}
	}

	
	private void checkUserPassword(String userId, String password) throws SheetsException {
		boolean correctPassword = usersM.checkPassword(userId, password);
	
		if (!correctPassword) {
			Log.info("Password is incorrect.");
			throw new SheetsException(FORBIDDEN);
		}
	}
}
