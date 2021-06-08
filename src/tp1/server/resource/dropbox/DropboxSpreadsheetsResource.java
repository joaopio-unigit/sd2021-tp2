package tp1.server.resource.dropbox;

import java.net.URI;
import java.sql.Timestamp;
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

	private static final String SHEET_ID_DELIMITER = "--";
	private static final String SAME_TW = "SAME TW";


	private final Map<String, Map<String, String[][]>> cache; //CACHE
	private final Map<String, Map<String, Timestamp>> ttls;	//CACHE
	private final Map<String, Timestamp> TWserver; //CACHE
	private final Map<String, Timestamp> TWclient; //CACHE
	private final long validTime = 20000; //CACHE
	private ExecutorService exec; //CACHE

	private static Logger Log = Logger.getLogger(DropboxSpreadsheetsResource.class.getName());

	private Discovery discovery;
	private UsersMiddleman usersM;
	private SheetsMiddleman sheetsM;
	private DropboxMiddleman dropboxM;

	public DropboxSpreadsheetsResource() {
		cache = new HashMap<String, Map<String, String[][]>>();	//CACHE
		ttls = new HashMap<String, Map<String, Timestamp>>(); //CACHE
		TWserver = new HashMap<String, Timestamp>(); //CACHE
		TWclient = new HashMap<String, Timestamp>(); //CACHE
		exec = Executors.newCachedThreadPool(); //CACHE

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
			
			TWserver.put(sheetURL, new Timestamp(System.currentTimeMillis()) );
			
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
		
		TWserver.remove(sheet.getSheetURL());
	}

	@Override
	public Spreadsheet getSpreadsheet(String sheetId, String userId, String password, Long version) {
		Log.info("getSpreadsheet : " + sheetId + "; userId = " + userId + "; pwd = " + password);

		if (sheetId == null || userId == null)
			throw new WebApplicationException(Status.BAD_REQUEST);

		checkValidUserId(userId);
		
		checkUserPassword(userId, password);
		
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

				return getSpreadsheetImportRanges(sheetURL, range, userIdDomain);
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
		
		TWserver.put(sheet.getSheetURL(), new Timestamp(System.currentTimeMillis()) );
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
		
		TWserver.put(sheet.getSheetURL(), new Timestamp(System.currentTimeMillis()) );
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
		
		TWserver.put(sheet.getSheetURL(), new Timestamp(System.currentTimeMillis()) );
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
	public String[][] importRange(String sheetId, String userId, String range, Timestamp twClient, String secret, Long version) {
		Log.info("importRange : " + sheetId + "; userId = " + userId + "; range = " + range);

		if(!secret.equals(DropboxSpreadsheetsServer.serverSecret))
			throw new WebApplicationException(Status.BAD_REQUEST);
		
		String sheetOwner = sheetId.split(SHEET_ID_DELIMITER)[0];
		Spreadsheet sheet = dropboxM.getSpreadsheet(sheetOwner, sheetId);

		checkIfSheetExists(sheet);

		if (!sheet.getSharedWith().contains(userId))
			throw new WebApplicationException(Status.FORBIDDEN);

		if(TWserver.get(sheet.getSheetURL()) != null && twClient != null && TWserver.get(sheet.getSheetURL()).compareTo(twClient) == 0) 
			return new String[][] {{SAME_TW}};
		
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

						return getSpreadsheetImportRanges(sheetURL, range, userIdDomain);
					}
				});

		return cellR.extractRangeValuesFrom(rangeValues);
	}

	@Override
	public Timestamp getTWServer(String sheetURL, String secret) {
		if(!secret.equals(DropboxSpreadsheetsServer.serverSecret))
			throw new WebApplicationException(Status.BAD_REQUEST);
		
		Timestamp twServer = TWserver.get(sheetURL);
		if(twServer == null)
			throw new WebApplicationException(Status.BAD_REQUEST);
		
		return twServer;
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

	private void insertNewValuesInCache(String sheetURL, String range, String[][] newRangeValues) {

		Map<String, String[][]> sheetCachedRanges = cache.get(sheetURL);
		if(sheetCachedRanges == null) {
			sheetCachedRanges = new HashMap<String, String[][]>();
		}
		
		sheetCachedRanges.put(range, newRangeValues);
		
		cache.put(sheetURL, sheetCachedRanges);
		
		updateTTLs(sheetURL, range);
	}
	
	private void updateTTLs(String sheetURL, String range) {
		Map<String, Timestamp> sheetRangesTimestamps = ttls.get(sheetURL);
		if(sheetRangesTimestamps == null) {
			sheetRangesTimestamps = new HashMap<String, Timestamp>();
		}	
		
		sheetRangesTimestamps.put(range, new Timestamp(System.currentTimeMillis() + validTime));
		
		ttls.put(sheetURL, sheetRangesTimestamps);
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
	
	private String[][] getSpreadsheetImportRanges(String sheetURL, String range, String userIdDomain) {
		
		boolean rangeStoredInCache = false;
		boolean validCachedRange = false;
		
		Timestamp currTimestamp = new Timestamp(System.currentTimeMillis());

		if(ttls.get(sheetURL) != null) {
			Timestamp rangeTTL = ttls.get(sheetURL).get(range); 
			if(rangeTTL != null) {
				rangeStoredInCache = true;
				
				if(rangeTTL.compareTo(currTimestamp) >= 0)
					validCachedRange = true;
			}
		}		
		
		String[][] sheetValues;
		
		if(validCachedRange) {
			//IR BUSCAR A CACHE OS VALORES E UTILIZAR
			sheetValues = cache.get(sheetURL).get(range);
		}
		else {
			Timestamp twClient = TWclient.get(sheetURL);					
			
			//IR BUSCAR NOVOS VALORES
			sheetValues = sheetsM.getSpreadsheetValues(sheetURL, userIdDomain, range, rangeStoredInCache, twClient, DropboxSpreadsheetsServer.serverSecret);
		
			//SE CONSEGUIU CONTACTAR O SERVIDOR
			if(sheetValues != null) {
				if(sheetValues[0][0].equals(SAME_TW)) {
					//UPDATE DO Tc
					updateTTLs(sheetURL, range);
					return cache.get(sheetURL).get(range);
				}
				else {
					//GUARDA NOVOS VALORES EM CACHE
					exec.execute(()-> {insertNewValuesInCache(sheetURL, range, sheetValues);});
					//INSERE O NOVO TW-CLIENT
					TWclient.put(sheetURL, sheetsM.getTWServer(sheetURL, DropboxSpreadsheetsServer.serverSecret));
					//RETORNA OS NOVOS VALORES
				}
			}
			else {
				//VAI BUSCAR OS VALORES NAO VALIDOS
				//SE HAVIA VALORES GUARDADOS EM CACHE
				if(rangeStoredInCache)
					return cache.get(sheetURL).get(range);
				else
					return null;
			}
		}
		
		return sheetValues;
	}
}
