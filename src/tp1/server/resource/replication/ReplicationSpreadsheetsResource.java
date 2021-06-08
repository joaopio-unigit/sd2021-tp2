package tp1.server.resource.replication;

import java.net.URI;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import java.util.logging.Logger;

import com.google.gson.Gson;

import jakarta.inject.Singleton;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import jakarta.ws.rs.core.UriBuilder;
import tp1.api.Spreadsheet;
import tp1.api.engine.AbstractSpreadsheet;
import tp1.api.service.rest.ReplicationRestSpreadsheets;
import tp1.api.service.rest.RestSpreadsheets;
import tp1.clients.rest.SheetsMiddleman;
import tp1.clients.rest.UsersMiddleman;
import tp1.impl.engine.SpreadsheetEngineImpl;
import tp1.replication.ReplicationManager;
import tp1.replication.Tasks;
import tp1.replication.json.ExecutedTasks;
import tp1.replication.tasks.CreateSpreadsheetTask;
import tp1.replication.tasks.DeleteSpreadsheetTask;
import tp1.replication.tasks.DeleteUserSpreadsheetsTask;
import tp1.replication.tasks.ShareSpreadsheetTask;
import tp1.replication.tasks.UnshareSpreadsheetTask;
import tp1.replication.tasks.UpdateCellTask;
import tp1.server.rest.replication.ReplicationSpreadsheetsServer;
import tp1.server.rest.UsersServer;
import tp1.util.CellRange;
import tp1.util.Discovery;

@Singleton
public class ReplicationSpreadsheetsResource implements ReplicationRestSpreadsheets {

	private static final boolean CLIENT_DEFAULT_RETRIES = false;

	private static final String REDIRECTING = "Request made to a secondary server. Redirecting...\n";
	private static final String REDIRECTING_OUTDATED = "Request made to an outdated secondary server. Redirecting...\n";
	private static final String UNRECOGNIZED_TASK = "Type of task not recognized";
	private static final int TASK_TYPE_INDEX = 0;
	private static final int TASK_JSON_INDEX = 1;

	private final Map<String, Spreadsheet> spreadsheets;
	private final Map<String, List<String>> owners;

	private final Map<String, Map<String, String[][]>> cache; // CACHE
	private ExecutorService exec; // CACHE

	private static Logger Log = Logger.getLogger(ReplicationSpreadsheetsResource.class.getName());

	private Discovery discovery;
	private UsersMiddleman usersM;
	private SheetsMiddleman sheetsM;

	private ReplicationManager replicationM;
	private Long localVersionNumber;
	private Gson json;

	public ReplicationSpreadsheetsResource() {

		spreadsheets = new HashMap<String, Spreadsheet>();
		owners = new HashMap<String, List<String>>();

		cache = new HashMap<String, Map<String, String[][]>>(); // CACHE
		exec = Executors.newCachedThreadPool(); // CACHE

		discovery = ReplicationSpreadsheetsServer.sheetsDiscovery;
		usersM = new UsersMiddleman();
		setUsersMiddlemanURI(ReplicationSpreadsheetsServer.spreadsheetsDomain);
		sheetsM = new SheetsMiddleman();
		setSheetsMiddlemanURI(ReplicationSpreadsheetsServer.spreadsheetsDomain);

		replicationM = ReplicationManager.getInstance();
		localVersionNumber = 0L;

		json = new Gson();
	}

	@Override
	public String createSpreadsheet(Spreadsheet sheet, String password) { // OPERACAO DE ESCRITA
		if (replicationM.isPrimary(ReplicationSpreadsheetsServer.serverURL)) {
			Log.info("createSpreadsheet : " + sheet + "; pwd = " + password);

			if (sheet == null || password == null)
				throw new WebApplicationException(Status.BAD_REQUEST);

			String sheetOwner = sheet.getOwner();

			if (sheetOwner == null || sheet.getRows() < 0 || sheet.getColumns() < 0) {
				Log.info("Sheet object invalid.");
				throw new WebApplicationException(Status.BAD_REQUEST);
			}

			if (!usersM.hasUser(sheetOwner, ReplicationSpreadsheetsServer.serverSecret)) {
				Log.info("User does not exist.");
				throw new WebApplicationException(Status.BAD_REQUEST);
			}

			boolean correctPassword = usersM.checkPassword(sheetOwner, password);

			if (correctPassword) {
				String sheetID = UUID.randomUUID().toString();
				sheet.setSheetId(sheetID);

				// MANDAR EXECUTAR PRIMEIRO NOS SECUNDARIOS
				Long taskAssignedVersion = replicationM.newTask(new CreateSpreadsheetTask(sheet));

				replicationM.createSpreadsheet(sheet, taskAssignedVersion);

				String sheetURL = ReplicationSpreadsheetsServer.serverURL + RestSpreadsheets.PATH + "/" + sheetID;
				sheet.setSheetURL(sheetURL);

				synchronized (this) {
					spreadsheets.put(sheetID, sheet);

					List<String> sheetOwnerSheets = owners.get(sheetOwner);
					if (sheetOwnerSheets == null) {
						sheetOwnerSheets = new ArrayList<String>();
						owners.put(sheetOwner, sheetOwnerSheets);

					}
					sheetOwnerSheets.add(sheetID);
				}

				updateLocalVersionNumber();

				return sheetID;
			} else {
				Log.info("Password is incorrect.");
				throw new WebApplicationException(Status.BAD_REQUEST);
			}
		} else {
			Log.info(REDIRECTING);
			UriBuilder uriB = UriBuilder.newInstance();
			uriB.uri(replicationM.getPrimaryServerURL()).path(RestSpreadsheets.PATH).queryParam("password", password);
			throw new WebApplicationException(Response.temporaryRedirect(uriB.build()).build());
		}
	}

	@Override
	public void deleteSpreadsheet(String sheetId, String password) { // OPERACAO DE ESCRITA
		if (replicationM.isPrimary(ReplicationSpreadsheetsServer.serverURL)) {
			Log.info("deleteSpreadsheet : sheet = " + sheetId + "; pwd = " + password);

			if (sheetId == null || password == null)
				throw new WebApplicationException(Status.BAD_REQUEST);

			Spreadsheet sheet;

			synchronized (this) {
				sheet = spreadsheets.get(sheetId);
			}

			checkIfSheetExists(sheet);

			checkUserPassword(sheet.getOwner(), password);

			// MANDAR EXECUTAR PRIMEIRO NOS SECUNDARIOS
			Long taskAssignedVersion = replicationM.newTask(new DeleteSpreadsheetTask(sheetId));
			replicationM.deleteSpreadsheet(sheetId, taskAssignedVersion);

			spreadsheets.remove(sheetId);

			updateLocalVersionNumber();
		} else {
			Log.info(REDIRECTING);
			UriBuilder uriB = UriBuilder.newInstance();
			uriB.uri(replicationM.getPrimaryServerURL()).path(RestSpreadsheets.PATH).path(sheetId)
					.queryParam("password", password);
			throw new WebApplicationException(Response.temporaryRedirect(uriB.build()).build());
		}
	}

	@Override
	public Spreadsheet getSpreadsheet(String sheetId, String userId, String password, Long version) { // OPERACAO DE
																										// LEITURA
		if (replicationM.isPrimary(ReplicationSpreadsheetsServer.serverURL) || version == null
				|| (version <= localVersionNumber)) {
			Log.info("getSpreadsheet : " + sheetId + "; userId = " + userId + "; pwd = " + password);

			if (sheetId == null || userId == null)
				throw new WebApplicationException(Status.BAD_REQUEST);

			checkValidUserId(userId);

			checkUserPassword(userId, password);

			Spreadsheet sheet;

			synchronized (this) {

				sheet = spreadsheets.get(sheetId);

				checkIfSheetExists(sheet);

				Set<String> sharedUsers = sheet.getSharedWith();

				String searchingUser = userId + "@" + ReplicationSpreadsheetsServer.spreadsheetsDomain;

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
			}

			return sheet;
		} else {
			Log.info(REDIRECTING_OUTDATED);
			exec.execute(() -> {
				checkForUpdates(localVersionNumber);
			});
			UriBuilder uriB = UriBuilder.newInstance();
			uriB.uri(replicationM.getPrimaryServerURL()).path(RestSpreadsheets.PATH).path(sheetId)
					.queryParam("userId", userId).queryParam("password", password);
			throw new WebApplicationException(Response.temporaryRedirect(uriB.build()).build());
		}
	}

	@Override
	public String[][] getSpreadsheetValues(String sheetId, String userId, String password, Long version) { // OPERACAO
																											// DE
																											// LEITURA
		if (replicationM.isPrimary(ReplicationSpreadsheetsServer.serverURL) || version == null
				|| (version <= localVersionNumber)) {

			Log.info("getSpreadsheetValues : " + sheetId + "; userId = " + userId + "; pwd = " + password);

			if (sheetId == null || userId == null || password == null)
				throw new WebApplicationException(Status.BAD_REQUEST);

			checkValidUserId(userId);

			String[][] sheetValues;

			Spreadsheet sheet;
			Set<String> sharedUsers;

			synchronized (this) {

				sheet = spreadsheets.get(sheetId);

				checkIfSheetExists(sheet);

				sharedUsers = sheet.getSharedWith();
			}

			checkUserPassword(userId, password);

			String sheetOwner = sheet.getOwner();

			String userIdDomain = userId + "@" + ReplicationSpreadsheetsServer.spreadsheetsDomain;

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

					String userIdDomain = sheetOwner + "@" + ReplicationSpreadsheetsServer.spreadsheetsDomain;

					boolean rangeStoredInCache = false;

					if (cache.get(sheetURL) != null) {
						if (cache.get(sheetURL).get(range) != null) {
							rangeStoredInCache = true;
						}
					}

					String[][] sheetValues = sheetsM.getSpreadsheetValues(sheetURL, userIdDomain, range,
							rangeStoredInCache, null, ReplicationSpreadsheetsServer.serverSecret);

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
		} else {
			Log.info(REDIRECTING_OUTDATED);
			exec.execute(() -> {
				checkForUpdates(localVersionNumber);
			});
			UriBuilder uriB = UriBuilder.newInstance();
			uriB.uri(replicationM.getPrimaryServerURL()).path(RestSpreadsheets.PATH).path(sheetId).path("values")
					.queryParam("userId", userId).queryParam("password", password);
			throw new WebApplicationException(Response.temporaryRedirect(uriB.build()).build());
		}
	}

	@Override
	public void updateCell(String sheetId, String cell, String rawValue, String userId, String password) { // OPERACAO
																											// DE
																											// ESCRITA
		if (replicationM.isPrimary(ReplicationSpreadsheetsServer.serverURL)) {
			Log.info("updateCell : " + cell + "; value = " + rawValue + "; sheet = " + sheetId + "; userId = " + userId
					+ "; pwd = " + password);

			if (sheetId == null || cell == null || rawValue == null || userId == null || password == null)
				throw new WebApplicationException(Status.BAD_REQUEST);

			checkUserPassword(userId, password);

			Spreadsheet sheet;

			synchronized (this) {
				sheet = spreadsheets.get(sheetId);
			}

			checkIfSheetExists(sheet);

			// MANDAR EXECUTAR PRIMEIRO NOS SECUNDARIOS
			Long taskAssignedVersion = replicationM.newTask(new UpdateCellTask(sheetId, cell, rawValue));
			replicationM.updateCell(sheetId, cell, rawValue, taskAssignedVersion);

			sheet.setCellRawValue(cell, rawValue);

			updateLocalVersionNumber();
		} else {
			Log.info(REDIRECTING);
			UriBuilder uriB = UriBuilder.newInstance();
			uriB.uri(replicationM.getPrimaryServerURL()).path(RestSpreadsheets.PATH).path(sheetId).path(cell)
					.queryParam("userId", userId).queryParam("password", password);
			throw new WebApplicationException(Response.temporaryRedirect(uriB.build()).build());
		}
	}

	@Override
	public void shareSpreadsheet(String sheetId, String userId, String password) { // OPERACAO DE ESCRITA
		if (replicationM.isPrimary(ReplicationSpreadsheetsServer.serverURL)) {

			Log.info("shareSpreadsheet : " + sheetId + "; userId = " + userId + "; pwd = " + password);

			if (sheetId == null || userId == null || password == null)
				throw new WebApplicationException(Status.BAD_REQUEST);

			String[] userInfo = userId.split("@");

			String userIdNoDomain = userInfo[0];
			String domain = userInfo[1];

			setUsersMiddlemanURI(domain); // MUDAR PARA O DOMINIO DO CLIENTE

			checkValidUserId(userIdNoDomain);

			setUsersMiddlemanURI(ReplicationSpreadsheetsServer.spreadsheetsDomain); // VOLTAR PARA O DOMINIO DO SERVICO
																					// DE FOLHAS

			Spreadsheet sheet;
			Set<String> sharedUsers;

			synchronized (this) {

				sheet = spreadsheets.get(sheetId);

				checkIfSheetExists(sheet);

				sharedUsers = sheet.getSharedWith();
			}

			checkUserPassword(sheet.getOwner(), password);

			if (sharedUsers.contains(userId)) {
				Log.info("Already shared with the user.");
				throw new WebApplicationException(Status.CONFLICT);
			}

			// MANDAR EXECUTAR PRIMEIRO NOS SECUNDARIOS
			Long taskAssignedVersion = replicationM.newTask(new ShareSpreadsheetTask(sheetId, userId));
			replicationM.shareSpreadsheet(sheetId, userId, taskAssignedVersion);

			sharedUsers.add(userId); // ADICIONA O UTILIZADOR X OU ENTAO X@DOMAIN SE PERTENCER A OUTRO DOMINIO

			updateLocalVersionNumber();
		} else {
			Log.info(REDIRECTING);
			UriBuilder uriB = UriBuilder.newInstance();
			uriB.uri(replicationM.getPrimaryServerURL()).path(RestSpreadsheets.PATH).path(sheetId).path("share")
					.path(userId).queryParam("password", password);
			throw new WebApplicationException(Response.temporaryRedirect(uriB.build()).build());
		}
	}

	@Override
	public void unshareSpreadsheet(String sheetId, String userId, String password) { // OPERACAO DE ESCRITA
		if (replicationM.isPrimary(ReplicationSpreadsheetsServer.serverURL)) {
			Log.info("unshareSpreadsheet : " + sheetId + "; userId = " + userId + "; pwd = " + password);

			if (sheetId == null || userId == null || password == null)
				throw new WebApplicationException(Status.BAD_REQUEST);

			String[] userInfo = userId.split("@");

			String userIdNoDomain = userInfo[0];
			String domain = userInfo[1];

			setUsersMiddlemanURI(domain); // MUDAR PARA O DOMINIO DO CLIENTE

			checkValidUserId(userIdNoDomain);

			setUsersMiddlemanURI(ReplicationSpreadsheetsServer.spreadsheetsDomain); // VOLTAR PARA O DOMINIO DO SERVICO
																					// DE FOLHAS
			Spreadsheet sheet;
			Set<String> sharedUsers;

			synchronized (this) {
				sheet = spreadsheets.get(sheetId);

				checkIfSheetExists(sheet);

				sharedUsers = sheet.getSharedWith();
			}

			if (!sharedUsers.contains(userId)) {
				Log.info("Share not existing.");
				throw new WebApplicationException(Status.NOT_FOUND);
			}

			checkUserPassword(sheet.getOwner(), password);
			// MANDAR EXECUTAR PRIMEIRO NOS SECUNDARIOS
			Long taskAssignedVersion = replicationM.newTask(new UnshareSpreadsheetTask(sheetId, userId));
			replicationM.unshareSpreadsheet(sheetId, userId, taskAssignedVersion);

			sharedUsers.remove(userId);

			updateLocalVersionNumber();
		} else {
			Log.info(REDIRECTING);
			UriBuilder uriB = UriBuilder.newInstance();
			uriB.uri(replicationM.getPrimaryServerURL()).path(RestSpreadsheets.PATH).path(sheetId).path("share")
					.path(userId).queryParam("password", password);
			throw new WebApplicationException(Response.temporaryRedirect(uriB.build()).build());
		}
	}

	@Override
	public void deleteUserSpreadsheets(String userId, String secret) { // OPERACAO DE ESCRITA
		if (replicationM.isPrimary(ReplicationSpreadsheetsServer.serverURL)) {
			Log.info("deleteUserSpreadsheets : " + userId);

			if (userId == null || !secret.equals(ReplicationSpreadsheetsServer.serverSecret))
				throw new WebApplicationException(Status.BAD_REQUEST);

			// MANDAR EXECUTAR PRIMEIRO NOS SECUNDARIOS
			Long taskAssignedVersion = replicationM.newTask(new DeleteUserSpreadsheetsTask(userId));
			replicationM.deleteUserSpreadsheets(userId, taskAssignedVersion);

			synchronized (this) {
				List<String> userIdSheets = owners.remove(userId);

				for (String sheetId : userIdSheets) {
					spreadsheets.remove(sheetId);
				}
			}
			updateLocalVersionNumber();
		} else {
			Log.info(REDIRECTING);
			UriBuilder uriB = UriBuilder.newInstance();
			uriB.uri(replicationM.getPrimaryServerURL()).path(RestSpreadsheets.PATH).path(RestSpreadsheets.DELETESHEETS)
					.path(userId).queryParam("secret", secret);
			throw new WebApplicationException(Response.temporaryRedirect(uriB.build()).build());
		}
	}

	@Override
	public String[][] importRange(String sheetId, String userId, String range, Timestamp twClient, String secret, Long version) { // OPERACAO
																												// DE
																												// LEITURA
		if (replicationM.isPrimary(ReplicationSpreadsheetsServer.serverURL) || version == null
				|| (version <= localVersionNumber)) {
			Log.info("importRange : " + sheetId + "; userId = " + userId + "; range = " + range);

			if (!secret.equals(ReplicationSpreadsheetsServer.serverSecret))
				throw new WebApplicationException(Status.BAD_REQUEST);

			Spreadsheet sheet;
			synchronized (this) {
				sheet = spreadsheets.get(sheetId);
			}

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
							String userIdDomain = sheet.getOwner() + "@"
									+ ReplicationSpreadsheetsServer.spreadsheetsDomain;

							String[][] sheetValues = sheetsM.getSpreadsheetValues(sheetURL, userIdDomain, range,
									CLIENT_DEFAULT_RETRIES, null, ReplicationSpreadsheetsServer.serverSecret);

							return sheetValues;
						}
					});

			return cellR.extractRangeValuesFrom(rangeValues);
		} else {
			Log.info(REDIRECTING_OUTDATED);
			exec.execute(() -> {
				checkForUpdates(localVersionNumber);
			});
			UriBuilder uriB = UriBuilder.newInstance();
			uriB.uri(replicationM.getPrimaryServerURL()).path(RestSpreadsheets.PATH).path(sheetId).path(userId)
					.path(range).queryParam("secret", secret);
			throw new WebApplicationException(Response.temporaryRedirect(uriB.build()).build());
		}
	}

	// OPERACOES NOS SECUNDARIOS

	@Override
	public String createSpreadsheetOperation(Spreadsheet sheet, String repSecret, Long version) {
		checkReplicationSecret(repSecret);

		checkForUpdates(version);

		replicationM.newTask(new CreateSpreadsheetTask(sheet));

		Log.info("createSpreadsheetOperation : " + sheet);

		String sheetID = sheet.getSheetId();

		String sheetOwner = sheet.getOwner();

		String sheetURL = ReplicationSpreadsheetsServer.serverURL + RestSpreadsheets.PATH + "/" + sheetID;

		sheet.setSheetURL(sheetURL);

		synchronized (this) {
			spreadsheets.put(sheetID, sheet);

			List<String> sheetOwnerSheets = owners.get(sheetOwner);

			if (sheetOwnerSheets == null) {
				sheetOwnerSheets = new ArrayList<String>();
				owners.put(sheetOwner, sheetOwnerSheets);
			}

			sheetOwnerSheets.add(sheetID);
		}
		// ATUALIZA A VERSAO LOCAL NOS SECUNDARIOS
		updateLocalVersionNumber();
		return sheetID;
	}

	@Override
	public void deleteSpreadsheetOperation(String sheetId, String repSecret, Long version) {
		checkReplicationSecret(repSecret);

		checkForUpdates(version);

		Log.info("deleteSpreadsheetOperation : sheet = " + sheetId);

		replicationM.newTask(new DeleteSpreadsheetTask(sheetId));
		spreadsheets.remove(sheetId);

		// ATUALIZA A VERSAO LOCAL NOS SECUNDARIOS
		updateLocalVersionNumber();
	}

	@Override
	public void updateCellOperation(String sheetId, String cell, String rawValue, String repSecret, Long version) {
		checkReplicationSecret(repSecret);

		checkForUpdates(version);

		replicationM.newTask(new UpdateCellTask(sheetId, cell, rawValue));

		Log.info("updateCellOperaion : " + cell + "; value = " + rawValue + "; sheet = " + sheetId);

		synchronized (this) {
			Spreadsheet sheet = spreadsheets.get(sheetId);
			checkIfSheetExists(sheet);
			sheet.setCellRawValue(cell, rawValue);
		}

		// ATUALIZA A VERSAO LOCAL NOS SECUNDARIOS
		updateLocalVersionNumber();
	}

	@Override
	public void shareSpreadsheetOperation(String sheetId, String userId, String repSecret, Long version) {
		checkReplicationSecret(repSecret);

		checkForUpdates(version);

		replicationM.newTask(new ShareSpreadsheetTask(sheetId, userId));

		Log.info("shareSpreadsheetOperation : " + sheetId + "; userId = " + userId);

		Spreadsheet sheet;
		Set<String> sharedUsers;

		synchronized (this) {
			sheet = spreadsheets.get(sheetId);
			checkIfSheetExists(sheet);
			sharedUsers = sheet.getSharedWith();
		}

		if (sharedUsers.contains(userId)) {
			Log.info("Already shared with the user.");
			throw new WebApplicationException(Status.CONFLICT);
		}

		sharedUsers.add(userId); // ADICIONA O UTILIZADOR X OU ENTAO X@DOMAIN SE PERTENCER A OUTRO DOMINIO
		// ATUALIZA A VERSAO LOCAL NOS SECUNDARIOS
		updateLocalVersionNumber();
	}

	@Override
	public void unshareSpreadsheetOperation(String sheetId, String userId, String repSecret, Long version) {
		checkReplicationSecret(repSecret);

		checkForUpdates(version);

		replicationM.newTask(new UnshareSpreadsheetTask(sheetId, userId));

		Log.info("unshareSpreadsheetOperation : " + sheetId + "; userId = " + userId);

		Spreadsheet sheet;
		Set<String> sharedUsers;

		synchronized (this) {
			sheet = spreadsheets.get(sheetId);
			checkIfSheetExists(sheet);
			sharedUsers = sheet.getSharedWith();
		}

		if (!sharedUsers.contains(userId)) {
			Log.info("Share not existing.");
			throw new WebApplicationException(Status.NOT_FOUND);
		}

		sharedUsers.remove(userId);
		// ATUALIZA A VERSAO LOCAL NOS SECUNDARIOS
		updateLocalVersionNumber();
	}

	@Override
	public void deleteUserSpreadsheetsOperation(String userId, String repSecret, Long version) {
		checkReplicationSecret(repSecret);

		checkForUpdates(version);

		replicationM.newTask(new DeleteUserSpreadsheetsTask(userId));

		Log.info("deleteUserSpreadsheetsOperation : " + userId);

		synchronized (this) {
			List<String> userIdSheets = owners.remove(userId);

			for (String sheetId : userIdSheets) {
				spreadsheets.remove(sheetId);
			}
		}
		// ATUALIZA A VERSAO LOCAL NOS SECUNDARIOS
		updateLocalVersionNumber();
	}

	@Override
	public void primaryNodeNotification(String repSecret) {
		checkReplicationSecret(repSecret);

		Log.info("primaryServerNotification");
		List<String[]> missingTasks = replicationM.getMissingTasks(localVersionNumber.intValue());
		executeTasks(missingTasks.subList(localVersionNumber.intValue(), missingTasks.size()));
	}

	@Override
	public String getExecutedTasks(int startingPos, String repSecret) {
		checkReplicationSecret(repSecret);

		Log.info("getExecutedTasks");
		return json.toJson(new ExecutedTasks(replicationM.getExecutedTasks(startingPos)));
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
		String service = domain + ":" + ReplicationSpreadsheetsServer.SERVICE;

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
		if (!usersM.hasUser(userId, ReplicationSpreadsheetsServer.serverSecret)) {
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

	private void checkReplicationSecret(String repSecret) {
		if (!repSecret.equals(ReplicationSpreadsheetsServer.replicationSecret))
			throw new WebApplicationException(Status.BAD_REQUEST);
	}

	// GESTAO DE VERSAO

	synchronized private void updateLocalVersionNumber() {
		localVersionNumber++;
	}

	synchronized private void checkForUpdates(Long receivedVersion) {
		if (receivedVersion > localVersionNumber) {
			List<String[]> missingTasks = replicationM.getMissingTasks(localVersionNumber.intValue());

			executeTasks(missingTasks.subList(0, missingTasks.size() - 1));
		}
	}

	private void executeTasks(List<String[]> missingTasks) {

		for (String[] taskJsonRepresentation : missingTasks) {
			Tasks taskType = Tasks.valueOf(taskJsonRepresentation[TASK_TYPE_INDEX]);
			switch (taskType) {
			case CreateSpreadsheetTask:
				CreateSpreadsheetTask cTask = json.fromJson(taskJsonRepresentation[TASK_JSON_INDEX],
						CreateSpreadsheetTask.class);
				createSpreadsheetOperation(cTask.getSpreadsheet(), ReplicationSpreadsheetsServer.replicationSecret,
						localVersionNumber);
				break;
			case DeleteSpreadsheetTask:
				DeleteSpreadsheetTask dTask = json.fromJson(taskJsonRepresentation[TASK_JSON_INDEX],
						DeleteSpreadsheetTask.class);
				deleteSpreadsheetOperation(dTask.getSheetId(), ReplicationSpreadsheetsServer.replicationSecret,
						localVersionNumber);
				break;
			case DeleteUserSpreadsheetsTask:
				DeleteUserSpreadsheetsTask dUTask = json.fromJson(taskJsonRepresentation[TASK_JSON_INDEX],
						DeleteUserSpreadsheetsTask.class);
				deleteUserSpreadsheetsOperation(dUTask.getUserId(), ReplicationSpreadsheetsServer.replicationSecret,
						localVersionNumber);
				break;
			case ShareSpreadsheetTask:
				ShareSpreadsheetTask sTask = json.fromJson(taskJsonRepresentation[TASK_JSON_INDEX],
						ShareSpreadsheetTask.class);
				shareSpreadsheetOperation(sTask.getSheetId(), sTask.getUserId(),
						ReplicationSpreadsheetsServer.replicationSecret, localVersionNumber);
				break;
			case UnshareSpreadsheetTask:
				UnshareSpreadsheetTask uTask = json.fromJson(taskJsonRepresentation[1], UnshareSpreadsheetTask.class);
				unshareSpreadsheetOperation(uTask.getSheetId(), uTask.getUserId(),
						ReplicationSpreadsheetsServer.replicationSecret, localVersionNumber);
				break;
			case UpdateCellTask:
				UpdateCellTask upTask = json.fromJson(taskJsonRepresentation[TASK_JSON_INDEX], UpdateCellTask.class);
				updateCellOperation(upTask.getSheetId(), upTask.getCell(), upTask.getRawValue(),
						ReplicationSpreadsheetsServer.replicationSecret, localVersionNumber);
				break;
			default:
				System.out.println(UNRECOGNIZED_TASK);
				break;
			}
		}
	}

	@Override
	public Timestamp getTWServer(String sheetURL, String secret) {
		Log.info("Not available");
		return null;
	}

}
