package tp1.server.ws;

import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import jakarta.jws.WebService;
import tp1.api.User;
import tp1.api.service.soap.SoapUsers;
import tp1.api.service.soap.UsersException;
import tp1.clients.soap.SheetsMiddleman;
import tp1.server.resource.UsersResource;
import tp1.server.soap.SpreadsheetsServer;
import tp1.server.soap.UsersServer;
import tp1.util.Discovery;

@WebService(serviceName = SoapUsers.NAME,
			targetNamespace = SoapUsers.NAMESPACE,
			endpointInterface = SoapUsers.INTERFACE)
public class UsersWS implements SoapUsers{
	
	public static final String BAD_REQUEST = "Bad Request";
	public static final String CONFLICT = "Bad Request";
	public static final String FORBIDDEN = "Forbidden";
	public static final String NOT_FOUND = "Not Found";

	private final Map<String, User> users;
	private static Logger Log = Logger.getLogger(UsersResource.class.getName());
	
	private Discovery discovery;											//ADICIONAR A CAPACIDADE DE O WS COMUNICAR COM OUTROS WS
	private SheetsMiddleman sheetsM;
	private ExecutorService exec;
	
	private static final int MAX_TRIES = 11;
	
	public UsersWS() {
		users  = new HashMap<String, User>();
		discovery = UsersServer.usersDiscovery;
		sheetsM = new SheetsMiddleman();
		exec = Executors.newCachedThreadPool();
	}
	
	@Override
	public String createUser(User user) throws UsersException {
		Log.info("createUser : " + user);

		// Check if user is valid, if not return HTTP BAD_REQUEST (400)
		if (user.getUserId() == null || user.getPassword() == null || user.getFullName() == null
				|| user.getEmail() == null) {
			Log.info("User object invalid.");
			throw new UsersException(BAD_REQUEST);
		}

		synchronized (this) {

			// Check if userId does not exist exists, if not return HTTP CONFLICT (409)
			if (users.containsKey(user.getUserId())) {
				Log.info("User already exists.");
				throw new UsersException(CONFLICT);
			}

			// Add the user to the map of users
			users.put(user.getUserId(), user);
		}

		return user.getUserId();
	}

	@Override
	public User getUser(String userId, String password) throws UsersException {
		Log.info("getUser : user = " + userId + "; pwd = " + password);

		User user;

		synchronized (this) {

			user = users.get(userId);

			// Check if user exists, if not return HTTP NOT_FOUND(404)
			checkIfUserExists(user);

			// Check if the password is correct, if not return HTTP FORBIDDEN(403)
			checkUserPassword(user, password);
		}

		return user;
	}

	@Override
	public User updateUser(String userId, String password, User user) throws UsersException {
		Log.info("updateUser : user = " + userId + "; pwd = " + password + " ; user = " + user);

		// Check if userID and password are valid, if not return HTTP BAD_REQUEST (400)
		if (userId == null || password == null) {
			Log.info("UserId or passwrod null.");
			throw new UsersException(BAD_REQUEST);
		}

		User storedUser;

		synchronized (this) {

			storedUser = users.get(userId);

			// Check if the User was stored in memory before being able to update, if not
			// return HTTP NOT FOUND (404)
			checkIfUserExists(storedUser);

			// Check if the password is correct, if not return HTTP FORBIDDEN (403)
			checkUserPassword(storedUser, password);

			String updatedEmail = user.getEmail();
			if (updatedEmail != null)
				storedUser.setEmail(updatedEmail);

			String updatedPassword = user.getPassword();
			if (updatedPassword != null)
				storedUser.setPassword(updatedPassword);

			String updatedName = user.getFullName();
			if (updatedName != null)
				storedUser.setFullName(updatedName);

		}
		return storedUser;
	}

	@Override
	public User deleteUser(String userId, String password) throws UsersException {
		Log.info("deleteUser : user = " + userId + "; pwd = " + password);
		
		User storedUser;

		synchronized (this) {
			
			storedUser = users.get(userId);

			// Check if the User was stored in memory before being able to update, if not
			// return HTTP NOT FOUND (404)
			checkIfUserExists(storedUser);

			// Check if the password is correct, if not return HTTP FORBIDDEN (403)
			checkUserPassword(storedUser, password);

			// Everything ready to delete the User
			users.remove(userId);
		}
		
				
		setSheetsMiddlemanURI();						
		exec.execute(()->{
			sheetsM.deleteUserSpreadsheets(userId);
		});
		
		return storedUser;
	}

	private void checkUserPassword(User user, String password) throws UsersException {
		
		if (!user.getPassword().equals(password)) {
			Log.info("Password is incorrect.");
			throw new UsersException(FORBIDDEN);
		}
	}

	@Override
	public List<User> searchUsers(String pattern) throws UsersException {
		Log.info("searchUsers : pattern = " + pattern);

		List<User> users = new LinkedList<User>();

		synchronized (this) {

			Set<Entry<String, User>> usersEntrySet = this.users.entrySet();

			Iterator<Entry<String, User>> usersIt = usersEntrySet.iterator();

			while (usersIt.hasNext()) {
				User user = usersIt.next().getValue();

				if (user.getFullName().toLowerCase().contains(pattern.toLowerCase())) {
					users.add(user);
				}
			}
		}

		return users;
	}

	@Override
	public void hasUser(String userId) throws UsersException {
		if (userId == null)
			throw new UsersException(BAD_REQUEST);

		synchronized (this) {

			boolean found = users.containsKey(userId);

			if (!found) {
				checkIfUserExists(null);
			}
		}
	}
	
	private void setSheetsMiddlemanURI() {
		String service = UsersServer.usersDomain + ":" + SpreadsheetsServer.SERVICE;
		
		URI[] uris = discovery.knownUrisOf(service);

		int tryNumber = 0;
		
		while (uris == null && tryNumber < MAX_TRIES) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			uris = discovery.knownUrisOf(service);
			tryNumber++;
		}
		
		if(uris != null)
			sheetsM.setSheetsServerURI(uris[0]);
	}

	private void checkIfUserExists(User user) throws UsersException {
		if (user == null) {
			Log.info("User does not exist.");
			throw new UsersException(NOT_FOUND);
		}
	}

}
