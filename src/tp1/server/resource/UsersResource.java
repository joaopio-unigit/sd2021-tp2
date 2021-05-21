package tp1.server.resource;

import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Logger;

import jakarta.inject.Singleton;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response.Status;
import tp1.api.User;
import tp1.api.service.rest.RestUsers;
import tp1.clients.rest.SheetsMiddleman;
import tp1.server.rest.SpreadsheetsServer;
import tp1.server.rest.UsersServer;
import tp1.util.Discovery;

@Singleton
public class UsersResource implements RestUsers {

	private static final int MAX_TRIES = 11;
	private final Map<String, User> users;
	private static Logger Log = Logger.getLogger(UsersResource.class.getName());
	
	private Discovery discovery;
	private SheetsMiddleman sheetsM;
	
	public UsersResource() {
		users  = new HashMap<String, User>();
		discovery = UsersServer.usersDiscovery;
		sheetsM = new SheetsMiddleman();
	}

	@Override
	public String createUser(User user) {
		Log.info("createUser : " + user);

		// Check if user is valid, if not return HTTP BAD_REQUEST (400)
		if (user.getUserId() == null || user.getPassword() == null || user.getFullName() == null
				|| user.getEmail() == null) {
			Log.info("User object invalid.");
			throw new WebApplicationException(Status.BAD_REQUEST);
		}

		synchronized (this) {

			// Check if userId does not exist exists, if not return HTTP CONFLICT (409)
			if (users.containsKey(user.getUserId())) {
				Log.info("User already exists.");
				throw new WebApplicationException(Status.CONFLICT);
			}

			// Add the user to the map of users
			users.put(user.getUserId(), user);
		}

		return user.getUserId();
	}

	@Override
	public User getUser(String userId, String password) {
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
	public User updateUser(String userId, String password, User user) {
		Log.info("updateUser : user = " + userId + "; pwd = " + password + " ; user = " + user);

		// Check if userID and password are valid, if not return HTTP BAD_REQUEST (400)
		if (userId == null || password == null) {
			Log.info("UserId or passwrod null.");
			throw new WebApplicationException(Status.BAD_REQUEST);
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
	public User deleteUser(String userId, String password) {
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
		sheetsM.deleteUserSpreadsheets(userId, UsersServer.serverSecret);
		
		return storedUser;
	}

	@Override
	public List<User> searchUsers(String pattern) {
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
	public void hasUser(String userId, String secret) {
		if (userId == null || !secret.equals(UsersServer.serverSecret))
			throw new WebApplicationException(Status.BAD_REQUEST);

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

	private void checkIfUserExists(User user) {
		if (user == null) {
			Log.info("User does not exist.");
			throw new WebApplicationException(Status.NOT_FOUND);
		}
	}

	private void checkUserPassword(User user, String password) {
		if (!user.getPassword().equals(password)) {
			Log.info("Password is incorrect.");
			throw new WebApplicationException(Status.FORBIDDEN);
		}
	}
}
