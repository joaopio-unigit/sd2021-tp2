package tp1.api.service.rest;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import tp1.api.Spreadsheet;

@Path(ReplicationRestSpreadsheets.PATH)
public interface ReplicationRestSpreadsheets {
	
	public static final String PATH="/spreadsheetsreplication";
	public static final String DELETESHEETS = "/deleteSheets";
	public static final String OPERATION = "/operation";
	
	/**
	 * Creates a new spreadsheet. The sheetId and sheetURL are generated by the server.
	 * After being created, the size of the spreadsheet is not modified.
	 * @param sheet - the spreadsheet to be created.
	 * @param password - the password of the owner of the spreadsheet.
	 * 
	 * @return 200 the sheetId; 
	 * 		   400 otherwise.
	 */
	@POST
	@Path(OPERATION)
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	String createSpreadsheetOperation(Spreadsheet sheet, @QueryParam("password") String password );

	
	/**
	 * Deletes a spreadsheet.  Only the owner can call this method.
	 * 
	 * @param sheetId - the sheet to be deleted.
	 * @param password - the password of the owner of the spreadsheet.
	 * 
	 * @return 204 if the sheet was successful.
	 *			404 if no sheet exists with the given sheetId.
	 *          403 if the password is incorrect.
	 *			400 otherwise.
	 */
	@DELETE
	@Path("/{sheetId}" + OPERATION)
	void deleteSpreadsheetOperation(@PathParam("sheetId") String sheetId, @QueryParam("password") String password);

	/**
	 * Retrieve a spreadsheet.
	 * 	
	 * @param sheetId - The  spreadsheet being retrieved.
	 * @param userId - The user performing the operation.
	 * @param password - The password of the user performing the operation.
	 * 
	 * @return 200 and the spreadsheet
	 *		   404 if no sheet exists with the given sheetId, or the userId does not exist.
	 *         403 if the password is incorrect.
	 * 		   400 otherwise
	 */
	@GET
	@Path("/{sheetId}" + OPERATION)
	@Produces(MediaType.APPLICATION_JSON)
	Spreadsheet getSpreadsheetOperation(@PathParam("sheetId") String sheetId , @QueryParam("userId") String userId, 
			@QueryParam("password") String password);
		
	
	/**
	 * Retrieves the calculated values of a spreadsheet.
	 * @param userId - The user requesting the values
	 * @param sheetId - the spreadsheet whose values are being retrieved.
	 * @param password - The password of the user performing the operation
	 * 
	 * @return 200, if the operation is successful
	 * 		   403, if the spreadsheet is not shared with user, or the user is not the owner, or the password is incorrect.
	 * 		   404, if the spreadsheet or the user do not exist
	 *		   400, otherwise
	 */
	@GET
	@Path("/{sheetId}/values" + OPERATION)
	@Produces(MediaType.APPLICATION_JSON)
	String[][] getSpreadsheetValuesOperation(@PathParam("sheetId") String sheetId, 
			@QueryParam("userId") String userId, @QueryParam("password") String password);


	/**
	 * Updates the raw values of some cells of a spreadsheet. 
	 * 
	 * @param userId - The user performing the update.
	 * @param sheetId - the spreadsheet whose values are being retrieved.
	 * @param cell - the cell being updated
	 * @param rawValue - the new raw value of the cell
	 * @param password - the password of the owner of the spreadsheet
	 * 
	 * @return 204, if the operation was successful
	 * 		  404, if no spreadsheet exists with the given sheetid
	 *        403, if the password is incorrect.
	 *        400 otherwise
	 **/
	@PUT
	@Path("/{sheetId}/{cell}" + OPERATION)
	@Consumes(MediaType.APPLICATION_JSON)
	void updateCellOperation( @PathParam("sheetId") String sheetId, @PathParam("cell") String cell, String rawValue, 
			@QueryParam("userId") String userId, @QueryParam("password") String password);

	
	/**
	 * Adds a new user to the list of shares of a spreadsheet. Only the owner can call this method.
	 * 
	 * @param sheetId - the sheet being shared.
	 * @param userId - the user that is being added to the list of shares. In this method, the userId is represented
	 *                 in the form userId@domain
	 * @param password - The password of the owner of the spreadsheet.
	 * 
	 * @return 204, in case of success.
	 * 		   404, if either the spreadsheet or user do not exist
	 * 		   409, if the sheet is already shared with the user
	 *         403 if the password is incorrect.
	 * 		   400, otherwise
	 * 
	 */
	@POST
	@Path("/{sheetId}/share/{userId}" + OPERATION)
	void shareSpreadsheetOperation( @PathParam("sheetId") String sheetId, @PathParam("userId") String userId, 
			@QueryParam("password") String password);

	
	/**
	 * Removes a user from the list of shares of a spreadsheet. Only the owner can call this method.
	 * 
	 * @param sheetId - the sheet being shared.
	 * @param userId - the user that is being added to the list of shares. In this method, the userId is represented
	 *                 in the form userId@domain
	 * @param password - The password of the owner of the spreadsheet.
	 * 
	 * @return 204, in case of success.
	 * 		   404, if the spreadsheet, the user or the share do not exist
	 *         403 if the password is incorrect.
	 * 		   400, otherwise
	 */
	@DELETE
	@Path("/{sheetId}/share/{userId}" + OPERATION)
	void unshareSpreadsheetOperation( @PathParam("sheetId") String sheetId, @PathParam("userId") String userId, 
			@QueryParam("password") String password);
	
	/**
	 * Removes all the Spreadsheets from the service that belong to the userId
	
	 * @param userId - the userId who's sheets shall be forgotten
	 * 
	 * @return 204, in case of success.
	 * 		   400, if userId is null
	 */
	@DELETE
	@Path(DELETESHEETS + "/{userId}" + OPERATION)
	void deleteUserSpreadsheetsOperation(@PathParam("userId") String userId, @QueryParam("secret") String secret);
	
	/**
	 * Return the calculated imported values in some range
	 * 
	 * @param sheetId - the sheet with the values we want
	 * @param userId - the user making the request
	 * @param range - the range to import
	 * 
	 * @return 200, in case of success.
	 * 		   400, if any of the arguments is null
	 */
	@GET
	@Path("/{sheetId}/{userId}/{range}" + OPERATION)
	String[][] importRangeOperation(@PathParam("sheetId") String sheetId, @PathParam("userId") String userId,
			@PathParam("range") String range, @QueryParam("secret") String secret);
}