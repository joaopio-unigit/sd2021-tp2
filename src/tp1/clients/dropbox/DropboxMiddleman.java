package tp1.clients.dropbox;

import java.io.IOException;

import tp1.api.Spreadsheet;
import tp1.dropbox.arguments.CreateFolderV2Args;

import org.pac4j.scribe.builder.api.DropboxApi20;

import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;
import com.google.gson.Gson;

import tp1.dropbox.arguments.DownloadFileV1Args;
import tp1.dropbox.replies.DownloadReturn;
import tp1.dropbox.arguments.DeleteV2Args;
import tp1.dropbox.arguments.CreateFileV1Args;

public class DropboxMiddleman {

	private static final String apiKey = "uiz15boz3dj0e94";
	private static final String apiSecret = "m7c4iokhadmg8nm";
	private static final String accessTokenStr = "Wctw6I6wZIYAAAAAAAAAAW7GD6hb2rAuhv17gCfbbWJEOHl1RmjZo2nxTcEcDRA2";

	protected static final String JSON_CONTENT_TYPE = "application/json; charset=utf-8";
	protected static final String OCTET_CONTENT_TYPE = "application/octet-stream";

	private static final String CREATE_FOLDER_V2_URL = "https://api.dropboxapi.com/2/files/create_folder_v2";
	private static final String CREATE_FILE_V1_URL = "https://content.dropboxapi.com/2/files/upload";
	private static final String DELETE_V2_URL = "https://api.dropboxapi.com/2/files/delete_v2";
	private static final String DOWNLOAD_V1_URL = "https://content.dropboxapi.com/2/files/download";
	
	private OAuth20Service service;
	private OAuth2AccessToken accessToken;
	private Gson json;

	private String serverDomain;

	public DropboxMiddleman(String serverDomain) {
		this.serverDomain = serverDomain;

		service = new ServiceBuilder(apiKey).apiSecret(apiSecret).build(DropboxApi20.INSTANCE);
		accessToken = new OAuth2AccessToken(accessTokenStr);
		json = new Gson();

	}

	public boolean createDomain(String userName) {
		// ESTAMOS A INDICAR O TIPO DE OPERACAO E O URL DA OPERACAO
		OAuthRequest createFolder = new OAuthRequest(Verb.POST, CREATE_FOLDER_V2_URL);
		// ADICIONAR TODOS OS HEADERS DA NOSSA RESPONSABILIDADE (NOME HEADER, VALOR DO HEADER)
		createFolder.addHeader("Content-Type", JSON_CONTENT_TYPE);
		
		String domainPath;
		if(userName != null)
			domainPath = "/" + serverDomain + "/" + userName;
		else
			domainPath = "/" + serverDomain;
		
		// UTILIZADO NO CASO DA OPCAO --data
		createFolder.setPayload(json.toJson(new CreateFolderV2Args(domainPath, false)));
		service.signRequest(accessToken, createFolder);

		boolean success = execute(service, createFolder, null);

		if (success) {
			System.out.println(serverDomain + " directory created successfuly.");
			return true;
		}
		else {
			System.out.println("Failed to create directory for " + serverDomain);
			return false;
		}
	}

	public boolean uploadSpreadsheet(Spreadsheet sheet) {
		// ESTAMOS A INDICAR O TIPO DE OPERACAO E O URL DA OPERACAO
		OAuthRequest createFile = new OAuthRequest(Verb.POST, CREATE_FILE_V1_URL);
		// ADICIONAR TODOS OS HEADERS DA NOSSA RESPONSABILIDADE (NOME HEADER, VALOR DO HEADER)
		String filePath = "/" + serverDomain + "/" + sheet.getOwner() + "/" + sheet.getSheetId();
		createFile.addHeader("Dropbox-API-Arg", json.toJson(new CreateFileV1Args(filePath, "overwrite", false, true, false)));
		createFile.addHeader("Content-Type", OCTET_CONTENT_TYPE);
		createFile.setPayload(json.toJson(sheet));

		service.signRequest(accessToken, createFile);

		boolean success = execute(service, createFile, null);

		if (success) {
			System.out.println(sheet.getSheetId() + " created successfuly.");
			return true;
		}
		else {
			System.out.println("Failed to create " + sheet.getSheetId());
			return false;
		}
	}

	public boolean deleteSpreadsheet(String sheetOwner, String sheetId) {
		// ESTAMOS A INDICAR O TIPO DE OPERACAO E O URL DA OPERACAO
		OAuthRequest deleteFile = new OAuthRequest(Verb.POST, DELETE_V2_URL);
		// ADICIONAR TODOS OS HEADERS DA NOSSA RESPONSABILIDADE (NOME HEADER, VALOR DO HEADER)
		deleteFile.addHeader("Content-Type", JSON_CONTENT_TYPE);
		String filePath;
		if(sheetId != null)
			filePath = "/" + serverDomain + "/" + sheetOwner + "/" + sheetId;
		else
			filePath = "/" + serverDomain + "/" + sheetOwner;
		// UTILIZADO NO CASO DA OPCAO --data
		deleteFile.setPayload(json.toJson(new DeleteV2Args(filePath)));
		service.signRequest(accessToken, deleteFile);

		boolean success = execute(service, deleteFile, null);

		if (success) {
			System.out.println(sheetId + " deleted successfuly.");
			return true;
		}
		else {
			System.out.println("Failed to delete " + sheetId);
			return false;
		}
	}
	
	public Spreadsheet getSpreadsheet(String sheetOwner, String sheetId) {
		// ESTAMOS A INDICAR O TIPO DE OPERACAO E O URL DA OPERACAO
		OAuthRequest downloadFile = new OAuthRequest(Verb.POST, DOWNLOAD_V1_URL);
		// ADICIONAR TODOS OS HEADERS DA NOSSA RESPONSABILIDADE (NOME HEADER, VALOR DO HEADER)
		downloadFile.addHeader("Content-Type", OCTET_CONTENT_TYPE);
		String filePath = "/" + serverDomain + "/" + sheetOwner + "/" + sheetId;
		downloadFile.addHeader("Dropbox-API-Arg", json.toJson(new DownloadFileV1Args(filePath)));
		service.signRequest(accessToken, downloadFile);

		DownloadReturn downloadReturn = new DownloadReturn();

		boolean success = execute(service, downloadFile, downloadReturn);

		Spreadsheet sheet = downloadReturn.getSpreadsheet();
				
		if (success)
			System.out.println(sheetId + " downloaded successfuly.");
		else
			System.out.println("Failed to download " + sheetId);

		return sheet;
	}
	
	private boolean execute(OAuth20Service service, OAuthRequest dropboxRequest, DownloadReturn downloadReturn) {
		Response r = null;

		try {
			r = service.execute(dropboxRequest);
			
			if (r.getCode() == 200) {
				if (dropboxRequest.getUrl().equals(DOWNLOAD_V1_URL)) {
					downloadReturn.setSpreadsheet(json.fromJson(r.getBody(), Spreadsheet.class));
				}
				return true;
				
			} else {
				System.err.println("HTTP Error Code: " + r.getCode() + ": " + r.getMessage());
				try {
					System.err.println(r.getBody());
				} catch (IOException e) {
					System.err.println("No body in the response");
				}
				return false;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	public boolean resetState() {
		// ESTAMOS A INDICAR O TIPO DE OPERACAO E O URL DA OPERACAO
		OAuthRequest deleteFile = new OAuthRequest(Verb.POST, DELETE_V2_URL);
		// ADICIONAR TODOS OS HEADERS DA NOSSA RESPONSABILIDADE (NOME HEADER, VALOR DO HEADER)
		deleteFile.addHeader("Content-Type", JSON_CONTENT_TYPE);
		String filePath = "/" + serverDomain;
		// UTILIZADO NO CASO DA OPCAO --data
		deleteFile.setPayload(json.toJson(new DeleteV2Args(filePath)));
		service.signRequest(accessToken, deleteFile);
		boolean success = execute(service, deleteFile, null);
		if (success) {
			System.out.println(serverDomain + " deleted successfuly.");
			return true;
		}
		else {
			System.out.println("Failed to delete " + serverDomain);
			return false;
		}
	}
}
