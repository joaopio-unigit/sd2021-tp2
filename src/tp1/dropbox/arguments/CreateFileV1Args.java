package tp1.dropbox.arguments;

public class CreateFileV1Args {

	final String path;
	final String mode;
	final boolean autorename;
	final boolean mute;
	final boolean strict_conflict;
	
	public CreateFileV1Args(String path, String mode, boolean autorename, boolean mute, boolean strict_conflict) {
		this.autorename = autorename;
		this.mode = mode;
		this.mute = mute;
		this.path = path;
		this.strict_conflict = strict_conflict;
	}


}
