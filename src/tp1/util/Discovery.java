package tp1.util;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;

/**
 * <p>A class to perform service discovery, based on periodic service contact endpoint 
 * announcements over multicast communication.</p>
 * 
 * <p>Servers announce their *name* and contact *uri* at regular intervals. The server actively
 * collects received announcements.</p>
 * 
 * <p>Service announcements have the following format:</p>
 * 
 * <p>&lt;service-name-string&gt;&lt;delimiter-char&gt;&lt;service-uri-string&gt;</p>
 */
public class Discovery {
	private static Logger Log = Logger.getLogger(Discovery.class.getName());

	static {
		// addresses some multicast issues on some TCP/IP stacks
		System.setProperty("java.net.preferIPv4Stack", "true");
		// summarizes the logging format
		System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s: %5$s");
	}
	
	
	// The pre-aggreed multicast endpoint assigned to perform discovery. 
	static final InetSocketAddress DISCOVERY_ADDR = new InetSocketAddress("226.226.226.226", 2266);
	static final int DISCOVERY_PERIOD = 1000;
	static final int DISCOVERY_TIMEOUT = 5000;
	public static final int CLIENT = 0;																				//CONSTATNTE UTILIZADA PARA O MODO DISCOVERY
	public static final int DEFAULT = 1;																			//CONSTATNTE UTILIZADA PARA O MODO DISCOVERY

	// Used separate the two fields that make up a service announcement.
	private static final String DELIMITER = "\t";

	private InetSocketAddress addr;
	private String serviceName;
	private String serviceURI;
	private int mode;
	private Map<String, Set<URI>> urisPerService;

	/**
	 * @param  serviceName the name of the service to announce
	 * @param  serviceURI an uri string - representing the contact endpoint of the service being announced
	 */
	public Discovery( InetSocketAddress addr, String serviceName, String serviceURI, int mode) {
		this.addr = addr;
		this.serviceName = serviceName;
		this.serviceURI  = serviceURI;
		this.mode = mode;
		urisPerService = new HashMap<>();
	}
	
	public Discovery(String serviceName, String serviceURI, int mode) {
		this.addr = DISCOVERY_ADDR;
		this.serviceName = serviceName;
		this.serviceURI  = serviceURI;
		this.mode = mode;
		urisPerService = new HashMap<>();
	}

	/**
	 * Starts sending service announcements at regular intervals... 
	 */
	public void start() {
		
		byte[] announceBytes = String.format("%s%s%s", serviceName, DELIMITER, serviceURI).getBytes();
		DatagramPacket announcePkt = new DatagramPacket(announceBytes, announceBytes.length, addr);

		try {
			@SuppressWarnings("resource")
			MulticastSocket ms = new MulticastSocket( addr.getPort());
			ms.joinGroup(addr, NetworkInterface.getByInetAddress(InetAddress.getLocalHost()));
				
			if(mode == DEFAULT){
				Log.info(String.format("Starting Discovery announcements on: %s for: %s -> %s\n", addr, serviceName, serviceURI));

				// start thread to send periodic announcements
				new Thread(() -> {
					for (;;) {
						try {
							ms.send(announcePkt);
							Thread.sleep(DISCOVERY_PERIOD);
						} catch (Exception e) {
							e.printStackTrace();
							// do nothing
						}
					}
				}).start();
			}
			
			// start thread to collect announcements
			new Thread(() -> {
				DatagramPacket pkt = new DatagramPacket(new byte[1024], 1024);
				for (;;) {
					try {
						pkt.setLength(1024);
						ms.receive(pkt);
						String msg = new String( pkt.getData(), 0, pkt.getLength());
						String[] msgElems = msg.split(DELIMITER);
						if( msgElems.length == 2) {	//periodic announcement
							
							Set<URI> serviceURIs = urisPerService.get(msgElems[0]);
							if(serviceURIs == null) {
								Set<URI> uris = new TreeSet<>();
								URI uri = new URI(msgElems[1]);
								uris.add(uri);
								urisPerService.put(msgElems[0], uris);
							
							} else {
								URI uri = new URI(msgElems[1]);
								serviceURIs.add(uri); 
							}
						}
					} catch (IOException e) {
						// do nothing
					} catch (URISyntaxException e) {
					e.printStackTrace();
					}
					
					if(mode == CLIENT)
						break;
				}
			}).start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Returns the known servers for a service.
	 * 
	 * @param  serviceName the name of the service being discovered
	 * @return an array of URI with the service instances discovered. 
	 * 
	 */
	public URI[] knownUrisOf(String serviceName) {
		Set<URI> serviceURIs = urisPerService.get(serviceName);
		
		if(serviceURIs == null)
			return null;
		
		int size = serviceURIs.size();
		URI[] uris = new URI[size];
		int i = 0;
		
		for(URI currURI : serviceURIs) {
			uris[i++] = currURI;
		}
		return uris;
	}
	
	
	// Main just for testing purposes
	public static void main( String[] args) throws Exception {
		Discovery discovery = new Discovery( DISCOVERY_ADDR, "test", "https://" + InetAddress.getLocalHost().getHostAddress(), DEFAULT);
		discovery.start();
	}
}
