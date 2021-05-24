package database_server;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import net.ddp2p.common.util.GetOpt;

public class Server {
	
	private static final boolean VERBOSE = true;

	private static int PORT = 40408;
	private static Database database = null;
	private static File databaseFile = null;

	public static void main(String[] args) {
		//parse command line arguments using GetOpts
		String fileName = null;
		char c;
		while( (c = GetOpt.getopt(args, "p:f:")) != GetOpt.END) {
			switch(c) {
			case 'p':
				try {
					PORT = Integer.parseInt(GetOpt.optarg);
				} catch(NumberFormatException e) {
					System.err.println("Invalid value passed as port.");
					System.exit(-1);
				}
				break;
			case 'f':
				fileName = GetOpt.optarg;
				break;
			case GetOpt.END:
				break;
			case '?': 
				System.out.println("?:"+GetOpt.optopt); 
				return;
			default:
				System.out.println("Error: "+c);
				return;
			}
		}
		//check for database file
		if(fileName == null) {
			System.err.println("No database file specified.");
		} else {
			//init database file
			databaseFile = new File(fileName);
			database = Database.loadDatabase(databaseFile);
			Thread databaseThread = new Thread(database);
			databaseThread.start();
			//start server
			try {
				startServer();
			} catch (IOException e) {
				if(VERBOSE) System.out.println("Failed to start servers.");
			}
		}
		database.stop();
		if(VERBOSE) System.out.println("Done.");
	}
	
	/**
	 * Starts and runs an NIO server, accepting both UDP and TCP connections over the same port.
	 * @throws IOException 
	 */
	private static void startServer() throws IOException {
		//init selector
		Selector selector = Selector.open();
		ServerSocketChannel serverSocket = ServerSocketChannel.open();
		DatagramChannel udpserver = DatagramChannel.open();
		
		//init tcp channel
		serverSocket.bind(new InetSocketAddress(PORT));
		serverSocket.configureBlocking(false);
		serverSocket.register(selector, SelectionKey.OP_ACCEPT);
		
		//init UDP channel
        udpserver.socket().bind(new InetSocketAddress(PORT));
        udpserver.configureBlocking(false);
        udpserver.register(selector, SelectionKey.OP_READ);
        
		ByteBuffer buff = ByteBuffer.allocate(256);
		
		//repeat forever
		while(true) {
			selector.select(15000);
			Set<SelectionKey> keys = selector.selectedKeys();

			// Iterate through the Set of keys.
			for (Iterator<SelectionKey> i = keys.iterator(); i.hasNext();) {
				try {
					selector.select();
					Set<SelectionKey> selectedKeys = selector.selectedKeys();
					Iterator<SelectionKey> iter = selectedKeys.iterator();
					while (iter.hasNext()) {

						SelectionKey key = iter.next();
						//accept TCP requests
						if (key.isAcceptable()) {
							register(selector, serverSocket);
						//process received data
						} else if (key.isReadable()) {
							//use proper socket type
							if(key.channel() instanceof SocketChannel)
								answerWithEchoTCP(buff, key);
							else if(key.channel() instanceof DatagramChannel)
								answerWithEchoUDP(buff, key);
							else
								if(VERBOSE) System.err.println("recieved unknown socket type");
						}
						iter.remove();
					}
				} catch(IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * Accepts a TCP requests and inits channel
	 */
    private static void register(Selector selector, ServerSocketChannel serverSocket) throws IOException {
        SocketChannel client = serverSocket.accept();
        client.configureBlocking(false);
        client.register(selector, SelectionKey.OP_READ);
    }
	
    /**
     * Receives and processes TCP data
     */
	private static void answerWithEchoTCP(ByteBuffer buffer, SelectionKey key) throws IOException {
		//read data
		SocketChannel client = (SocketChannel) key.channel();
		try {
			buffer.clear();
			client.read(buffer);
			
			//process data
			String line = new String(buffer.array()).trim();
			int pi = line.indexOf('%');		if(pi == -1) pi = 2000;
			int ni = line.indexOf('\n');	if(ni == -1) ni = 2000;
			if(pi < ni)
				line = line.substring(0,pi);
			else if(ni <= pi && ni < 2000)
				line = line.substring(0,ni);
			
			if(VERBOSE) System.out.println("TCP <<< " + line );
			String out = processCommand(line);
			
			//send response
			client.write(ByteBuffer.wrap(out.getBytes()));
			if(VERBOSE) System.out.println("TCP >>> " + out );
			
			//cleanup
			buffer.clear();
		} catch(IOException e) {
			if(VERBOSE) System.out.println("Closing TCP connection" );
			client.close();
		}
	}
	
	/**
	 * Receives and processes UDP data
	 */
	private static void answerWithEchoUDP(ByteBuffer buffer, SelectionKey key) throws IOException {
		//read data
		DatagramChannel client = (DatagramChannel) key.channel();
		buffer.clear();
		SocketAddress address = client.receive(buffer);
		
		//process data
		String line = new String(buffer.array()).trim();
		int pi = line.indexOf('%');		if(pi == -1) pi = 2000;
		int ni = line.indexOf('\n');	if(ni == -1) ni = 2000;
		if(pi < ni)
			line = line.substring(0,pi);
		else if(ni <= pi && ni < 2000)
			line = line.substring(0,ni);
		
		String out = processCommand(line);
		if(VERBOSE) System.out.println("UDP <<< " + line );
		
		//send response
		client.connect(address);
		client.write(ByteBuffer.wrap(out.getBytes()));
		if(VERBOSE) System.out.println("UDP >>> " + out );
		
		//cleanup
		buffer.clear();
		client.disconnect();
	}

	public static String processCommand(String event) {
		String[] split = event.split(";");
		
		if(split[0].equals("EVENT_DEFINITION")) {
			database.addEntry(split);
			return "ok;" + event + "\n";
		}
		if(split[0].equals("GET_NEXT_EVENTS")) {
			ArrayList<String> entries = database.getEntries(split);
			String out = "EVENTS;" + entries.size();
			for(String entry:entries)
				out += ";" + entry;
			return out + "\n";
		}
		return "ok;" + event + "\n";
	}
	
	public static File getDatabaseFile() {
		return databaseFile;
	}

}
