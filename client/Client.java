package client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

import net.ddp2p.common.util.GetOpt;

public class Client {
	public static void main(String[] args) {
		//parse command line arguments
		int port = 40408;
		boolean istcp = true;
		String message = "Hello World";
		
		char c;
		while( (c = GetOpt.getopt(args, "up:m:")) != GetOpt.END) {
			switch(c) {
				case 'u':
					istcp = false;
					break;
				case 'p':
					try {
						port = Integer.parseInt(GetOpt.optarg);
					} catch(NumberFormatException e) {
						System.err.println("Invalid value passed as port.");
					}
					break;
				case 'm':
					message = GetOpt.optarg;
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

		try {
			if(istcp) {
				sendTCP(message, port);
			} else {
				sendUDP(message, port);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void sendUDP(String message, int port) throws IOException {
		DatagramSocket socket = new DatagramSocket();
		SocketAddress address = new InetSocketAddress("localhost",port);
		
		byte[] buf = (message + "\n").getBytes();
		DatagramPacket packet = new DatagramPacket(buf, buf.length, address);
		System.out.println("UDP >>> " + message);
		socket.send(packet);
		
		packet = new DatagramPacket(buf, buf.length,address);
		socket.receive(packet);
		String received = new String(packet.getData(), 0, packet.getLength());
		System.out.println("UDP <<< " + received);
		
		socket.close();
	}
	
	private static void sendTCP(String message, int port) throws IOException {
		Socket socket = new Socket("localhost",port);
		
		PrintStream out = new PrintStream( socket.getOutputStream() );
		BufferedReader in = new BufferedReader( new InputStreamReader( socket.getInputStream() ) );
		
		out.println(message + "\n");
		System.out.println("TCP >>> " + message);
		
		String line = in.readLine();
		System.out.println("TCP <<< " + line);
		
		socket.close();
	}
}
