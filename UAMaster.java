

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;



public class UAMaster {
	
	private static final int TIMEOUT = 2000;
	private static final int PORT = 32000;
	private static final int MAX_CONNECTIONS = 20;
	private static final String CONFIG_PATH = "config.txt";
	private static final String FILE_MAP_PATH = "filemap.txt";
	private static final String USER_MAP_PATH = "usermap.txt";
	
	private int dataNodes;
	private BufferedReader configIn;
	private BufferedReader filemapIn;
	private BufferedReader usermapIn;
	private BufferedWriter filemapOut;
	private BufferedWriter usermapOut;
	private ConcurrentHashMap<String, String> dataNodeIPs = new ConcurrentHashMap<>();
	private ConcurrentHashMap<String, Integer> dataNodePorts = new ConcurrentHashMap<>();
	private ConcurrentHashMap<Integer, String> serverNumbers = new ConcurrentHashMap<>();
	private ConcurrentHashMap<String, String> fileToDataNode = new ConcurrentHashMap<>();
	private ConcurrentHashMap<String, String> fileToUser = new ConcurrentHashMap<>();
	private ConcurrentHashMap<String, List<String>> userToFiles = new ConcurrentHashMap<>();
	private ServerSocket server;
	
	public void init() {
		try {
			server = new ServerSocket(PORT);
			server.setSoTimeout(TIMEOUT);
			configIn = new BufferedReader(new FileReader(CONFIG_PATH));
			filemapIn = new BufferedReader(new FileReader(FILE_MAP_PATH));
			usermapIn = new BufferedReader(new FileReader(USER_MAP_PATH));
			String line;
			String[] tokens;
			
			while ((line = configIn.readLine()) != null) {
				tokens = line.split(" ");
				dataNodeIPs.put(tokens[0], tokens[1]);
				dataNodePorts.put(tokens[0], Integer.parseInt(tokens[2]));
				serverNumbers.put(dataNodes, tokens[0]);
				dataNodes++;
			}
			
			while ((line = filemapIn.readLine()) != null) {
				tokens = line.split(" ");
				fileToDataNode.put(tokens[0], tokens[1]);
			}
			
			while ((line = usermapIn.readLine()) != null) {
				tokens = line.split(" ");
				if (!userToFiles.containsKey(tokens[0])) {
                    userToFiles.put(tokens[0], new ArrayList<>());
				}
				
				userToFiles.get(tokens[0]).add(tokens[1]);
                fileToUser.put(tokens[1], tokens[0]);
// 				for (int i = 1; i < tokens.length; i++) {
// 					userToFiles.get(tokens[0]).add(tokens[1]);
// 					fileToUser.put(tokens[1], tokens[0]);
// 				}
			}
		} catch(IOException e) {
			e.printStackTrace();
		} finally {
			if (configIn != null) {
				try {
					configIn.close();
				} catch(IOException e) {
					e.printStackTrace();
				}
			}
			
			if (filemapIn != null) {
				try {
					filemapIn.close();
				} catch(IOException e) {
					e.printStackTrace();
				}
			}
			
			if (usermapIn != null) {
				try {
					usermapIn.close();
				} catch(IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public void start() {
		while (!UAClientCounter.isShutdown()) {
			if (UAClientCounter.getCount() < MAX_CONNECTIONS) {
				try {
					Socket socket = server.accept();
					System.out.println(socket.getInetAddress());
					UAClientCounter.add();
					new UAClientConnection(socket, dataNodeIPs, dataNodePorts, serverNumbers, fileToDataNode, fileToUser, userToFiles).run();
				} catch(Exception e) {
					continue;
				}
			}
		}
	}
	
	public void shutdown() {
		try {
			filemapOut = new BufferedWriter(new FileWriter(FILE_MAP_PATH));
			usermapOut = new BufferedWriter(new FileWriter(USER_MAP_PATH));
			
			for (Entry<String, String> entry : fileToDataNode.entrySet()) {
				filemapOut.write(entry.getKey() + "\t" + entry.getValue());
			}
			
			for (Entry<String, String> entry : fileToUser.entrySet()) {
				filemapOut.write(entry.getKey() + "\t" + entry.getValue());
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (filemapOut != null) {
				try {
					filemapOut.close();
				} catch(IOException e) {
					e.printStackTrace();
				}
			}
			
			if (usermapOut != null) {
				try {
					usermapOut.close();
				} catch(IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void main(String[] args) {
		UAMaster master = new UAMaster();
		master.init();
		master.start();
	}

}
