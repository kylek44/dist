

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class UAClientConnection implements Runnable {
	private static final String GET_ALL_FILES = "GET_ALL_USER_FILES";
	private static final String RETRIEVE_FILE = "RETRIEVE_FILE";
	private static final String UPLOAD_FILE = "UPLOAD_FILE";
	private static final String DELETE_FILE = "DELETE_FILE";
	private final Socket socket;
	private ConcurrentHashMap<String, String> dataNodeIPs;
	private ConcurrentHashMap<String, Integer> dataNodePorts;
	private ConcurrentHashMap<Integer, String> serverNumbers;
	private ConcurrentHashMap<String, String> fileToDataNode;
	private ConcurrentHashMap<String, String> fileToUser;
	private ConcurrentHashMap<String, List<String>> userToFiles;
	
	public UAClientConnection(Socket socket, ConcurrentHashMap<String, String> dataNodeIPs, ConcurrentHashMap<String, Integer> dataNodePorts, ConcurrentHashMap<Integer, String> serverNumbers, ConcurrentHashMap<String, String> fileToDataNode, ConcurrentHashMap<String, String> fileToUser, ConcurrentHashMap<String, List<String>> userToFiles) {
		this.socket = socket;
		this.dataNodeIPs = dataNodeIPs;
		this.dataNodePorts = dataNodePorts;
		this.serverNumbers = serverNumbers;
		this.fileToDataNode = fileToDataNode;
		this.fileToUser = fileToUser;
		this.userToFiles = userToFiles;
	}
	
	public void getAllFiles(byte[] dataIn) {
		String username = new String(dataIn).trim();
		
		if (userToFiles.containsKey(username)) {
			List<String> files = userToFiles.get(username);
			System.out.println(files);
			byte[] data = new byte[2048];
			int pos = 0;
			for (String file : files) {
				if (pos > 0) {
					data[pos++] = 9;
				}
				byte[] fileNameBytes = file.getBytes();
				for (int i = 0; i < fileNameBytes.length; i++) {
					data[pos++] = fileNameBytes[i];
				}
			}
			try {
				OutputStream out = socket.getOutputStream();
				out.write(data);
				out.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void retrieveFile(byte[] dataIn) {
		String[] tokens = new String(dataIn).trim().split("\t");
		
		if (userToFiles.containsKey(tokens[0]) && fileToUser.containsKey(tokens[1]) && fileToDataNode.containsKey(tokens[1])) {
			try {
				Socket dataNode = new Socket(dataNodeIPs.get(fileToDataNode.get(tokens[1])), 32000);
				InputStream dataNodeIn = dataNode.getInputStream();
				OutputStream dataNodeOut = dataNode.getOutputStream();
				
				byte[] dataSent = new byte[500];
				byte[] request = RETRIEVE_FILE.getBytes();
				byte[] username = tokens[0].getBytes();
				byte[] filename = tokens[1].getBytes();
				
				for (int i = 0; i < request.length; i++) {
					dataSent[i] = request[i];
				}
				
				for (int i = 0; i < username.length; i++) {
					dataSent[i + 50] = username[i];
				}
				
				dataSent[50 + username.length] = 9;
				
				for (int i = 0; i < filename.length; i++) {
					dataSent[i + 50 + username.length + 1] = filename[i];
				}
				
				dataNodeOut.write(dataSent, 0, dataSent.length);
				dataNodeOut.flush();
				
				byte[] dataReceived = new byte[1024 * 10];
				
				int bytesReceived = dataNodeIn.read(dataReceived, 0, dataReceived.length);
				
				OutputStream out = socket.getOutputStream();
				out.write(dataReceived, 0, bytesReceived);
				out.flush();
				
				dataNode.close();
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public void deleteFile(byte[] dataIn) {
		String[] tokens = new String(dataIn).trim().split("\t");
		System.out.println("User: " + tokens[0]);
		System.out.println("File: " + tokens[1]);
		
		if (userToFiles.containsKey(tokens[0]) && fileToUser.containsKey(tokens[1]) && fileToDataNode.containsKey(tokens[1])) {
			try {
				String ip = dataNodeIPs.get(fileToDataNode.get(tokens[1]));
				int port = dataNodePorts.get(fileToDataNode.get(tokens[1]));
				System.out.println("Connecting to IP: " + ip + " on PORT: " + port);
				Socket dataNode = new Socket(ip, port);
				InputStream dataNodeIn = dataNode.getInputStream();
				OutputStream dataNodeOut = dataNode.getOutputStream();

				byte[] header = new byte[50];
				byte[] dataSent = new byte[500];
				byte[] request = DELETE_FILE.getBytes();
				byte[] username = tokens[0].getBytes();
				byte[] filename = tokens[1].getBytes();
				
				for (int i = 0; i < request.length; i++) {
					header[i] = request[i];
				}
				
				for (int i = 0; i < username.length; i++) {
					dataSent[i] = username[i];
				}
				
				dataSent[username.length] = 9;
				
				for (int i = 0; i < filename.length; i++) {
					dataSent[i + username.length + 1] = filename[i];
				}

				dataNodeOut.write(header, 0, header.length);
				dataNodeOut.flush();
				System.out.println("Sent Header");
				dataNodeOut.write(dataSent, 0, dataSent.length);
				dataNodeOut.flush();
				System.out.println("Sent Data");
				
				byte[] dataReceived = new byte[500];
				
				int bytesReceived = dataNodeIn.read(dataReceived, 0, dataReceived.length);
				String result = new String(dataReceived).trim();

				if (result.equals("SUCCESS")) {
					fileToUser.remove(tokens[1]);
					fileToDataNode.remove(tokens[1]);
					userToFiles.get(tokens[0]).remove(tokens[1]);
				}
				
				OutputStream out = socket.getOutputStream();
				out.write(dataReceived, 0, dataReceived.length);
				out.flush();
				
				dataNode.close();
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public void uploadFile(byte[] dataIn, InputStream in) {
		String[] tokens = new String(dataIn).trim().split("\t");
		System.out.println(tokens[0] + " " + tokens[1]);
		
		if (userToFiles.containsKey(tokens[0])) {
			System.out.println("User exists");
			int hash = Math.abs(tokens[1].hashCode() % dataNodeIPs.size());
			try {
				Socket dataNode = new Socket(dataNodeIPs.get(serverNumbers.get(hash)), dataNodePorts.get(serverNumbers.get(hash)));
				System.out.println("connected");
				InputStream dataNodeIn = dataNode.getInputStream();
				OutputStream dataNodeOut = dataNode.getOutputStream();
				
				byte[] header = new byte[50];
				byte[] secondHeader = new byte[500];
				byte[] request = UPLOAD_FILE.getBytes();
				byte[] username = tokens[0].getBytes();
				byte[] filename = tokens[1].getBytes();

				System.out.println("Writing header");
				System.out.println(new String(request));

				for (int i = 0; i < request.length; i++) {
					header[i] = request[i];
				}

				System.out.println("Done writing header");
				
				for (int i = 0; i < username.length; i++) {
					secondHeader[i] = username[i];
				}
				
				secondHeader[username.length] = 9;
				
				for (int i = 0; i < filename.length; i++) {
					secondHeader[i + username.length + 1] = filename[i];
				}
				
				dataNodeOut.write(header, 0, header.length);
				dataNodeOut.flush();
				System.out.println("Sent header");
				dataNodeOut.write(secondHeader, 0, secondHeader.length);
				dataNodeOut.flush();
				System.out.println("Sent second header");
				
				int bytesRead = -1;
				byte[] data = new byte[1024 * 10];
				
				while (in.read(data) != -1) {
					dataNodeOut.write(data);
					dataNodeOut.flush();
				}
				System.out.println("Wrote file");
				dataNodeOut.flush();

				userToFiles.get(tokens[0]).add(tokens[1]);
				fileToUser.put(tokens[1], tokens[0]);
				fileToDataNode.put(tokens[1], serverNumbers.get(hash));

				byte[] dataNodeDataIn = new byte[500];
				dataNodeIn.read(dataNodeDataIn, 0, dataNodeDataIn.length);
				
				OutputStream out = socket.getOutputStream();
				out.write(dataNodeDataIn, 0, dataNodeDataIn.length);
				out.flush();

				System.out.println("Wrote back to master");
				
				dataNode.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			System.out.println("User doesn't exist");
		}
	}

	@Override
	public void run() {
		try {
            System.out.println("start");
			InputStream in = socket.getInputStream();
			byte[] dataIn = new byte[500];
			byte[] header = new byte[50];
			in.read(header, 0, header.length);
			String type = new String(header).trim();
			
			System.out.println("before switch");
			switch (type) {
				case GET_ALL_FILES:
                    System.out.println("case 1");
					//in.read(dataIn, 50, dataIn.length);
					in.read(dataIn, 0, dataIn.length);
					System.out.println(new String(dataIn).trim());
					getAllFiles(dataIn);
					break;
				case RETRIEVE_FILE:
                    System.out.println("case 2");
					in.read(dataIn, 0, dataIn.length);
					retrieveFile(dataIn);
					break;
				case DELETE_FILE:
                    System.out.println("case 3");
					in.read(dataIn, 0, dataIn.length);
					deleteFile(dataIn);
					break;
				case UPLOAD_FILE:
                    System.out.println("case 4");
					in.read(dataIn, 0, dataIn.length);
					uploadFile(dataIn, in);
					break;
                default:
                    System.out.println("05hyt default case");
			}
			
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
