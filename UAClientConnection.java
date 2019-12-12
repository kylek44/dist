

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
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
		UADebug.print("Getting all files for User: " + username);
		
		if (userToFiles.containsKey(username)) {
			List<String> files = userToFiles.get(username);
			UADebug.print("Files: " + files);
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
				UADebug.print("File names sent");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void retrieveFile(byte[] dataIn) {
		String[] tokens = new String(dataIn).trim().split("\t");
		UADebug.print("Retrieving File: " + tokens[1] + " for User: " + tokens[0] + "...");
		
		if (userToFiles.containsKey(tokens[0]) && fileToUser.containsKey(tokens[1]) && fileToDataNode.containsKey(tokens[1])) {
			try {
				UADebug.print("Connecting to Data Node: " + fileToDataNode.get(tokens[1]));
				Socket dataNode = new Socket(dataNodeIPs.get(fileToDataNode.get(tokens[1])), dataNodePorts.get(fileToDataNode.get(tokens[1])));
				InputStream dataNodeIn = dataNode.getInputStream();
				OutputStream dataNodeOut = dataNode.getOutputStream();

				byte[] header = new byte[50];
				byte[] dataSent = new byte[500];
				byte[] request = RETRIEVE_FILE.getBytes();
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
				dataNodeOut.write(dataSent, 0, dataSent.length);
				dataNodeOut.flush();

				UADebug.print("Sent headers");

				byte[] dataReceived = new byte[1024 * 10];

				UADebug.print("Retrieving file");
				int bytesReceived = dataNodeIn.read(dataReceived, 0, dataReceived.length);
				
				OutputStream out = socket.getOutputStream();
				out.write(dataReceived, 0, bytesReceived);
				out.flush();
				
				dataNode.close();

				UADebug.print("File sent");
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public void deleteFile(byte[] dataIn) {
		String[] tokens = new String(dataIn).trim().split("\t");
		UADebug.print("Deleting File: " + tokens[1] + " for User: " + tokens[0] + "...");

		if (userToFiles.containsKey(tokens[0]) && fileToUser.containsKey(tokens[1]) && fileToDataNode.containsKey(tokens[1])) {
			try {
				String ip = dataNodeIPs.get(fileToDataNode.get(tokens[1]));
				int port = dataNodePorts.get(fileToDataNode.get(tokens[1]));
				UADebug.print("Connecting to Data Node: " + fileToDataNode.get(tokens[1]));
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
				dataNodeOut.write(dataSent, 0, dataSent.length);
				dataNodeOut.flush();

				UADebug.print("Sent headers");
				
				byte[] dataReceived = new byte[500];
				
				int bytesReceived = dataNodeIn.read(dataReceived, 0, dataReceived.length);
				String result = new String(dataReceived).trim();

				UADebug.print("Result: " + result);

				if (result.equals("SUCCESS")) {
					fileToUser.remove(tokens[1]);
					fileToDataNode.remove(tokens[1]);
					userToFiles.get(tokens[0]).remove(tokens[1]);
				}

				UADebug.print("Sending result");

				OutputStream out = socket.getOutputStream();
				out.write(dataReceived, 0, dataReceived.length);
				out.flush();
				
				dataNode.close();

				UADebug.print("Result sent");
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public void uploadFile(byte[] dataIn, InputStream in) {
		String[] tokens = new String(dataIn).trim().split("\t");
		UADebug.print("Uploading File: " + tokens[1] + " for User: " + tokens[0] + "...");
		System.out.println(tokens[0] + " " + tokens[1]);
		
		if (userToFiles.containsKey(tokens[0])) {
			int hash = tokens[1].hashCode() % dataNodeIPs.size();
			try {
				UADebug.print("Connecting to Data Node: " + serverNumbers.get(hash));
				Socket dataNode = new Socket(dataNodeIPs.get(serverNumbers.get(hash)), dataNodePorts.get(serverNumbers.get(hash)));
				InputStream dataNodeIn = dataNode.getInputStream();
				OutputStream dataNodeOut = dataNode.getOutputStream();
				
				byte[] header = new byte[50];
				byte[] secondHeader = new byte[500];
				byte[] request = UPLOAD_FILE.getBytes();
				byte[] username = tokens[0].getBytes();
				byte[] filename = tokens[1].getBytes();

				for (int i = 0; i < request.length; i++) {
					header[i] = request[i];
				}

				for (int i = 0; i < username.length; i++) {
					secondHeader[i] = username[i];
				}
				
				secondHeader[username.length] = 9;
				
				for (int i = 0; i < filename.length; i++) {
					secondHeader[i + username.length + 1] = filename[i];
				}
				
				dataNodeOut.write(header, 0, header.length);
				dataNodeOut.flush();
				dataNodeOut.write(secondHeader, 0, secondHeader.length);
				dataNodeOut.flush();
				UADebug.print("Sent headers");

				int bytesRead = -1;
				byte[] data = new byte[1024 * 10];
				
				while (in.read(data) != -1) {
					dataNodeOut.write(data);
				}

				dataNodeOut.flush();
				dataNodeOut.close();

				UADebug.print("Sent file");

				userToFiles.get(tokens[0]).add(tokens[1]);
				fileToUser.put(tokens[1], tokens[0]);
				fileToDataNode.put(tokens[1], serverNumbers.get(hash));

//				byte[] dataNodeDataIn = new byte[500];
//				dataNodeIn.read(dataNodeDataIn, 0, dataNodeDataIn.length);
//
//				OutputStream out = socket.getOutputStream();
//				out.write(dataNodeDataIn, 0, dataNodeDataIn.length);
//				out.flush();
//
//				System.out.println("Wrote back to master");
				dataNode.close();
				UADebug.print("Done uploading");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			System.out.println("User doesn't exist");
		}
	}

	private void writeFilemap() {
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter("filemap.txt"));
			for (Map.Entry<String, String> entry : fileToDataNode.entrySet()) {
				out.write(entry.getKey() + " " + entry.getValue());
				out.newLine();
			}
			out.close();
			UADebug.print("filemap.txt updated");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void writeUsermap() {
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter("usermap.txt"));
			for (Map.Entry<String, String> entry : fileToUser.entrySet()) {
				out.write(entry.getValue() + " " + entry.getKey());
				out.newLine();
			}
			out.close();
			UADebug.print("usermap.txt updated");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		try {
			InputStream in = socket.getInputStream();
			byte[] dataIn = new byte[500];
			byte[] header = new byte[50];
			in.read(header, 0, header.length);
			String type = new String(header).trim();
			
			switch (type) {
				case GET_ALL_FILES:
					//in.read(dataIn, 50, dataIn.length);
					in.read(dataIn, 0, dataIn.length);
					System.out.println(new String(dataIn).trim());
					getAllFiles(dataIn);
					break;
				case RETRIEVE_FILE:
					in.read(dataIn, 0, dataIn.length);
					retrieveFile(dataIn);
					break;
				case DELETE_FILE:
					in.read(dataIn, 0, dataIn.length);
					deleteFile(dataIn);
					writeFilemap();
					writeUsermap();
					break;
				case UPLOAD_FILE:
					in.read(dataIn, 0, dataIn.length);
					uploadFile(dataIn, in);
					writeFilemap();
					writeUsermap();
					break;
                default:
                	UADebug.print("Unknown request: " + type);
                    break;
			}

			socket.close();
			UAClientCounter.remove();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
