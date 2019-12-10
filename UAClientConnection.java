

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
	private ConcurrentHashMap<String, String> dataNodeMap;
	private ConcurrentHashMap<Integer, String> serverNumbers;
	private ConcurrentHashMap<String, String> fileToDataNode;
	private ConcurrentHashMap<String, String> fileToUser;
	private ConcurrentHashMap<String, List<String>> userToFiles;
	
	public UAClientConnection(Socket socket, ConcurrentHashMap<String, String> dataNodeMap, ConcurrentHashMap<Integer, String> serverNumbers, ConcurrentHashMap<String, String> fileToDataNode, ConcurrentHashMap<String, String> fileToUser, ConcurrentHashMap<String, List<String>> userToFiles) {
		this.socket = socket;
		this.dataNodeMap = dataNodeMap;
		this.serverNumbers = serverNumbers;
		this.fileToDataNode = fileToDataNode;
		this.fileToUser = fileToUser;
		this.userToFiles = userToFiles;
	}
	
	public void getAllFiles(byte[] bytesRead) {
		String username = new String(bytesRead).trim();
		
		if (userToFiles.containsKey(username)) {
			List<String> files = userToFiles.get(username);
			byte[] data = new byte[2048];
			int pos = 0;
			System.out.println("WAY before flushing turdwords");
			for (String file : files) {
				if (pos > 0) {
					data[pos++] = 9;
				}
				byte[] fileNameBytes = file.getBytes();
				for (int i = 0; i < fileNameBytes.length; i++) {
					data[pos++] = fileNameBytes[i];
				}
			}
			System.out.println("before flushing turdwords");
			try {
				OutputStream out = socket.getOutputStream();
				out.write(data);
				out.flush();
				System.out.println("flush turdwords");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void retrieveFile(byte[] bytesRead) {
		String[] tokens = new String(bytesRead).trim().split("\t");
		
		if (userToFiles.containsKey(tokens[0]) && fileToUser.containsKey(tokens[1]) && fileToDataNode.containsKey(tokens[1])) {
			try {
				Socket dataNode = new Socket(dataNodeMap.get(fileToDataNode.get(tokens[1])), 32000);
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
	
	public void deleteFile(byte[] bytesRead) {
		String[] tokens = new String(bytesRead).trim().split("\t");
		
		if (userToFiles.containsKey(tokens[0]) && fileToUser.containsKey(tokens[1]) && fileToDataNode.containsKey(tokens[1])) {
			try {
				Socket dataNode = new Socket(dataNodeMap.get(fileToDataNode.get(tokens[1])), 32000);
				InputStream dataNodeIn = dataNode.getInputStream();
				OutputStream dataNodeOut = dataNode.getOutputStream();
				
				byte[] dataSent = new byte[500];
				byte[] request = DELETE_FILE.getBytes();
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
				
				byte[] dataReceived = new byte[500];
				
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
	
	public void uploadFile(byte[] dataIn, InputStream in) {
		String[] tokens = new String(dataIn).trim().split("\t");
		
		if (userToFiles.contains(tokens[0])) {
			int hash = tokens[1].hashCode() % dataNodeMap.size();
			try {
				Socket dataNode = new Socket(dataNodeMap.get(serverNumbers.get(hash)), 32000);
				InputStream dataNodeIn = dataNode.getInputStream();
				OutputStream dataNodeOut = dataNode.getOutputStream();
				
				byte[] header = new byte[500];
				byte[] request = UPLOAD_FILE.getBytes();
				byte[] username = tokens[0].getBytes();
				byte[] filename = tokens[1].getBytes();
				
				for (int i = 0; i < request.length; i++) {
					header[i] = request[i];
				}
				
				for (int i = 0; i < username.length; i++) {
					header[i + 50] = username[i];
				}
				
				header[50 + username.length] = 9;
				
				for (int i = 0; i < filename.length; i++) {
					header[i + 50 + username.length + 1] = filename[i];
				}
				
				dataNodeOut.write(header, 0, header.length);
				dataNodeOut.flush();
				
				int bytesRead = -1;
				byte[] data = new byte[1024 * 10];
				
				while ((bytesRead = in.read(data)) != -1) {
					dataNodeOut.write(data, 0, bytesRead);
				}
				
				dataNodeOut.flush();
				
				byte[] dataNodeDataIn = new byte[500];
				dataNodeIn.read(dataNodeDataIn, 0, dataNodeDataIn.length);
				
				OutputStream out = socket.getOutputStream();
				out.write(dataNodeDataIn, 0, dataNodeDataIn.length);
				out.flush();
				
				dataNode.close();
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public void run() {
		try {
            System.out.println("start");
			InputStream in = socket.getInputStream();
			byte[] dataIn = new byte[4096];
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
					in.read(dataIn, 50, dataIn.length);
					retrieveFile(dataIn);
					break;
				case DELETE_FILE:
                    System.out.println("case 3");
					in.read(dataIn, 50, dataIn.length);
					deleteFile(dataIn);
					break;
				case UPLOAD_FILE:
                    System.out.println("case 4");
					in.read(dataIn, 50, 450);
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
