import java.io.*;
import java.net.Socket;

public class UADataConnection implements Runnable {
    private Socket socket;

    public UADataConnection(Socket socket) {
        this.socket = socket;
    }

    private void deleteFile(InputStream in) {
        byte[] dataIn = new byte[500];
        try {
            in.read(dataIn, 0, dataIn.length);
            String[] tokens = new String(dataIn).trim().split("\t");
            File dir = new File(tokens[0]);
            boolean s = false;
            byte[] dataOut = new byte[50];
            byte[] success = "SUCCESS".getBytes();
            byte[] failure = "FAILURE".getBytes();
            OutputStream out = socket.getOutputStream();

            if (dir.isDirectory()) {
                File file = new File(tokens[0] + "/" + tokens[1]);

                if (file.exists()) {
                    s = file.delete();
                }
            }
            if (s) {
                for (int i = 0; i < success.length; i++) {
                    dataOut[i] = success[i];
                }
            } else {
                for (int i = 0; i < failure.length; i++) {
                    dataOut[i] = failure[i];
                }
            }

            out.write(dataOut, 0, dataOut.length);
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void uploadFile(InputStream in) {
        try {
            byte[] secondHeader = new byte[500];
            in.read(secondHeader, 0, secondHeader.length);
            String[] tokens = new String(secondHeader).trim().split("\t");
            File dir = new File(tokens[0]);

            if (!dir.exists() || !dir.isDirectory()) {
                dir.mkdirs();
            }

            OutputStream fileOut = new FileOutputStream(new File(tokens[0] + "/" + tokens[1]));
            int bytesRead = -1;
            byte[] dataIn = new byte[1024 * 10];

            while ((bytesRead = in.read(dataIn)) != -1) {
                fileOut.write(dataIn, 0, bytesRead);
            }

            fileOut.flush();
            fileOut.close();

            OutputStream out = socket.getOutputStream();
            byte[] result = new byte[50];
            byte[] success = "SUCCESS".getBytes();

            for (int i = 0; i < success.length; i++) {
                result[i] = success[i];
            }

            out.write(result, 0, result.length);
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void retrieveFile(InputStream in) {
        try {
            byte[] secondHeader = new byte[500];
            in.read(secondHeader, 0, secondHeader.length);
            String[] tokens = new String(secondHeader).trim().split("\t");
            File file = new File(tokens[0] + "/" + tokens[1]);
            InputStream fileIn = new FileInputStream(file);
            OutputStream out = socket.getOutputStream();
            byte[] buffer = new byte[1024 * 10];

            while (fileIn.read(buffer) > -1) {
                out.write(buffer);
            }

            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        try {
            byte[] header = new byte[50];
            byte[] dataIn = new byte[500];

            InputStream in = socket.getInputStream();
            in.read(header, 0, header.length);
            String request = new String(header).trim();

            switch (request) {
                case "DELETE_FILE":
                    deleteFile(in);
                    break;
                case "UPLOAD_FILE":
                    uploadFile(in);
                    break;
                case "RETRIEVE_FILE":
                    retrieveFile(in);
                    break;
                default:
                    System.out.println("Hit default");
                    break;
            }

            in.close();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
