import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
            File file = new File(tokens[1]);
            OutputStream out = socket.getOutputStream();
            byte[] dataOut = new byte[50];
            byte[] success = "SUCCESS".getBytes();
            byte[] failure = "FAILURE".getBytes();
            boolean s = false;

            if (file.exists()) {
                s = file.delete();
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

            out.close();
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

            if (request.equals("DELETE_FILE")) {
                deleteFile(in);
            }

            in.close();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
