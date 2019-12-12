import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class UADataNode {
    private static final int PORT = 32000;
    private ServerSocket server;

    public UADataNode() {
        try {
            server = new ServerSocket(PORT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start() {
        while (true) {
            try {
                Socket socket = server.accept();
                new UADataConnection(socket).run();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        UADataNode node = new UADataNode();
        node.start();
    }
}
