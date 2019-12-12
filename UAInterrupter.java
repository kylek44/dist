import java.io.IOException;

public class UAInterrupter {

	public static void main(String[] args) {
		UAClientCounter.shutdown();
		if (UAMaster.getServer() != null) {
			try {
				UAMaster.getServer().close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
