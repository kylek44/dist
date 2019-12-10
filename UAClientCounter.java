

public class UAClientCounter {
	
	static int count = 0;
	static boolean shutdown = false;
	
	public static synchronized void add() {
		count++;
	}
	
	public static synchronized void remove() {
		count--;
	}
	
	public static int getCount() {
		return count;
	}
	
	public static synchronized void shutdown() {
		shutdown = true;
	}
	
	public static boolean isShutdown() { 
		return shutdown;
	}

}
