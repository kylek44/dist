public class UADebug {
    private static final boolean DEBUG = true;

    public static void print(String s) {
        if (DEBUG) {
            System.out.println(s);
        }
    }

}
