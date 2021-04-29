import java.util.Scanner;

public class BootstrapUI implements Runnable {
    private final String PROMPT;

    private BootstrapNameServer bootstrapServer;
    private Scanner scan;

    public BootstrapUI(BootstrapNameServer bootstrapServer, int bootstrapID) {
        this.bootstrapServer = bootstrapServer;
        scan = new Scanner(System.in);
        PROMPT = "bootstrap [" + bootstrapID + "] >_ ";
    }

    /*
     * Prints response and reprints user input prompt.
     */
    public void printResponse(String response) {
        if (response != null) {
            syncPrint("\n", response, "\n", PROMPT);
        }
    }

    /*
     * Synchronized print of each string arg.
     */
    private void syncPrint(String... strings) {
        synchronized (System.out) {
            // Print response lines
            for (String string: strings) {
                if (string != null) {
                    System.out.print(string);
                }
            }
        }
    }

    private boolean readUserInput() {
        syncPrint(PROMPT);
        String[] input = scan.nextLine().split(" ");
        String command = input[0].toLowerCase();

        if (command.equals("quit")) {
            bootstrapServer.shutdown();
            return false;
        } else if (command.equals("lookup")) {
            int key = Integer.parseInt(input[1]);
            bootstrapServer.lookupKey(key);
        } else if (command.equals("insert")) {
            int key = Integer.parseInt(input[1]);
            String value = input[2];
            bootstrapServer.insertValue(key, value);
        } else if (command.equals("delete")) {
            int key = Integer.parseInt(input[1]);
            bootstrapServer.deleteKey(key);
        } else {
            syncPrint("[ERROR] Unknown command.\n");
        }

        return true;
    }

    @Override
    public void run() {
        while (readUserInput()) { };
    }
}
