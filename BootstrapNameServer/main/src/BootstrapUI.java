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
            if (input.length != 2) {
                syncPrint("[ERROR] lookup <key>\n");
            } else {
                int key = Integer.parseInt(input[1]);

                String message = bootstrapServer.lookupKey(key);
                if (message != null) syncPrint(message + "\n");
            }
        } else if (command.equals("insert")) {
            if (input.length != 3) {
                syncPrint("[ERROR] insert <key> <value>\n");
            } else {
                int key = Integer.parseInt(input[1]);
                String value = input[2];

                String message = bootstrapServer.insertValue(key, value);
                if (message != null) syncPrint(message + "\n");
            }
        } else if (command.equals("delete")) {
            if (input.length != 2) {
                syncPrint("[ERROR] delete <key>\n");
            } else {
                int key = Integer.parseInt(input[1]);

                String message = bootstrapServer.deleteKey(key);
                if (message != null) syncPrint(message + "\n");
            }
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
