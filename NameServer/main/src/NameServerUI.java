import java.util.Scanner;

public class NameServerUI implements Runnable {
    private final String PROMPT;

    private NameServer nameServer;
    private Scanner scan;

    public NameServerUI(NameServer nameServer, int nameServerID) {
        this.nameServer = nameServer;
        scan = new Scanner(System.in);
        PROMPT = "name server [" + nameServerID + "] >_ ";
    }

    /*
     * Prints response and reprints user input prompt.
     */
    public void printMessage(String response) {
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
            nameServer.exit();
            nameServer.shutdown();
            return false;
        } else if (command.equals("enter")) {
            String message = nameServer.enter();
            if (message != null) {
                syncPrint(message + "\n");
            }
        } else if (command.equals("exit")) {
            String message = nameServer.exit();
            if (message != null) {
                syncPrint(message + "\n");
            }
        } else {
            syncPrint("[ERROR] Unknown command.\n");
        }
        return true;
    }

    @Override
    public void run() {
        while (readUserInput()) { }
    }
}
