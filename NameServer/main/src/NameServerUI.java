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
        // TODO
        if (command.equals("quit")) {
            // TODO handle name server shutdown
            return false;
        } else if (command.equals("enter")) {

        } else if (command.equals("exit")) {

        } else {
            syncPrint("[ERROR] Unknown command.\n");
        }

        return false;
    }

    @Override
    public void run() {
        while (readUserInput()) { }
    }
}
