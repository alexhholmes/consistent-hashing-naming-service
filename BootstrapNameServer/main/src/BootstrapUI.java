import java.util.Scanner;

public class BootstrapUI implements Runnable {
    private final String PROMPT = ">_ ";

    private BootstrapNameServer bootstrapServer;
    private Scanner scan;

    public BootstrapUI(BootstrapNameServer bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
        scan = new Scanner(System.in);
    }

    public void printResponse(String response) {
        if (response != null) {
            syncPrint(response);
        }
    }

    private void syncPrint(String... lines) {
        synchronized (System.out) {
            // Print response lines
            for (String line: lines) {
                System.out.println(line);
            }

            // Reprint prompt and scan buffer after printing response
            System.out.print(PROMPT);
        }
    }

    private boolean readUserInput() {
        synchronized (System.out) {
            System.out.println(PROMPT);
        }
        String[] input = scan.nextLine().split(" ");
        String command = input[0].toLowerCase();

        if (command.equals("quit")) {
            // TODO handle bootstrap server quit
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
            syncPrint("[ERROR] Unknown command.");
        }

        return true;
    }

    @Override
    public void run() {
        while (readUserInput()) { };
    }
}
