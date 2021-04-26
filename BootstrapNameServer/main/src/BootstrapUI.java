import java.util.Scanner;

public class BootstrapUI implements Runnable {
    final String prompt = ">_ ";

    BootstrapNameServer bootstrapServer;
    Scanner scan;

    public BootstrapUI(BootstrapNameServer bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
        scan = new Scanner(System.in);
    }

    private boolean readUserInput() {
        System.out.print(prompt);
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
            System.out.println("[ERROR] Unknown command.");
        }

        return true;
    }

    @Override
    public void run() {
        while (readUserInput()) { };
    }
}
