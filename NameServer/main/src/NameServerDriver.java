import java.io.File;
import java.io.IOException;
import java.util.Scanner;

public class NameServerDriver {
    public static void main(String[] args) throws IOException {
        // Parse config file
        File config = new File(args[0]);
        Scanner configScanner = new Scanner(config);
        int nameServerID = Integer.parseInt(configScanner.nextLine());
        int nameServerPort = Integer.parseInt(configScanner.nextLine());
        String bootstrapServerAddr = configScanner.next();
        int bootstrapServerPort = configScanner.nextInt();

        configScanner.close();

        NameServer nameServer = new NameServer(nameServerID, nameServerPort, bootstrapServerAddr, bootstrapServerPort);
        new Thread(nameServer).start();
    }
}
