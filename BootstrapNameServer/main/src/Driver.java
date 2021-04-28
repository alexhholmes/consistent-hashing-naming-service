import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import java.util.TreeMap;

public class Driver {
    public static void main(String[] args) throws IOException {
        // Parse config file
        File config = new File(args[0]);
        Scanner configScanner = new Scanner(config);
        int bootstrapID = Integer.parseInt(configScanner.nextLine());
        int bootstrapPort = Integer.parseInt(configScanner.nextLine());

        // Collect initial objects
        TreeMap<Integer, String> objects = new TreeMap<>();
        while (configScanner.hasNextLine()) {
            String[] line = configScanner.nextLine().split(" ");
            int key = Integer.parseInt(line[0]);
            String value = line[1];
            objects.put(key, value);
        }

        configScanner.close();

        BootstrapNameServer bootstrap = new BootstrapNameServer(bootstrapID, bootstrapPort, objects);
        new Thread(bootstrap).start();
    }
}
