import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;

public class NameServer implements Runnable {

    private NameServerUI nameServerUI;

    private int nameServerID;
    private int nameServerPort;
    private ServerSocket incomingSocket;

    private HashMap<Integer, String> objects;
    private int rangeMin;
    private int rangeMax;

    volatile boolean connected;
    volatile boolean exit;

    public NameServer(int nameServerID, int nameServerPort, String bootstrapServerAddr, int bootstrapServerPort) {
        this.nameServerID = nameServerID;
        this.nameServerPort = nameServerPort;
        this.connected = false;
    }

    private String buildLogMessage(String command, int key, String value, int[] visitedServers) {
        final String LOG = "[LOG]: ";
        final String TAB = "\t";

        String logMessage = LOG + TAB + command + TAB + key + TAB;
        if (value != null) {
            logMessage += value + TAB;
        }
        logMessage += visitedToString(visitedServers) + "\n";

        return logMessage;
    }

    private String buildLogMessage(String command, int key, int[] visitedServers) {
        return buildLogMessage(command, key, null, visitedServers);
    }

    private String lookupKey(final int key, int[] visitedServers) {
        // Append ID to visitedServers
        int length = visitedServers.length;
        visitedServers = Arrays.copyOf(visitedServers, length - 1);
        visitedServers[length] = nameServerID;

        String message = buildLogMessage("lookup", key, visitedServers);

        if (objects.containsKey(key)) {
            // Key found, message bootstrap server
            String value = objects.get(key);
            // TODO
            message += "Key found, messaging bootstrap server";
        } else {
            // Forward message to successor
            // TODO
            message += "Key not found, forwarding message to successor";
        }

        return message;
    }

    private String insertValue(final int key, final String object, int[] visitedServers) {
    }

    private String deleteKey(final int key, int[] visitedServers) {

    }

    private void handleCommand(String command, ObjectInputStream inputStream, ObjectOutputStream outputStream) {
        String message = null;

        try {
            if (command.equals("shutdown")) {
                // Shuts down this name server and tell successor to shut down as well
                // TODO
            }

            if (command.equals("lookup")) {
                int key = inputStream.readInt();
                int[] visitedServers = (int[]) inputStream.readObject();
                message = lookupKey(key, visitedServers);
            } else if (command.equals("insert")) {
                int key = inputStream.readInt();
                String value = inputStream.readUTF();
                int[] visitedServers = (int[]) inputStream.readObject();
                message = insertValue(key, value, visitedServers);
            } else if (command.equals("delete")) {
                int key = inputStream.readInt();
                int[] visitedServers = (int[]) inputStream.readObject();
                message = deleteKey(key, visitedServers);
            } else {
                message = "Unknown command received from predecessor(Name Server " + predecessor + ").";
            }
        } catch (IOException e) {
            // Close streams & socket, let client crash
            message = "[ERROR] New connection i/o failed.";
        } catch (ClassNotFoundException e) {
            // Should not happen, exit program
            System.err.println("[ERROR] Input stream failure.");
            System.exit(1);
        }

        nameServerUI.printMessage(message);
    }

    private void acceptConnections() {
        while (true) {
            try {
                Socket sock = incomingSocket.accept();
                ObjectInputStream inputStream = new ObjectInputStream(sock.getInputStream());
                ObjectOutputStream outputStream = new ObjectOutputStream(sock.getOutputStream());

                String command = inputStream.readUTF();
                handleCommand(command, inputStream, outputStream);

                try {
                    inputStream.close();
                    outputStream.close();
                    sock.close();
                } catch (IOException ex) {
                    System.err.println("[ERROR] Error closing streams & socket of new connection.");
                }
            } catch (IOException e) {
                System.err.println("[ERROR] New connection failed.");
            }
        }
    }

    /*
     * Returns a space separated String of ints.
     */
    private String visitedToString(final int[] visitedServers) {
        StringBuilder visitedString = new StringBuilder("");
        visitedString.ensureCapacity(visitedServers.length * 2 + 1);

        for (int id : visitedServers) {
            visitedString.append(id);
            visitedString.append(" ");
        }
        visitedString.deleteCharAt(visitedString.length() - 1);

        return visitedString.toString();
    }

    @Override
    public void run() {
        nameServerUI = new NameServerUI(this, nameServerID);
        new Thread(nameServerUI).start();

        try {
            incomingSocket = new ServerSocket(nameServerPort);
            acceptConnections();
        } catch (IOException e) {
            System.err.println("[ERROR] Unable to create server socket.");
            System.exit(1);
        }
    }
}
