import com.sun.net.httpserver.Authenticator;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

public class BootstrapNameServer implements Runnable {

    private BootstrapUI bootstrapUI;

    // Name Servers Connection
    private ServerSocket serverSocket;
    private int bootstrapID;
    private int bootstrapPort;

    // Local Object Storage
    private HashMap<Integer, String> objects;
    private int rangeMin;
    private int rangeMax;

    // Successor
    private int successor;
    private InetAddress successorAddr;
    private int successorPort;

    // Predecessor
    private int predecessor;
    private InetAddress predecessorAddr;
    private int predecessorPort;

    public BootstrapNameServer(int bootstrapID, int bootstrapPort, HashMap<Integer, String> initialObjects) {
        this.bootstrapID = bootstrapID;
        this.bootstrapPort = bootstrapPort;
        this.successor = bootstrapID;
        this.predecessor = bootstrapID;

        this.objects = initialObjects;
        this.rangeMin = 0;
        this.rangeMax = 1023;

        if (initialObjects == null) {
            objects = new HashMap<>();
        }
    }

    /*
     * Looks up a key in local storage, if key is not in key local key range, looks for it
     * in the distributed system. CALLED BY BOOTSTRAP UI.
     */
    // TODO Do check for local key range
    public void lookupKey(final int key) {
        final int[] visitedServers = { 0 };
        if (objects.containsKey(key)) {
            // Key found on this server, immediately reply to user
            String response = lookupKeyResponse(key, objects.get(key), visitedServers);
            bootstrapUI.printResponse(response);
        } else if (successor == bootstrapID) {
            // No other name servers and object not found, immediately reply to user
            String response = lookupKeyResponse(key, null, visitedServers);
            bootstrapUI.printResponse(response);
        } else {
            // Pass lookup message to successor
            try {
                Socket successorSocket = new Socket(successorAddr, successorPort);
                ObjectOutputStream outputStream = new ObjectOutputStream(successorSocket.getOutputStream());
                outputStream.writeUTF("lookup");
                outputStream.writeInt(key);
                outputStream.writeObject(visitedServers);
                outputStream.close();
                successorSocket.close();
            } catch (IOException e) {
                // TODO
            }
        }
    }

    /*
     * Inserts a value in local storage, if local storage is not in key range, inserts it
     * in the distributed system. CALLED BY BOOTSTRAP UI.
     */
    // TODO Do check for local key range
    public void insertValue(int key, String value) {
        final int[] visitedServers = { 0 };
        if (key >= rangeMin && key <= rangeMax) {
            // Store object on this server
            objects.put(key, value);
            String response = insertValueResponse(key, visitedServers);
            bootstrapUI.printResponse(response);
        } else {
            // Pass insert message to successor
            try {
                Socket successorSocket = new Socket(successorAddr, successorPort);
                ObjectOutputStream outputStream = new ObjectOutputStream(successorSocket.getOutputStream());
                outputStream.writeUTF("insert");
                outputStream.writeInt(key);
                outputStream.writeUTF(value);
                outputStream.writeObject(visitedServers);
                outputStream.close();
                successorSocket.close();
            } catch (IOException e) {
                // TODO
            }
        }
    }

    /*
     * Deletes a key in local storage, if local storage is not in key range, deletes it in
     * the distributed system. CALLED BY BOOTSTRAP UI.
     */
    // TODO Do check for local key range
    public void deleteKey(int key) {
        final int[] visitedServers = { 0 };
        if (objects.containsKey(key)) {
            // Key found on this server, immediately reply to user
            objects.remove(key);
            String response = deleteKeyResponse(key, true, visitedServers);
            bootstrapUI.printResponse(response);
        } else if (successor == bootstrapID) {
            // No other name servers and object not found, immediately reply to user
            String response = deleteKeyResponse(key, false, visitedServers);
            bootstrapUI.printResponse(response);
        } else {
            // Pass delete message to successor
            try {
                Socket successorSocket = new Socket(successorAddr, successorPort);
                ObjectOutputStream outputStream = new ObjectOutputStream(successorSocket.getOutputStream());
                outputStream.writeUTF("delete");
                outputStream.writeInt(key);
                outputStream.writeObject(visitedServers);
                outputStream.close();
                successorSocket.close();
            } catch (IOException e) {
                // TODO
            }
        }
    }

    /*
     * Returns a formatted lookup response String.
     */
    private String lookupKeyResponse(final int key, final String object, final int[] visitedServers) {
        String response = "Key: " + key + "\n";
        if (object == null) {
            response += "Key not found";
        } else {
            response += "Value: " + object;
        }
        response += "\nVisited Servers: " + visitedToString(visitedServers);

        return response;
    }

    /*
     * Returns a formatted insert response String.
     */
    private String insertValueResponse(final int key, final int[] visitedServers) {
        return "Key: " + key + "\n" +
                "Inserted on Server: " + visitedServers[visitedServers.length - 1] + "\n" +
                "Visited Servers: " + visitedToString(visitedServers);
    }

    /*
     * Returns a formatted delete response String.
     */
    private String deleteKeyResponse(final int key, final boolean deleted, final int[] visitedServers) {
        String response = "Key: " + key + "\n";
        if (deleted) {
            response += "Successful deletion";
        } else {
            response += "Key not found";
        }
        response += "\nVisited Servers: " + visitedToString(visitedServers);

        return response;
    }

    /*
     * Adds name server to system.
     */
    private void nameServerEnter(ObjectInputStream inputStream, ObjectOutputStream outputStream) {
        try {
            if (successor == bootstrapID) {
                // First name server added
                successor = inputStream.readInt();
                // TODO
            } else {

            }
        } catch (IOException e) {
            // TODO
        }
        // Streams are closed by calling function
    }

    /*
     * Remove name server from system.
     */
    private void nameServerExit(ObjectInputStream inputStream, ObjectOutputStream outputStream) {

        // Streams are closed by calling function
    }

    /*
     * Handles commands from name servers.
     */
    private void handleCommand(String command, ObjectInputStream inputStream, ObjectOutputStream outputStream) {
        String response = null;

        try {
            if (command.equals("lookup")) {
                // Lookup failed
                int key = inputStream.readInt();
                int[] visitedServers = (int[]) inputStream.readObject();
                response = lookupKeyResponse(key, null, visitedServers);
            } else if (command.equals("lookup_found")) {
                int key = inputStream.readInt();
                String object = inputStream.readUTF();
                int[] visitedServers = (int[]) inputStream.readObject();
                response = lookupKeyResponse(key, object, visitedServers);
            } else if (command.equals("insert")) {
                // Insert failed somehow?
                inputStream.readUTF();
                inputStream.readInt();
                inputStream.readObject();
                response = "Insert failed, message reached back to bootstrap server.";
            } else if (command.equals("insert_found")) {
                int key = inputStream.readInt();
                int[] visitedServers = (int[]) inputStream.readObject();
                response = insertValueResponse(key, visitedServers);
            } else if (command.equals("delete")) {
                // Delete failed
                int key = inputStream.readInt();
                int[] visitedServers = (int[]) inputStream.readObject();
                response = deleteKeyResponse(key, false, visitedServers);
            } else if (command.equals("delete_found")) {
                int key = inputStream.readInt();
                int[] visitedServers = (int[]) inputStream.readObject();
                response = deleteKeyResponse(key, true, visitedServers);
            } else if (command.equals("enter")) {
                // Register new name server
                nameServerEnter(inputStream, outputStream);
                response = null;
            } else if (command.equals("exit")) {
                // Deregister name server
                nameServerExit(inputStream, outputStream);
                response = null;
            } else {
                response = "Unknown command received from: " + serverSocket.getInetAddress().toString();
            }
        } catch (IOException e) {
            // Close streams & socket, let client crash
            response = "[ERROR] New connection i/o failed.";
        } catch (ClassNotFoundException e) {
            // Should not happen, exit program
            System.err.println("[ERROR] Input stream failure.");
            System.exit(1);
        }

        bootstrapUI.printResponse(response);
    }

    /*
     * Handles incoming connections from name servers.
     */
    private void acceptConnections() {
        while (true) {
            try {
                Socket sock = serverSocket.accept();
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
        String visitedString = "";
        for (int i: visitedServers) {
            visitedString += i + " ";
        }
        return visitedString;
    }

    @Override
    public void run() {
        // Start UI thread
        bootstrapUI = new BootstrapUI(this);
        new Thread(bootstrapUI).start();

        try {
            serverSocket = new ServerSocket(bootstrapPort);
            acceptConnections();
        } catch (IOException e) {
            System.err.println("[ERROR] Unable to create server socket.");
            System.exit(1);
        }
    }
}
