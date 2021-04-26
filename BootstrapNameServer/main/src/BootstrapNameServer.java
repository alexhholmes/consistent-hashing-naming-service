import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

public class BootstrapNameServer implements Runnable {
    private ServerSocket serverSocket;
    private int bootstrapID;
    private int bootstrapPort;

    private HashMap<Integer, String> objects;
    private int rangeMin;
    private int rangeMax;

    private int predecessor;
    private int successor;
    private InetAddress successorAddr;
    private int successorPort;

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

    /**
     * Immediately returns the key's value if it is found in this name server else it
     * returns null.
     * @param key
     * @return null if the key is not on this name server
     */
    public void lookupKey(final int key) {
        final int[] visitedServers = { 0 };
        if (objects.containsKey(key)) {
            // Key found on this server, immediately reply to user
            lookupKeyResponse(key, objects.get(key), visitedServers);
        } else if (successor == bootstrapID) {
            // No other name servers and object not found, immediately reply to user
            lookupKeyResponse(key, null, visitedServers);
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

    public void insertValue(int key, String value) {
        final int[] visitedServers = { 0 };
        if (key >= rangeMin && key <= rangeMax) {
            // Store object on this server
            objects.put(key, value);
            insertValueResponse(key, visitedServers);
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

    public void deleteKey(int key) {
        final int[] visitedServers = { 0 };
        if (objects.containsKey(key)) {
            // Key found on this server, immediately reply to user
            objects.remove(key);
            deleteKeyResponse(key, true, visitedServers);
        } else if (successor == bootstrapID) {
            // No other name servers and object not found, immediately reply to user
            deleteKeyResponse(key, false, visitedServers);
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

    private void lookupKeyResponse(final int key, final String object, final int[] visitedServers) {
        System.out.println("Key: " + key);
        if (object == null) {
            System.out.println("Key not found");
        } else {
            System.out.println("Value: " + object);
        }
        System.out.println("Visited Servers: " + visitedToString(visitedServers));
    }

    private void insertValueResponse(final int key, final int[] visitedServers) {
        System.out.println("Key: " + key);
        System.out.println("Inserted on server " + visitedServers[visitedServers.length - 1]);
        System.out.println("Visited Servers: " + visitedToString(visitedServers));
    }

    private void deleteKeyResponse(final int key, final boolean deleted, final int[] visitedServers) {
        System.out.println("Key: " + key);
        if (deleted) {
            System.out.println("Successful deletion");
        } else {
            System.out.println("Key not found");
        }
        System.out.println("Visited Servers: " + visitedToString(visitedServers));
    }

    private void nameServerEnter() {

    }

    private void nameServerExit() {

    }

    private void acceptConnections() {
        while (true) {
            try {
                Socket sock = serverSocket.accept();
                ObjectInputStream inputStream = new ObjectInputStream(sock.getInputStream());
                ObjectOutputStream outputStream = new ObjectOutputStream(sock.getOutputStream());

                String command = inputStream.readUTF();
                if (command == "lookup") {
                    // Lookup failed
                    int key = inputStream.readInt();
                    int[] visitedServers = (int[]) inputStream.readObject();
                    lookupKeyResponse(key, null, visitedServers);
                } else if (command == "lookup_found") {
                    int key = inputStream.readInt();
                    String object = inputStream.readUTF();
                    int[] visitedServers = (int[]) inputStream.readObject();
                    lookupKeyResponse(key, object, visitedServers);
                } else if (command == "insert") {
                    // Insert failed somehow?
                    System.err.println("Insert failed, message reached back to bootstrap server");
                    System.exit(1);
                } else if (command == "insert_found") {
                    int key = inputStream.readInt();
                    int[] visitedServers = (int[]) inputStream.readObject();
                    insertValueResponse(key, visitedServers);
                } else if (command == "delete") {
                    // Delete failed
                    int key = inputStream.readInt();
                    int[] visitedServers = (int[]) inputStream.readObject();
                    deleteKeyResponse(key, false, visitedServers);
                } else if (command == "delete_found") {
                    int key = inputStream.readInt();
                    int[] visitedServers = (int[]) inputStream.readObject();
                    deleteKeyResponse(key, true, visitedServers);
                } else if (command == "enter") {
                    // Register new name server

                } else if (command == "exit") {
                    // Deregister name server

                } else {
                    System.err.println("Unknown command received from: " + serverSocket.getInetAddress().toString());
                    System.exit(1);
                }
            } catch (IOException | ClassNotFoundException e) {
                System.err.println("[ERROR] New connection failed.");
            }
        }
    }

    private String visitedToString(final int[] visitedServers) {
        String visitedString = "";
        for (int i: visitedServers) {
            visitedString += i + " ";
        }
        return visitedString;
    }

    @Override
    public void run() {
        try {
            serverSocket = new ServerSocket(bootstrapPort);
            acceptConnections();
        } catch (IOException e) {
            System.err.println("[ERROR] Unable to create server socket.");
            System.exit(1);
        }
    }
}
