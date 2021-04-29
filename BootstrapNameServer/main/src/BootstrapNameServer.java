import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.NavigableMap;
import java.util.TreeMap;

public class BootstrapNameServer implements Runnable {

    public static final int MAX_OBJECT_AMOUNT = 1024;

    private BootstrapUI bootstrapUI;

    // Name Servers Connection
    private ServerSocket serverSocket;
    private int bootstrapID;
    private int bootstrapPort;

    // Local Object Storage
    private TreeMap<Integer, String> objects; // TreeMap allows for creating a sub-map from a range
    private int rangeStart;
    private int rangeEnd;

    // Successor
    private int successor;
    private InetAddress successorAddr;
    private int successorPort;

    // Predecessor
    private int predecessor;
    private InetAddress predecessorAddr;
    private int predecessorPort;

    public BootstrapNameServer(int bootstrapID, int bootstrapPort, TreeMap<Integer, String> initialObjects) {
        this.bootstrapID = bootstrapID;
        this.bootstrapPort = bootstrapPort;
        this.successor = bootstrapID;
        this.predecessor = bootstrapID;

        this.objects = initialObjects;
        this.rangeStart = 1;
        this.rangeEnd = 0;

        if (initialObjects == null) {
            objects = new TreeMap<>();
        }
    }

    /*
     * Looks up a key in local storage, if key is not in key local key range, looks for it
     * in the distributed system. CALLED BY BOOTSTRAP UI.
     */
    public void lookupKey(final int key) {
        final int[] visitedServers = new int[] { 0 };
        if (betweenRange(key, rangeStart, rangeEnd)) {
            // Key should be stored on this bootstrap server
            if (objects.containsKey(key)) {
                // Key found on this server, immediately reply to user
                String response = lookupKeyResponse(key, objects.get(key), visitedServers);
                bootstrapUI.printResponse(response);
            } else {
                // No other name servers and object not found, immediately reply to user
                String response = lookupKeyResponse(key, null, visitedServers);
                bootstrapUI.printResponse(response);
            }
        } else {
            // Pass lookup message to successor
            forwardCommand("lookup", key, visitedServers);
        }
    }

    /*
     * Inserts a value in local storage, if local storage is not in key range, inserts it
     * in the distributed system. CALLED BY BOOTSTRAP UI.
     */
    public void insertValue(int key, String value) {
        final int[] visitedServers = new int[] { 0 };
        if (betweenRange(key, rangeStart, rangeEnd)) {
            // Store object on this server
            objects.put(key, value);
            String response = insertValueResponse(key, value, visitedServers);
            bootstrapUI.printResponse(response);
        } else {
            // Pass insert message to successor
            forwardCommand("insert", key, value, visitedServers);
        }
    }

    /*
     * Deletes a key in local storage, if local storage is not in key range, deletes it in
     * the distributed system. CALLED BY BOOTSTRAP UI.
     */
    public void deleteKey(int key) {
        final int[] visitedServers = new int[] { 0 };
        if (betweenRange(key, rangeStart, rangeEnd)) {
            if (objects.containsKey(key)) {
                // Key found on this server, immediately reply to user
                objects.remove(key);
                String response = deleteKeyResponse(key, true, visitedServers);
                bootstrapUI.printResponse(response);
            } else {
                // Object not found, immediately reply to user
                String response = deleteKeyResponse(key, false, visitedServers);
                bootstrapUI.printResponse(response);
            }
        } else {
            // Pass delete message to successor
            forwardCommand("delete", key, visitedServers);
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
    private String insertValueResponse(final int key, String value, final int[] visitedServers) {
        return "Key: " + key + " with value: " + value + "\n" +
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

    private void forwardCommand(String command, int key, int[] visitedServers) {
        forwardCommand(command, key, null, visitedServers);
    }

    private void forwardCommand(String command, int key, String value, int[] visitedServers) {
        Socket successorSocket = null;
        ObjectOutputStream outputStream = null;
        ObjectInputStream inputStream = null;

        try {
            successorSocket = new Socket(successorAddr, successorPort);
            outputStream = new ObjectOutputStream(successorSocket.getOutputStream());
            inputStream = new ObjectInputStream(successorSocket.getInputStream());

            outputStream.writeUTF(command);
            outputStream.writeInt(key);
            if (value != null) outputStream.writeUTF(value);
            outputStream.writeObject(visitedServers);

        } catch (IOException e) {
            System.err.println("[ERROR] Problem occurred when forwarding request to successor name server.");
        } finally {
            try { if (outputStream != null) outputStream.close(); } catch (IOException e) { }
            try { if (inputStream != null) outputStream.close(); } catch (IOException e) { }
            try { if (successorSocket != null) successorSocket.close(); } catch (IOException e) { }
        }
    }

    private void forwardCommand(String command, int newNameServerID, InetAddress newNameServerAddr,
                                int newNameServerPort, int[] visitedServers) {
        Socket successorSocket = null;
        ObjectOutputStream outputStream = null;
        ObjectInputStream inputStream = null;

        try {
            successorSocket = new Socket(successorAddr, successorPort);
            outputStream = new ObjectOutputStream(successorSocket.getOutputStream());
            inputStream = new ObjectInputStream(successorSocket.getInputStream());

            outputStream.writeUTF(command);
            outputStream.writeInt(newNameServerID);
            outputStream.writeObject(newNameServerAddr);
            outputStream.writeInt(newNameServerPort);
            outputStream.writeObject(visitedServers);
        } catch (IOException e) {
            System.err.println("[ERROR] Problem occurred when forwarding request to successor name server.");
        } finally {
            try { if (outputStream != null) outputStream.close(); } catch (IOException e) { }
            try { if (inputStream != null) outputStream.close(); } catch (IOException e) { }
            try { if (successorSocket != null) successorSocket.close(); } catch (IOException e) { }
        }
    }

    /*
     * Moves the (inclusive, exclusive) range of keys from this name server to the new name server.
     */
    private void moveStoredObjects(ObjectOutputStream outputStream, int rangeStart, int rangeEnd) throws IOException {
        if (rangeStart < rangeEnd) {
            NavigableMap<Integer, String> nameServerObjects = objects.subMap(rangeStart, false, rangeEnd, true);
            outputStream.writeObject(nameServerObjects);
            outputStream.flush();

            // Removes this range of keys from the objects tree map after sent.
            nameServerObjects.clear();
        } // else {
        // Okay, not gonna both implementing this because we can safely assume with
        // the project description that bootstrap will ALWAYS be ID of 0. For some
        // reason we still have to read the ID from the config file??? In the case
        // were you'd actually want to implement this stuff, there are a bunch of
        // lingering flaws in this project with not checking if a new index go out of
        // bounds of the possible server IDs (Name server IDs allowed are between
        // [0, 1024].
        // }

    }

    private void immediateEntry(ObjectOutputStream outputStream, boolean firstEntry) throws IOException {
        // Successfully added immediately
        outputStream.writeBoolean(true);
        outputStream.writeBoolean(firstEntry);
        outputStream.flush();

        if (firstEntry) {
            // Key range
            int rangeStart = bootstrapID + 1;
            outputStream.writeInt(rangeStart);
        } else {
            // New name server will contact bootstrap's predecessor and exchange info
            outputStream.writeInt(predecessor);
            outputStream.writeObject(predecessorAddr);
            outputStream.writeInt(predecessorPort);
        }

        // Bootstrap server is successor
        // Already knows IP and port #
        outputStream.writeInt(bootstrapID);
    }

    /*
     * Adds name server to system.
     */
    private void nameServerEnter(ObjectInputStream inputStream, ObjectOutputStream outputStream) {
        try {
            // Read new name server info
            int newID = inputStream.readInt();
            InetAddress newAddr = InetAddress.getByName(inputStream.readUTF());
            int newPort = inputStream.readInt();

            // Check if ID is in use
            if ((newID == bootstrapID || newID == successor)) {
                // Name server should print error and panic
                outputStream.writeBoolean(true);
                outputStream.flush();
                return;
            }
            outputStream.writeBoolean(false);
            outputStream.flush();

            if (successor == bootstrapID) {
                // First name server added
                // Notify of immediate entry and send successor/predecessor info.
                immediateEntry(outputStream, true);
                moveStoredObjects(outputStream, bootstrapID, newID);

                // Update bootstrap's successor/predecessor
                successor = predecessor = newID;
                successorAddr = predecessorAddr = newAddr;
                successorPort = predecessorPort = newPort;

                // Update bootstrap's key ranges
                rangeStart = newID + 1;
                // rangeEnd always stays the same; rangeEnd == bootstrapID

                outputStream.flush();
            } else if (betweenRange(newID, successor, bootstrapID)) { // Already checked if newID does not equal bootstrapID & successorID
                // New name server becomes predecessor to bootstrap server
                immediateEntry(outputStream, false);
                moveStoredObjects(outputStream, predecessor, newID);

                // Update bootstrap's predecessor
                predecessor = newID;
                predecessorAddr = newAddr;
                predecessorPort = newPort;

                // Update bootstrap's key ranges
                rangeStart = newID + 1;
                // rangeEnd always stays the same; rangeEnd == bootstrapID

                outputStream.flush();
            } else {
                // Forwarding name server entry to successor
                // New node should expect to receive entry info from a different name server
                outputStream.writeBoolean(false);
                outputStream.flush();

                // Name server that becomes the new node's successor will directly contact
                // the new node upon successful entry.
                forwardCommand("enter", newID, newAddr, newPort, new int[] { 0 });
            }
        } catch (IOException e) {
            System.err.println("[ERROR] Connection problem occurred when adding new name server.");
        }

        // Streams and socket are closed by calling function
    }

    /*
     * Remove name server from system.
     */
    private void nameServerExit(ObjectInputStream inputStream, ObjectOutputStream outputStream) {
        // TODO
        // Streams and socket are closed by calling function
    }

    /*
     * Handles commands from name servers.
     *
     * lookup       | Lookup went to all name servers, lookup failed.
     * lookup_found | Lookup successful, name server directly contacted bootstrap server.
     * insert       | Insert went to all name servers, insert failed. Should not happen,
     *              |  for debugging purposes!
     * insert_found | Insert successful, name server directly contacted bootstrap server.
     * delete       | Delete went to all name servers, delete failed, key/value not found.
     * delete_found | Delete successful, name server directly contacted bootstrap server.
     * enter        | New name server entering system.
     * new_successor| New name server contacts its new predecessor. This name server
     *              |  updates its successor.
     * exit         | Removes a name server from the system.
     * [UNKNOWN]    | Prints "unknown command" message, continues. Should not happen, for
     *              |  debugging purposes!
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
                inputStream.readInt();
                inputStream.readUTF();
                inputStream.readObject();
                response = "Insert failed, message reached back to bootstrap server.";
            } else if (command.equals("insert_found")) {
                int key = inputStream.readInt();
                String value = inputStream.readUTF();
                int[] visitedServers = (int[]) inputStream.readObject();
                response = insertValueResponse(key, value, visitedServers);
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
                response = null; // TODO?
            } else if (command.equals("new_successor")) {
                this.successor = inputStream.readInt();
                this.successorAddr = (InetAddress) inputStream.readObject();
                this.successorPort = inputStream.readInt();
                response = null;
            } else if (command.equals("exit")) {
                // Deregister name server
                nameServerExit(inputStream, outputStream);
                response = null; // TODO?
            } else {
                response = "Unknown command received from predecessor(Name Server " + predecessor + ").";
            }
        } catch (IOException e) {
            // Close streams & socket, let client crash
            response = "[ERROR] New connection i/o failed.";
        } catch (ClassNotFoundException e) {
            // Should not happen, exit program
            System.err.println("[ERROR] Input stream failure.");
            e.printStackTrace();
            System.exit(1);
        }

        bootstrapUI.printResponse(response);
    }

    /*
     * Handles incoming connections from name servers.
     */
    private void acceptConnections() {
        while (true) {
            Socket sock = null;
            ObjectInputStream inputStream = null;
            ObjectOutputStream outputStream = null;

            try {
                sock = serverSocket.accept();
                inputStream = new ObjectInputStream(sock.getInputStream());
                outputStream = new ObjectOutputStream(sock.getOutputStream());

                String command = inputStream.readUTF();
                handleCommand(command, inputStream, outputStream);
            } catch (IOException e) {
                System.err.println("[ERROR] New connection failed.");
            } finally {
                try { if (inputStream != null) inputStream.close(); } catch (IOException e) { }
                try { if (outputStream != null) outputStream.close(); } catch (IOException e) { }
                try { if (sock != null) sock.close(); } catch (IOException e) { }
            }
        }
    }

    public void shutdown() {
        try {
            serverSocket.close();
            // How did I handle this in proj 3?
        } catch (IOException e) {

        }
        // TODO Tell successor to exit
    }

    /*
     * Checks if an index is between (inclusive) the given range; range can overflow past
     * MAX_OBJECT_COUNT.
     */
    private boolean betweenRange(int index, int rangeStart, int rangeEnd) throws IndexOutOfBoundsException {
        // Index should be between 0 and (MAX_OBJECT_COUNT - 1)
        if (index < 0 && index >= MAX_OBJECT_AMOUNT) throw new IndexOutOfBoundsException();

       if (rangeStart <= rangeEnd) {
           return index >= rangeStart && index <= rangeEnd;
       } else {
            return index >= rangeStart || index <= rangeEnd;
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
        // Start UI thread
        bootstrapUI = new BootstrapUI(this, bootstrapID);
        new Thread(bootstrapUI).start();

        try {
            serverSocket = new ServerSocket(bootstrapPort);
            acceptConnections();
        } catch (IOException e) {
            System.err.println("[ERROR] Unable to create server socket.");
            System.exit(1);
        } finally {
            try { if (serverSocket != null) serverSocket.close(); } catch (IOException e) { }
        }
    }
}
