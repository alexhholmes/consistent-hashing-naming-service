import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.NavigableMap;
import java.util.TreeMap;

public class NameServer implements Runnable {

    public static final int MAX_OBJECT_AMOUNT = 1024;

    private NameServerUI nameServerUI;

    private int nameServerID;
    private InetAddress nameServerAddr;
    private int nameServerPort;
    private ServerSocket incomingSocket;

    private InetAddress bootstrapServerAddr;
    private int bootstrapServerPort;

    private TreeMap<Integer, String> objects;
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

    private boolean connected;
    private volatile boolean isShutdown;// TODO

    public NameServer(int nameServerID, int nameServerPort, String bootstrapServerAddr, int bootstrapServerPort) throws UnknownHostException {
        this.nameServerID = nameServerID;
        this.nameServerAddr = InetAddress.getLocalHost();
        this.nameServerPort = nameServerPort;
        this.connected = false;
        this.bootstrapServerAddr = InetAddress.getByName(bootstrapServerAddr);
        this.bootstrapServerPort = bootstrapServerPort;
        this.objects = new TreeMap<>();
    }

    private void messageNewPredecessor() {
        Socket predecessorSocket = null;
        ObjectOutputStream outputStream = null;
        ObjectInputStream inputStream = null;

        try {
            predecessorSocket = new Socket(predecessorAddr, predecessorPort);
            outputStream = new ObjectOutputStream(predecessorSocket.getOutputStream());
            inputStream = new ObjectInputStream(predecessorSocket.getInputStream());

            outputStream.writeUTF("new_successor");
            outputStream.writeInt(nameServerID);
            outputStream.writeObject(nameServerAddr);
            outputStream.writeInt(nameServerPort);
            outputStream.flush();
        } catch (IOException e) {
            System.err.println("[ERROR] Problem occurred when forwarding request to bootstrap name server.");
        } finally {
            try { if (outputStream != null) outputStream.close(); } catch (IOException e) { }
            try { if (inputStream != null) inputStream.close(); } catch (IOException e) { }
            try { if (predecessorSocket != null) predecessorSocket.close(); } catch (IOException e) { }
        }
}

    private String immediateEntry(ObjectInputStream inputStream) throws IOException, ClassNotFoundException {
        boolean isFirstEntry = inputStream.readBoolean();
        int bootstrapID;

        if (isFirstEntry) {
            rangeStart = inputStream.readInt();
            rangeEnd = nameServerID;

            bootstrapID = inputStream.readInt();

            successor = predecessor = bootstrapID;
            successorAddr = predecessorAddr = bootstrapServerAddr;
            successorPort = predecessorPort = bootstrapServerPort;
        } else {
            // Must contact predecessor and exchange info
            predecessor = inputStream.readInt();
            predecessorAddr = (InetAddress) inputStream.readObject();
            predecessorPort = inputStream.readInt();

            bootstrapID = inputStream.readInt();

            successor = bootstrapID;
            successorAddr = bootstrapServerAddr;
            successorPort = bootstrapServerPort;

            rangeStart = predecessor + 1;
            rangeEnd = nameServerID;

            messageNewPredecessor();
        }
        return buildEntrySuccessMessage(new int[] { bootstrapID });
    }

    public String enter() {
        if (connected) {
            return "[ERROR] Already connected.";
        }

        String returnMessage = null;
        Socket bootstrapSocket = null;
        ObjectOutputStream outputStream = null;
        ObjectInputStream inputStream = null;

        try {
            bootstrapSocket = new Socket(bootstrapServerAddr, bootstrapServerPort);
            outputStream = new ObjectOutputStream(bootstrapSocket.getOutputStream());
            inputStream = new ObjectInputStream(bootstrapSocket.getInputStream());

            outputStream.writeUTF("enter");
            outputStream.writeInt(nameServerID);
            outputStream.writeUTF(nameServerAddr.getHostAddress());
            outputStream.writeInt(nameServerPort);
            outputStream.flush();

            // Check if ID is in use (bootstrap will only compare with its own ID and its
            // successor ID.
            if (inputStream.readBoolean()) {
                System.err.println("[ERROR] ID is already in use!");
                System.exit(1);
            }

            // If immediately added by bootstrap name server, continue. Else...
            // Message was forwarded to bootstrap's successor name server, will have
            // to wait to receive an "enter_success" command from new successor.
            if (inputStream.readBoolean()) {
                returnMessage = immediateEntry(inputStream);
                objects.putAll((NavigableMap<Integer, String>) inputStream.readObject());
            }

            connected = true;
        } catch (IOException e) {
            System.err.println("[ERROR] Problem occurred when forwarding request to bootstrap name server.");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            try { if (outputStream != null) outputStream.close(); } catch (IOException e) { }
            try { if (inputStream != null) inputStream.close(); } catch (IOException e) { }
            try { if (bootstrapSocket != null) bootstrapSocket.close(); } catch (IOException e) { }
        }

        return returnMessage;
    }

    public String exit() {
        if (!connected) {
            return "[ERROR] Already disconnected.";
        }


        // TODO
    }

    private void messageBootstrap(String command, int key, int[] visitedServers) {
        messageAny(command, key, null, visitedServers, bootstrapServerAddr, bootstrapServerPort);
    }

    private void messageBootstrap(String command, int key, String value, int[] visitedServers) {
        messageAny(command, key, value, visitedServers, bootstrapServerAddr, bootstrapServerPort);
    }

    private void forwardCommand(String command, int key, int[] visitedServers) {
        messageAny(command, key, null, visitedServers, successorAddr, successorPort);
    }

    private void forwardCommand(String command, int key, String value, int[] visitedServers) {
        messageAny(command, key, value, visitedServers, successorAddr, successorPort);
    }

    /*
     * Sends command message to specified host. Value can be null if not needed for command.
     */
    private void messageAny(String command, int key, String value, int[] visitedServers, InetAddress addr, int port) {
        Socket successorSocket = null;
        ObjectOutputStream outputStream = null;
        ObjectInputStream inputStream = null;

        try {
            successorSocket = new Socket(addr, port);
            outputStream = new ObjectOutputStream(successorSocket.getOutputStream());
            inputStream = new ObjectInputStream(successorSocket.getInputStream());

            outputStream.writeUTF(command);
            outputStream.writeInt(key);
            if (value != null) outputStream.writeUTF(value);
            outputStream.writeObject(visitedServers);
            outputStream.flush();
        } catch (IOException e) {
            if (addr == bootstrapServerAddr) {
                System.err.println("[ERROR] Problem occurred when forwarding request to bootstrap name server.");
            } else {
                System.err.println("[ERROR] Problem occurred when forwarding request to name server.");
            }
        } finally {
            try { if (outputStream != null) outputStream.close(); } catch (IOException e) { }
            try { if (inputStream != null) inputStream.close(); } catch (IOException e) { }
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
     * Appends this name server's ID to the end of the visited servers list.
     */
    private int[] appendVisitedID(int[] visitedServers) {
        int length = visitedServers.length;
        visitedServers = Arrays.copyOf(visitedServers, length + 1);
        visitedServers[length] = nameServerID;

        return visitedServers;
    }

    private String lookupKey(final int key, int[] visitedServers) {
        // Append ID to visitedServers
        visitedServers = appendVisitedID(visitedServers);
        String message = buildLogMessage("lookup", key, visitedServers);

        if (betweenRange(key, rangeStart, rangeEnd) && objects.containsKey(key)) {
            // Key found, message bootstrap server
            String value = objects.get(key);
            messageBootstrap("lookup_found", key, value, visitedServers);
            message += "Key found, messaging bootstrap server.";
        } else {
            // Forward message to successor
            forwardCommand("lookup", key, visitedServers);
            message += "Key not found, forwarding message to successor.";
        }

        return message;
    }

    private String insertValue(final int key, final String value, int[] visitedServers) {
        // Append ID to visitedServers
        visitedServers = appendVisitedID(visitedServers);
        String message = buildLogMessage("insert", key, value, visitedServers);

        if (betweenRange(key, rangeStart, rangeEnd)) {
            // Key should be inserted on this name server
            objects.put(key, value);
            messageBootstrap("insert_found", key, value, visitedServers);
            message += "Key is within this name server's range, inserting value.";
        } else {
            // Forward message to successor
            forwardCommand("insert", key, value, visitedServers);
            message += "Key is not within this name server's range, forwarding message to successor.";
        }

        return message;
    }

    private String deleteKey(final int key, int[] visitedServers) {
        // Append ID to visitedServers
        visitedServers = appendVisitedID(visitedServers);
        String message = buildLogMessage("insert", key, visitedServers);

        if (betweenRange(key, rangeStart, rangeEnd) && objects.containsKey(key)) {
            // Delete key off of this name server
            objects.remove(key);
            messageBootstrap("delete_found", key, visitedServers);
            message += "Key is within this name server's range, deleting key.";
        } else {
            // Forward message to successor
            forwardCommand("delete", key, visitedServers);
            message += "Key is not within this name server's range, forwarding message to successor.";
        }

        return message;
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

    private String buildEntrySuccessMessage(int[] visitedServers) {
        return "Successful entry.\n" +
                "Key Range: " + rangeStart + "-" + rangeEnd + "\n" +
                "Predecessor: " + predecessor + "\n" +
                "Successor: " + successor + "\n" +
                "Visited Servers: " + visitedToString(visitedServers);
    }

    private String enterComplete(ObjectInputStream inputStream) throws IOException, ClassNotFoundException {
        // New successor
        successor = inputStream.readInt();
        successorAddr = (InetAddress) inputStream.readObject();
        successorPort = inputStream.readInt();

        // New predecessor
        predecessor = inputStream.readInt();
        predecessorAddr = (InetAddress) inputStream.readObject();
        predecessorPort = inputStream.readInt();

        // New ranges
        rangeStart = predecessor + 1;
        rangeEnd = nameServerID;

        int[] visitedServers = (int[]) inputStream.readObject();

        // Initial key/value pairs
        objects.putAll((NavigableMap<Integer, String>) inputStream.readObject());

        return buildEntrySuccessMessage(visitedServers);
    }

    /*
     * Notifies new name server that they have been entered. Sends new name server's new
     * successor/predecessor info and transfers stored objects.
     */
    private void sendEnterComplete(int oldPredecessor, InetAddress oldPredecessorAddr, int oldPredecessorPort, int[] visitedServers) {
        Socket predecessorSocket = null;
        ObjectOutputStream outputStream = null;
        ObjectInputStream inputStream = null;

        try {
            predecessorSocket = new Socket(predecessorAddr, predecessorPort);
            outputStream = new ObjectOutputStream(predecessorSocket.getOutputStream());
            inputStream = new ObjectInputStream(predecessorSocket.getInputStream());

            outputStream.writeUTF("enter_complete");

            // New name server becomes predecessor to this name server
            outputStream.writeInt(nameServerID);
            outputStream.writeObject(nameServerAddr);
            outputStream.writeInt(nameServerPort);

            outputStream.writeInt(oldPredecessor);
            outputStream.writeObject(oldPredecessorAddr);
            outputStream.writeInt(oldPredecessorPort);

            outputStream.writeObject(visitedServers);

            moveStoredObjects(outputStream, oldPredecessor, predecessor);
        } catch (IOException e) {
            System.err.println("[ERROR] Problem occurred when notifying new predecessor name server.");
            e.printStackTrace();
        } finally {
            try { if (outputStream != null) outputStream.close(); } catch (IOException e) { }
            try { if (inputStream != null) outputStream.close(); } catch (IOException e) { }
            try { if (predecessorSocket != null) predecessorSocket.close(); } catch (IOException e) { }
        }
    }

    private void nameServerEnter(ObjectInputStream inputStream, ObjectOutputStream outputStream) {
        try {
            // Read new name server info
            int newID = inputStream.readInt();
            InetAddress newAddr = (InetAddress) inputStream.readObject();
            int newPort = inputStream.readInt();
            int[] visitedServers = (int[]) inputStream.readObject();

            // Add this name server's ID to the list
            visitedServers = appendVisitedID(visitedServers);

            // Check if ID is in use
            if ((newID == nameServerID || newID == successor)) {
                // Name server should print error and panic
                // TODO contact name server directly with "invalid_id"
                return;
            }

            if (betweenRange(newID, predecessor, nameServerID)) {
                // Update name server's predecessor
                int oldPredecessor = predecessor;
                InetAddress oldPredecessorAddr = predecessorAddr;
                int oldPredecessorPort = predecessorPort;

                predecessor = newID;
                predecessorAddr = newAddr;
                predecessorPort = newPort;

                sendEnterComplete(oldPredecessor, oldPredecessorAddr, oldPredecessorPort, visitedServers);

                // Update name server's key ranges
                // THIS SHOULD BE DONE AFTER sendEnterComplete()!!!
                // Key/Values must be transferred first!
                rangeStart = newID + 1;
                // rangeEnd always stays the same; rangeEnd == nameServerID

                outputStream.flush();
            } else {
                // Forwarding name server entry to successor

                // Name server that becomes the new node's successor will directly contact
                // the new node upon successful entry.
                forwardCommand("entry", newID, newAddr, newPort, visitedServers);
            }

        } catch (IOException e) {
            System.err.println("[ERROR] Connection problem occurred when adding new name server.");
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // Should not happen, exit program
            System.err.println("[ERROR] Input stream failure.");
            e.printStackTrace();
            System.exit(1);
        }

        // Streams and socket are closed by calling function
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
            } else if (command.equals("enter")) {
                nameServerEnter(inputStream, outputStream);
                message = null;
            } else if (command.equals("enter_complete")) {
                message = enterComplete(inputStream);
                messageNewPredecessor();
            } else if (command.equals("new_successor")) {
                this.successor = inputStream.readInt();
                this.successorAddr = (InetAddress) inputStream.readObject();
                this.successorPort = inputStream.readInt();
            } else if (command.equals("exit")) {
                // TODO
            } else {
                message = "Unknown command received from predecessor(Name Server " + predecessor + "): " + command + ".";
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
            Socket sock = null;
            ObjectInputStream inputStream = null;
            ObjectOutputStream outputStream = null;

            try {
                sock = incomingSocket.accept();
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

    private String buildLogMessage(String command, int key, int[] visitedServers) {
        return buildLogMessage(command, key, null, visitedServers);
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

    public void shutdown() {
        // TODO
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
        } finally {
            try { if (incomingSocket != null) incomingSocket.close(); } catch (IOException e) { }
        }
    }
}
