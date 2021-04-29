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

    private volatile boolean connected;
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

    private void immediateEntry(ObjectInputStream inputStream) throws IOException, ClassNotFoundException {
        boolean isFirstEntry = inputStream.readBoolean();
        if (isFirstEntry) {
            rangeStart = inputStream.readInt();
            rangeEnd = nameServerID;

            int bootstrapID = inputStream.readInt();

            successor = predecessor = bootstrapID;
            successorAddr = predecessorAddr = bootstrapServerAddr;
            successorPort = predecessorPort = bootstrapServerPort;
        } else {
            // Must contact predecessor and exchange info
            predecessorAddr = (InetAddress) inputStream.readObject();
            predecessorPort = inputStream.readInt();

            int bootstrapID = inputStream.readInt();

            successor = bootstrapID;
            successorAddr = bootstrapServerAddr;
            successorPort = bootstrapServerPort;
        }
    }

    public void enter() {
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
                immediateEntry(inputStream);
                objects.putAll((NavigableMap<Integer, String>) inputStream.readObject());
            }
        } catch (IOException e) {
            System.err.println("[ERROR] Problem occurred when forwarding request to bootstrap name server.");
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            System.err.println(e.getStackTrace());
            System.exit(1);
        } finally {
            try { if (outputStream != null) outputStream.close(); } catch (IOException e) { }
            try { if (inputStream != null) inputStream.close(); } catch (IOException e) { }
            try { if (bootstrapSocket != null) bootstrapSocket.close(); } catch (IOException e) { }
        }
    }

    public void exit() {

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
            outputStream.writeUnshared(visitedServers);
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
                // TODO
            } else if (command.equals("exit")) {
                // TODO
            } else {
                message = "Unknown command received from predecessor(Name Server " + predecessor + ").";
            }
        } catch (IOException e) {
            // Close streams & socket, let client crash
            message = "[ERROR] New connection i/o failed.";
            e.printStackTrace();
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

        return index >= rangeStart && index <= rangeEnd;
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
