package uk.ac.ucl.cs.sec.chainspace.bft;


// This is the class which sends requests to replicas
import bftsmart.communication.client.ReplyListener;
import bftsmart.reconfiguration.views.View;
import bftsmart.tom.AsynchServiceProxy;
import bftsmart.tom.RequestContext;
import bftsmart.tom.ServiceProxy;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import uk.ac.ucl.cs.sec.chainspace.SimpleLogger;

import java.math.BigInteger;
import java.util.Vector;
import java.nio.charset.Charset;

// Classes that need to be declared to implement this
// replicated Map
import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class MapClient implements Map<String, String> {

    //private AsynchServiceProxy clientProxyAsynch[] = new AsynchServiceProxy[2];
    private HashMap<Integer, AsynchServiceProxy> clientProxyAsynch = null; // Asynch client proxies indexed by shard IDs
    //private TOMMessage clientAsynchResponse[] = new TOMMessage[2]; // Responses to AsynchServiceProxies
    //private boolean sentShardsAsynch[] =  new boolean[2]; // Shards to which Asynch requests have been sent

    //private ServiceProxy clientProxy[] = new ServiceProxy[2];
    private HashMap<Integer, ServiceProxy> clientProxy = null; // Synch client proxies indexed by shard IDs

    public int defaultShardID; // Use this for testing for a single shard from a driver test program
    public int thisShard; // The shard with which this replica-client is associated
    public int thisReplica; // The replica with which this replica-client is associated
    String strLabel;

    // Shard configuration info
    private HashMap<Integer, String> shardToConfig = null; // Configurations indexed by shard ID
    private static int currClientID; // Each shard gets its own client with a unique ID
    private HashMap<Integer, Integer> shardToClient = null; // Client IDs indexed by shard ID
    private HashMap<Integer, Integer> shardToClientAsynch = null; // Asynch Client IDs indexed by shard ID

    private HashMap<String, Long> txSentTimes = new HashMap<String, Long>();
    private SimpleLogger latencylog = new SimpleLogger("latencylog");

    //public HashMap<String,Transaction> transactions = null; // Transactions indexed by Transaction ID

    // Asynch replies need to be saved somewhere until sufficient replies have been received or
    // the request times out, after which we do not need the replies any more and space can be released.
    public HashMap<String, HashSet<Integer>> asynchRepliesPreparedCommit = null; // indexed by (clientID,requestID,requestType)
                                                                          // concatenated into a string which will
                                                                          // be always unique (as per class TomSender),
                                                                          // Val is the shard IDs that have responded with
                                                                          // PREPARED_T_COMMIT so far.
    //public HashMap<String, HashSet<Integer>> asynchRepliesAcceptedCommit = null; // indexed by (clientID,requestID,requestType)
                                                                          // concatenated into a string which will
                                                                          // be always unique (as per class TomSender),
                                                                          // Val is the shard IDs that have responded with
                                                                          // ACCEPTED_T_COMMIT so far.

    HashMap<String, TransactionSequence> sequences; // Indexed by Transaction ID,
                                                    // Val is the state transition sequence for this transaction
    HashMap<String, HashSet<Integer>> targetShards; // Indexed by Transaction ID,
                                                    // Val is target shards corresponding to a transaction
    HashMap<String, Transaction> transactions; // Indexed by Transaction ID,
                                               // Val is the full transaction
                                              // Contains all transactions being processed

    public MapClient(String shardConfigFile, int thisShard, int thisReplica) {
        this.defaultShardID = thisShard;
        // These two are set just for logging purposes
        this.thisShard = thisShard;
        this.thisReplica = thisReplica;

        String strModule = "MapClient: ";
        strLabel = "[s" + thisShard + "n" + thisReplica + "] "; // This string is used in debug messages

        // Shards
        if (!initializeShards(shardConfigFile)) {
            logMsg(strLabel, strModule, "Could not read shard configuration file. Now exiting.");
            System.exit(0);
        }

        // Clients
        Random rand = new Random(System.currentTimeMillis());
        currClientID = Math.abs(rand.nextInt()); // Initializing with a random number because the program hangs
        // if there are two or more clients with the same ID

        initializeShardClients();

        sequences = new HashMap<>(); // contains operation sequences for transactions
        targetShards = new HashMap<>();
        transactions = new HashMap<>();
        asynchRepliesPreparedCommit = new HashMap<>();
        //asynchRepliesAcceptedCommit  = new HashMap<>();
    }

    public int mapObjectToShard(String object) {
        return BFTUtils.mapObjectToShard(object, shardToConfig.size());
    }

    // This function returns a unique client ID every time it is called
    private int getNextClientID() {
        if (++currClientID == Integer.MIN_VALUE)
            currClientID = 0;
        return currClientID;
    }


    private boolean initializeShards(String configFile) {
        // The format of configFile is <shardID> \t <pathToShardConfigFile>

        // Shard-to-Configuration Mapping
        shardToConfig = new HashMap<Integer, String>();
        String strModule = "initializeShards: ";

        try {
            BufferedReader lineReader = new BufferedReader(new FileReader(configFile));
            String line;
            int countLine = 0;
            int limit = 2; //Split a line into two tokens, the key and value

            while ((line = lineReader.readLine()) != null) {
                countLine++;
                String[] tokens = line.split("\\s+", limit);

                if (tokens.length == 2) {
                    int shardID = Integer.parseInt(tokens[0]);
                    String shardConfig = tokens[1];
                    shardToConfig.put(shardID, shardConfig);
                } else
                    logMsg(strLabel, strModule, "Skipping Line # " + countLine + " in config file: Insufficient tokens");
            }
            lineReader.close();
            return true;
        } catch (Exception e) {
            logMsg(strLabel, strModule, "There was an exception reading shard configuration file " + e.toString());
            return false;
        }

    }


    private void initializeShardClients() {
        String strModule = "initializeShardClients: ";
        // Clients IDs indexed by shard IDs
        shardToClient = new HashMap<Integer, Integer>();
        shardToClientAsynch = new HashMap<Integer, Integer>();

        // Client objects indexed by shard IDs
        clientProxyAsynch = new HashMap<Integer, AsynchServiceProxy>();
        clientProxy = new HashMap<Integer, ServiceProxy>();

        // Each shard has a synch and asynch client, with different client IDs
        for (int shardID : shardToConfig.keySet()) {
            String config = shardToConfig.get(shardID);
            logMsg(strLabel, strModule, "Shard " + shardID + "Config " + config);

            // Synch client proxy
            int clientID = this.getNextClientID();
            shardToClient.put(shardID, clientID);
            ServiceProxy sp = new ServiceProxy(clientID, config);

            logMsg(strLabel, strModule, "NEW port of client 0 in shard " + shardID + " is  " + sp.getViewManager().getStaticConf().getPort(0));
            clientProxy.put(shardID, sp);
            //View v = new View(0, getStaticConf().getInitialView(), getStaticConf().getF(), getInitAdddresses());
            //sp.getViewManager().reconfigureTo(sp.getViewManager().getStaticConf().getInitialView());
            logMsg(strLabel, strModule, "Created new client proxy ID " + clientID + " for shard " + shardID + " with config " + config);
            logMsg(strLabel, strModule, "The view of client " + clientID + "for shard " + shardID + " is: " + sp.getViewManager().getCurrentView().toString());

            // Asynch client proxy
            int clientIDAsynch = this.getNextClientID();
            shardToClientAsynch.put(shardID, clientIDAsynch);
            AsynchServiceProxy asp = new AsynchServiceProxy(clientIDAsynch, config);
            clientProxyAsynch.put(shardID, asp);
            logMsg(strLabel, strModule, "Created new ASYNCH client proxy ID " + clientIDAsynch + " for shard " + shardID + " with config " + config);
            logMsg(strLabel, strModule, "The view of client " + clientID + "for shard " + shardID + " is: " + asp.getViewManager().getCurrentView().toString());

        }
    }

    private String getKeyAsynchReplies(int a, int b, String c) {
        return a + ";" + b + ";" + c;
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean containsKey(Object key) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Set<String> keySet() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Collection<String> values() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String put(String key, String value) {
        String strModule = "PUT (DRIVER)";
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(out);

            oos.writeInt(RequestType.PUT);
            oos.writeUTF(key);
            oos.writeUTF(value);
            oos.close();
            logMsg(strLabel, strModule, "Putting a key-value pair in shard ID " + defaultShardID);
            byte[] reply = clientProxy.get(defaultShardID).invokeOrdered(out.toByteArray());
            if (reply != null) {
                String previousValue = new String(reply);
                return previousValue;
            }
            return null;
        } catch (IOException ioe) {
            logMsg(strLabel, strModule, "Exception putting value into table " + ioe.getMessage());
            return null;
        }
    }

    @Override
    public String get(Object key) {
        String strModule = "GET (DRIVER)";
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(out);
            oos.writeInt(RequestType.GET);
            oos.writeUTF(String.valueOf(key));
            oos.close();
            byte[] reply = clientProxy.get(defaultShardID).invokeUnordered(out.toByteArray());
            String value = new String(reply);
            return value;
        } catch (IOException ioe) {
            logMsg(strLabel, strModule, "Exception getting value from table " + ioe.getMessage());
            return null;
        }
    }

    @Override
    public String remove(Object key) {
        String strModule = "REMOVE (DRIVER)";
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(out);
            oos.writeInt(RequestType.REMOVE);
            oos.writeUTF(String.valueOf(key));
            oos.close();
            byte[] reply = clientProxy.get(defaultShardID).invokeOrdered(out.toByteArray());
            if (reply != null) {
                String removedValue = new String(reply);
                return removedValue;
            }
            return null;
        } catch (IOException ioe) {
            logMsg(strLabel, strModule, "Exception removing value from table " + ioe.getMessage());
            return null;
        }
    }

    @Override
    public int size() {
        String strModule = "SIZE (DRIVER)";
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(out);
            oos.writeInt(RequestType.SIZE);
            oos.close();
            byte[] reply = clientProxy.get(defaultShardID).invokeUnordered(out.toByteArray());
            ByteArrayInputStream in = new ByteArrayInputStream(reply);
            DataInputStream dis = new DataInputStream(in);
            int size = dis.readInt();
            return size;
        } catch (IOException ioe) {
            logMsg(strLabel, strModule, "Exception getting the size of table " + ioe.getMessage());
            return -1;
        }
    }

    // Tells the shards to read objects from a file. Each shard will read its own object file
    // File path has been hardcoded to: "ChainSpaceConfig/test_objects"+thisShard+".txt";
    public void loadObjectsFromFile(int shardID) {
        String strModule = "loadTransactionsFromFile (DRIVER)";
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(out);

            oos.writeInt(RequestType.LOAD_TEST_OBJECTS_FROM_FILE);
            oos.close();
            logMsg(strLabel, strModule, "Sending request to shard "+shardID+" to load objects from file");
            byte[] reply = clientProxy.get(shardID).invokeUnordered(out.toByteArray());
            if (reply != null) {
                logMsg(strLabel, strModule, "Server reply is: "+new String(reply));
            }
            else
                logMsg(strLabel, strModule, "Null reply");

        } catch (IOException ioe) {
            logMsg(strLabel, strModule, "Exception loading objects from file" + ioe.getMessage());
        }
    }

    public void loadObjectsFromFile() {
        loadObjectsFromFile(defaultShardID);
    }

    public void loadObjectsFromFileAllShards() {
        for (int shardID : shardToConfig.keySet()) {
            loadObjectsFromFile(shardID);
        }
    }

    public Boolean sendTransactionsFromFile(String fileID) {
        return sendTransactionsFromFile(fileID, "ChainSpaceClientConfig", 1000, 0);
    }

    public Boolean sendTransactionsFromFile(String fileID, int batchSize, int batchSleep) {
        return sendTransactionsFromFile(fileID, "ChainSpaceClientConfig", batchSize, batchSleep);
    }

    // This is a client side function, for submitting transactions read from a file
    public Boolean sendTransactionsFromFile(String fileID, String dir, int batchSize, int batchSleep) {
        String fileTransactions = dir + "/test_transactions"+fileID+".txt";

        String strModule = "sendTransactionsFromFile: ";
        String strLabel = "";
        logMsg(strLabel,strModule,"Reading transactions");

        int sent = 0;

        try {
            BufferedReader lineReader = new BufferedReader(new FileReader(fileTransactions));
            String line;
            int countLine = 0;
            int limit = 3; //Split a line into three tokens delimited by spaces:
            // (1) transaction ID, (2) inputs (delimited by ";"), (3) outputs (delimited by ";")

            while ((line = lineReader.readLine()) != null) {
                countLine++;
                String[] tokens = line.split("\\s+",limit);

                Transaction t = new Transaction();

                if(tokens.length == 3) { //Split a line into three tokens delimited by spaces:
                    // (1) transaction ID
                    // (2) input objects (delimited by ";")
                    // (3) output objects (delimited by ";")
                    String transactionID = tokens[0];
                    String inputs = tokens[1];
                    String outputs = tokens[2];

                    logMsg(strLabel,strModule,"Read this line from thefile: "+line);

                    // Read transaction ID
                    t.id = transactionID;
                    logMsg(strLabel,strModule,"Transaction ID is: "+transactionID);

                    // Read transaction inputs
                    String[] tokens_inputs = inputs.split(";");
                    for(String eachInput: tokens_inputs) {
                        t.inputs.add(eachInput);
                        logMsg(strLabel,strModule,"Input is: : "+eachInput);
                    }

                    // Read transaction outputs
                    String[] tokens_outputs = outputs.split(";");
                    for(String eachOutput: tokens_outputs) {
                        t.outputs.add(eachOutput);
                        logMsg(strLabel,strModule,"Output is: : "+eachOutput);
                    }

                    // Submit transaction
                    submitTransaction(t);
                    sent++;
                    txSentTimes.put(t.id, System.currentTimeMillis());

                    if (sent % batchSize == 0) {
                        TimeUnit.SECONDS.sleep(batchSleep);
                    }
                }
                else
                    logMsg(strLabel,strModule,"Skipping Line # "+countLine+" in file: Insufficient tokens");
            }
            lineReader.close();
            return true;
        } catch (Exception e) {
            logMsg(strLabel,strModule,"There was an exception reading configuration file "+ e.toString());
            return false;
        }

    }

    public void createObjects(List<String> outputObjects) {
        TOMMessageType reqType = TOMMessageType.ORDERED_REQUEST; // CREATE_OBJECT messages require BFT consensus, so type is ordered

        String strModule = "CREATE_OBJECT (DRIVER): ";

        try {
            HashMap<Integer, ArrayList<String>> shardToObjects = new HashMap<>(); // Output objects managed by a shard

            // Group objects by the managing shard
            for (String output : outputObjects) {
                int shardID = mapObjectToShard(output);

                logMsg(strLabel, strModule, "Mapped object " + output + " to shard " + shardID);

                if (shardID == -1) {
                    logMsg(strLabel, strModule, "Cannot map output " + output + " to a shard. Will not create object.");
                } else {
                    if (!shardToObjects.containsKey(shardID)) {
                        shardToObjects.put(shardID, new ArrayList<String>());
                    }
                    shardToObjects.get(shardID).add(output);
                }
            }

            // Send a request to each shard relevant to the outputs
            for (int shardID : shardToObjects.keySet()) {
                ByteArrayOutputStream bs = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bs);
                oos.writeInt(RequestType.CREATE_OBJECT);
                oos.writeObject(shardToObjects.get(shardID));
                oos.close();


                logMsg(strLabel, strModule, "Sending CREATE_OBJECT to shard " + shardID);
                int req = clientProxyAsynch.get(shardID).invokeAsynchRequest(bs.toByteArray(), new ReplyListener() {
                    @Override
                    public void replyReceived(RequestContext context, TOMMessage reply) {
                    }
                }, reqType);

                logMsg(strLabel, strModule, "Sent a request to shard ID " + shardID);
            }
        } catch (Exception e) {
            logMsg(strLabel, strModule, "Experienced Exception " + e.getMessage());
        }
    }


    // The BFT initiator uses this function to inform other replicas about the
    // decision of a BFT round.
    // TODO: The message should include proof (e.g., bundle of signatures) that
    // TODO: other replicas agree on this decision
    public void broadcastBFTDecision(int msgType, Transaction t, int shardID) {
        //TOMMessageType reqType = TOMMessageType.UNORDERED_REQUEST;
        TOMMessageType reqType = TOMMessageType.UNORDERED_REQUEST;
        String strModule = "broadcastBFTDecision (DRIVER): ";
        try {
            ByteArrayOutputStream bs = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bs);
            oos.writeInt(msgType);
            oos.writeObject(t);
            oos.close();

            logMsg(strLabel, strModule, "Broadcasting " + RequestType.getReqName(msgType) + " to shard " + shardID +
                    " for transaction " + t.id);
            /*
            int req = clientProxyAsynch.get(shardID).invokeAsynchRequest(bs.toByteArray(), new ReplyListener() {
                @Override
                public void replyReceived(RequestContext context, TOMMessage reply) { }
            }, reqType); */
            byte[] reply = clientProxy.get(shardID).invokeUnordered(bs.toByteArray());
        } catch (Exception e) {
            logMsg(strLabel, strModule, "Experienced Exception " + e.getMessage());
        }
    }


    public void submitTransaction(Transaction t) {
        String strModule = "SUBMIT_T (DRIVER)";
        List<String> inputObjects = t.inputs;

        // Identify target shards for this transaction
        for (String input : inputObjects) {
            int shardID = mapObjectToShard(input);

            if (shardID == -1) {
                logMsg(strLabel, strModule, "Cannot map input " + input + " in transaction ID " + t.id + " to a shard.");
                return;
            }

            if (!targetShards.containsKey(t.id)) {
                HashSet newSet = new HashSet<Integer>();
                targetShards.put(t.id, newSet);
            }
            targetShards.get(t.id).add(shardID);
        }

        // print out target shards
        logMsg(strLabel, strModule, "Target shards for transaction ID " + t.id + "are as follows:");
        for (Integer shardID : targetShards.get(t.id)) {
            System.out.print(shardID+"; ");
        }
        System.out.println("");

        // Create fresh sequence for this transaction
        sequences.put(t.id, new TransactionSequence());

        // Add this transaction to those currently being processed
        transactions.put(t.id, t);

        // Send PREPARE_T message to the relevant shards for this transaction
        sequences.get(t.id).PREPARE_T = true; // Update sequence
        prepare_t(t);
    }

    // ===============
    // PREPARE_T BLOCK BEGINS
    // ================

    public void prepare_t(Transaction t) {
        TOMMessageType reqType = TOMMessageType.ORDERED_REQUEST; // PREPARE_T messages require BFT consensus, so type is ordered
        String transactionID = t.id;
        String strModule = "PREPARE_T (DRIVER)";

        try {
            // Initialize the replies map, to collect shard replies for this transaction
            HashSet newSet = new HashSet<Integer>(); // empty shard set
            asynchRepliesPreparedCommit.put(t.id, newSet); // No shards have replied yet with PREPARED_T_COMMIT
            sequences.get(transactionID).PREPARE_T = true; // Update transaction sequence
            // Send a request to each shard relevant to this transaction
            for ( int shardID : targetShards.get(transactionID) ) {
                ByteArrayOutputStream bs = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bs);
                oos.writeInt(RequestType.PREPARE_T);
                oos.writeObject(t);
                oos.close();
                // PREPARE_T BFT rounds done asynchronously over all relevant shards
                //logMsg(strLabel, strModule, "Sending " + RequestType.getReqName(RequestType.PREPARE_T) +
                //            " to shard " + shardID + " for transaction " + t.id);
                int req = clientProxyAsynch.get(shardID).invokeAsynchRequest(bs.toByteArray(), new ReplyListenerAsynchQuorumPrepareT(shardID, transactionID), reqType);
                logMsg(strLabel, strModule, "Sent " + RequestType.getReqName(RequestType.PREPARE_T) + "with request ID "+ req + " to shard ID " + shardID);
                                        // Note: the req ID is unique per clientProxyAsynch
            }
        } catch (Exception e) {
            logMsg(strLabel, strModule, "Transaction ID " + transactionID + " experienced Exception " + e.getMessage());
        }
    }

    // This class is used to track responses from replicas of a shard to PREPARE_T messages
    //
    private class ReplyListenerAsynchQuorumPrepareT implements ReplyListener {
        AsynchServiceProxy client;
        private int shardID;
        private String transactionID;
        private int replyQuorum; // size of the reply quorum
        private TOMMessage replies[];
        private int receivedReplies; // Number of received replies
        private String strModule;


        private Comparator<byte[]> comparator = new Comparator<byte[]>() {
            @Override
            public int compare(byte[] o1, byte[] o2) {
                return Arrays.equals(o1, o2) ? 0 : -1;
            }
        };

        private Extractor extractor = new Extractor() {
            @Override
            public TOMMessage extractResponse(TOMMessage[] replies, int sameContent, int lastReceived) {
                return replies[lastReceived];
            }
        };

        private ReplyListenerAsynchQuorumPrepareT(int shardID, String transactionID) {
            this.shardID = shardID;
            this.transactionID = transactionID;
            replyQuorum = getReplyQuorum(shardID);
            client = clientProxyAsynch.get(shardID);
            replies = new TOMMessage[client.getViewManager().getCurrentViewN()];
            receivedReplies = 0;
            strModule = "ReplyListenerAsynchQuorumPrepareT: ";
        }


        @Override
        public void replyReceived(RequestContext context, TOMMessage reply) {
            //System.out.println("New reply received by client ID "+client.getProcessId()+" from  "+reply.getSender());
            StringBuilder builder = new StringBuilder();
            builder.append("[RequestContext] id: " + context.getReqId() + " type: " + context.getRequestType());
            builder.append("[TOMMessage reply] sender id: " + reply.getSender() + " Hash content: " + Arrays.toString(reply.getContent()));
            //System.out.println("ACCEPT_T: New reply received from shard ID"+shardID+": "+builder.toString());

            // When to give reply to the application layer

            int pos = client.getViewManager().getCurrentViewPos(reply.getSender());

            if (pos >= 0) { //only consider messages from replicas

                int sameContent = 1;

                if (replies[pos] == null) {
                    receivedReplies++;
                }
                replies[pos] = reply;

                // Compare the reply just received, to the others
                for (int i = 0; i < replies.length; i++) {

                    if ((i != pos || client.getViewManager().getCurrentViewN() == 1) && replies[i] != null
                            && (comparator.compare(replies[i].getContent(), reply.getContent()) == 0)) {
                        sameContent++;
                        // Shard reply received (quorum satisfied)
                        if (sameContent >= replyQuorum) {
                            //String key = shardToClientAsynch.get(shardID) + ";" + context.getReqId() + ";" + context.getRequestType();
                            TOMMessage m = extractor.extractResponse(replies, sameContent, pos);

                            if (m == null) {
                                // Error handling
                                logMsg(strLabel, strModule, "Transaction ID " + this.transactionID + "received a null response from shard "+this.shardID);
                            } else {
                                byte[] shardResponse = m.getContent();
                                String strShardResponse = new String(shardResponse, Charset.forName("UTF-8"));

                                logMsg(strLabel, strModule, "Shard ID " + shardID + " replied: " + strShardResponse);

                                if ( strShardResponse.equals(ResponseType.PREPARED_T_COMMIT) ) {
                                    asynchRepliesPreparedCommit.get(transactionID).add(shardID);
                                    // Check if all the shards have replied with PREPARED_T_COMMIT
                                    if( asynchRepliesPreparedCommit.get(transactionID).size() == targetShards.get(transactionID).size() ) {
                                        logMsg(strLabel, strModule, "Transaction ID " + transactionID + "has been locally committed (PREPARED_T_COMMITTED)");
                                        sequences.get(transactionID).PREPARED_T_COMMIT = true; // Update transaction sequence
                                        asynchRepliesPreparedCommit.remove(transactionID); // no longer waiting for any replies

                                        // >>>>> Send ACCEPT_T_COMMIT to relevant shards asynchronously (this will spawn new threads; see accept_t()) <<<<<<<
                                        accept_t(transactions.get(this.transactionID), RequestType.ACCEPT_T_COMMIT);
                                        // >>>>>>>>>>>>>><<<<<<<<<<<<<
                                    }
                                }
                                else if (strShardResponse.equals(ResponseType.PREPARED_T_ABORT)
                                        && !sequences.get(transactionID).PREPARED_T_ABORT) { // This is the first PREPARED_T_ABORT
                                    // Unlike PREPARED_T_COMMIT (see above) here we don't wait for responses from all shards.
                                    // PREPARED_T_ABORT from any single shard suffices to emit ACCEPT_T_ABORT

                                    logMsg(strLabel, strModule, "Transaction ID " + transactionID + "has been locally aborted (PREPARED_T_ABORTED) by shard "+this.shardID);
                                    sequences.get(transactionID).PREPARED_T_ABORT = true; // Update transaction sequence

                                    //asynchRepliesPreparedCommit.remove(transactionID); // no longer waiting for any replies

                                    // >>>>> Send ACCEPT_T_ABORT to relevant shards asynchronously (this will spawn new threads; see accept_t()) <<<<<<<
                                    accept_t(transactions.get(this.transactionID), RequestType.ACCEPT_T_ABORT);
                                    // >>>>>>>>>>>>>><<<<<<<<<<<<<
                                }
                            }
                            logMsg(strLabel, strModule, "[RequestContext] clean request to shard ID " + shardID + " with context id " + context.getReqId());
                            client.cleanAsynchRequest(context.getReqId());
                        }
                    }
                }
            }

        }
    }

    // ===============
    // PREPARE_T BLOCK ENDS
    // ================

    // ===============
    // ACCEPT_T BLOCK BEGINS
    // ================

    public void accept_t(Transaction t, int msgType) {
        TOMMessageType reqType = TOMMessageType.ORDERED_REQUEST; // PREPARE_T messages require BFT consensus, so type is ordered
        String transactionID = t.id;
        String strModule = "ACCEPT_T (DRIVER)";

        try {
            // Initialize the replies map, to collect shard replies for this transaction
            HashSet newSet = new HashSet<Integer>(); // empty shard set
            //asynchRepliesAcceptedCommit.put(t.id, newSet); // No shards have replied yet with PREPARED_T_COMMIT
            // Update transaction sequence
            if( msgType == RequestType.ACCEPT_T_COMMIT )
                sequences.get(transactionID).ACCEPT_T_COMMIT = true;
            else
                sequences.get(transactionID).ACCEPT_T_ABORT = true;

            // Send a request to each shard relevant to this transaction
            for ( int shardID : targetShards.get(transactionID) ) {
                ByteArrayOutputStream bs = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bs);
                oos.writeInt(msgType);
                oos.writeObject(t);
                oos.close();
                // ACCEPT_T BFT rounds done asynchronously over all relevant shards
                //logMsg(strLabel, strModule, "Sending " + RequestType.getReqName(msgType) +
                //        " to shard " + shardID + " for transaction " + t.id);
                int req = clientProxyAsynch.get(shardID).invokeAsynchRequest(bs.toByteArray(), new ReplyListenerAsynchQuorumAcceptT(shardID, transactionID), reqType);
                logMsg(strLabel, strModule, "Sent " + RequestType.getReqName(msgType) + "with request ID "+ req + " to shard ID " + shardID);
                // Note: the req ID is unique per clientProxyAsynch
            }
        } catch (Exception e) {
            logMsg(strLabel, strModule, "Transaction ID " + transactionID + " experienced Exception " + e.getMessage());
        }
    }


    // This class is used to track responses from replicas of a shard to ACCEPT_T messages
    //
    private class ReplyListenerAsynchQuorumAcceptT implements ReplyListener {
        AsynchServiceProxy client;
        private int shardID;
        private String transactionID;
        private int replyQuorum; // size of the reply quorum
        private TOMMessage replies[];
        private int receivedReplies; // Number of received replies
        private String strModule;


        private Comparator<byte[]> comparator = new Comparator<byte[]>() {
            @Override
            public int compare(byte[] o1, byte[] o2) {
                return Arrays.equals(o1, o2) ? 0 : -1;
            }
        };

        private Extractor extractor = new Extractor() {
            @Override
            public TOMMessage extractResponse(TOMMessage[] replies, int sameContent, int lastReceived) {
                return replies[lastReceived];
            }
        };

        private ReplyListenerAsynchQuorumAcceptT(int shardID, String transactionID) {
            this.shardID = shardID;
            this.transactionID = transactionID;
            replyQuorum = getReplyQuorum(shardID);
            client = clientProxyAsynch.get(shardID);
            replies = new TOMMessage[client.getViewManager().getCurrentViewN()];
            receivedReplies = 0;
            strModule = "ReplyListenerAsynchQuorumAcceptT: ";
        }


        @Override
        public void replyReceived(RequestContext context, TOMMessage reply) {

            // When to give reply to the application layer

            int pos = client.getViewManager().getCurrentViewPos(reply.getSender());

            if (pos >= 0) { //only consider messages from replicas

                int sameContent = 1;

                if (replies[pos] == null) {
                    receivedReplies++;
                }
                replies[pos] = reply;

                // Compare the reply just received, to the others
                for (int i = 0; i < replies.length; i++) {

                    if ((i != pos || client.getViewManager().getCurrentViewN() == 1) && replies[i] != null
                            && (comparator.compare(replies[i].getContent(), reply.getContent()) == 0)) {
                        sameContent++;
                        // Shard reply received (quorum satisfied)
                        if (sameContent >= replyQuorum) {
                            //String key = shardToClientAsynch.get(shardID) + ";" + context.getReqId() + ";" + context.getRequestType();
                            TOMMessage m = extractor.extractResponse(replies, sameContent, pos);

                            if (m == null) {
                                // Error handling
                                logMsg(strLabel, strModule, "Transaction ID " + this.transactionID + "received a null response from shard "+this.shardID);
                            } else {
                                byte[] shardResponse = m.getContent();
                                String strShardResponse = new String(shardResponse, Charset.forName("UTF-8"));

                                logMsg(strLabel, strModule, "Shard ID " + shardID + " replied: " + strShardResponse);

                                // For ACCEPT_T_* responses, any single shard's response suffices to move to the next step
                                if(!sequences.get(transactionID).ACCEPTED_T_ABORT && !sequences.get(transactionID).ACCEPTED_T_COMMIT)
                                                                                 // This is the first ACCEPTED_T_* msg from any shard

                                {
                                    if (strShardResponse.equals(ResponseType.ACCEPTED_T_COMMIT) ) {
                                        latencylog.justLog(Long.toString(System.currentTimeMillis() - txSentTimes.get(this.transactionID)));
                                        logMsg(strLabel, strModule, "Transaction ID " + transactionID + "has been committed (ACCEPTED_T_COMMITTED)");
                                        sequences.get(transactionID).ACCEPTED_T_COMMIT = true; // Update transaction sequence
                                        //asynchRepliesAcceptedCommit.remove(transactionID); // no longer waiting for any replies

                                        // >>>>> Send CREATE_OBJECT to relevant shards asynchronously
                                        // (this will spawn new threads; see createObjects()) <<<<<<<
                                        if( transactions.get(this.transactionID).outputs.size() > 0 )
                                            createObjects(transactions.get(this.transactionID).outputs);
                                        // >>>>>>>>>>>>><<<<<<<<<<<<<

                                    } else if (strShardResponse.equals(ResponseType.ACCEPTED_T_ABORT) ) {
                                        // cleanup
                                        logMsg(strLabel, strModule, "Transaction ID " + transactionID + "has been aborted (ACCEPTED_T_ABORT)");
                                        sequences.get(transactionID).ACCEPTED_T_ABORT = true; // Update transaction sequence
                                        //asynchRepliesAcceptedCommit.remove(transactionID); // no longer waiting for any replies
                                    }
                                }
                            }
                            logMsg(strLabel, strModule, "[RequestContext] clean request to shard ID " + shardID + " with context id " + context.getReqId());
                            client.cleanAsynchRequest(context.getReqId());
                        }
                    }
                }
            }

        }
    }

    // ===============
    // ACCEPT_T BLOCK ENDS
    // ================

    /*

    public String accept_t_commit(Transaction t) {
        //saveTransaction(t);
        return accept_t(t, RequestType.ACCEPT_T_COMMIT);
    }


    public String accept_t_abort(Transaction t) {
        //saveTransaction(t);
        return accept_t(t, RequestType.ACCEPT_T_ABORT);
    }

    public String accept_t(Transaction t, int msgType) {
        Set<Integer> targetShards = new HashSet<Integer>();
        ; // The shards relevant to this transaction
        HashMap<Integer, Integer> shardToReq = new HashMap<Integer, Integer>();
        ; // Request IDs indexed by shard IDs
        TOMMessageType reqType = TOMMessageType.ORDERED_REQUEST; // ACCEPT_T messages require BFT consensus, so type is ordered
        boolean earlyTerminate = false;
        String finalResponse = null;
        String transactionID = t.id;
        String strModule = "ACCEPT_T (DRIVER)";

        try {
            List<String> inputObjects = t.inputs;

            // Send a request to each shard relevant to this transaction
            for (String input : inputObjects) {
                int shardID = mapObjectToShard(input);

                if (shardID == -1) {
                    logMsg(strLabel, strModule, "Cannot map input " + input + " in transaction ID " + transactionID + " to a shard.");
                    finalResponse = ResponseType.ACCEPT_T_SYSTEM_ERROR;
                    earlyTerminate = true;
                    break;
                }

                if (!targetShards.contains(shardID)) {
                    targetShards.add(shardID);
                    ByteArrayOutputStream bs = new ByteArrayOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(bs);
                    oos.writeInt(msgType);
                    oos.writeObject(t);
                    oos.close();
                    // ACCEPT_T BFT rounds done asynchronously over all relevant shards
                    logMsg(strLabel, strModule, "Sending " + RequestType.getReqName(msgType) +
                            " to shard " + defaultShardID + " for transaction " + t.id);
                    int req = clientProxyAsynch.get(shardID).invokeAsynchRequest(bs.toByteArray(), new ReplyListenerAsynchQuorum(shardID), reqType);
                    logMsg(strLabel, strModule, "Sent " + RequestType.getReqName(msgType) + ") to shard ID " + shardID);
                    shardToReq.put(shardID, req); // Bano: This assumes only one req per shard, but there could be multiple,
                    //       need to change the index - maybe the index could be shardID+transactionID
                    //       because there will be only one request for a shard+transaction pair
                }
            }

            if (!earlyTerminate) {

                // all timeout values in milliseconds
                // FIXME: Choose suitable timeout values
                int minWait = 1; // First wait will be minWait long
                int timeoutIncrement = 2; // subsequent wait will proceed in timeoutIncrement until all shards reply
                int maxWait = 10000; // time out if waitedSoFar exceeds maxWait

                boolean firstAttempt = true;
                int waitedSoFar = 0;


                // Wait until we have waited for more than maxWait
                while (waitedSoFar < maxWait) {

                    //logMsg(strLabel,strModule,"Checking shard replies; been waiting for " + waitedSoFar);

                    boolean abortShardReplies = false; // at least 1 shard replied abort
                    boolean missingShardReplies = false; // at least 1 shard reply is missing (null)

                    // Check responses from all shards in asynchReplies
                    for (int shard : targetShards) {

                        // Get a shard reply
                        int client = shardToClientAsynch.containsKey(shard) ? shardToClientAsynch.get(shard) : -1;
                        int req = shardToReq.containsKey(shard) ? shardToReq.get(shard) : -1;
                        String key = getKeyAsynchReplies(client, req, reqType.toString());
                        TOMMessage m = asynchReplies.get(key);

                        if (m == null) {
                            missingShardReplies = true;
                            break; // A shard hasn't replied yet, we need to wait more
                        } else {
                            byte[] reply = m.getContent();
                            String strReply = new String(reply, Charset.forName("UTF-8"));

                            logMsg(strLabel, strModule, "Shard ID " + shard + " replied: " + strReply);

                            if (strReply.equals(ResponseType.ACCEPTED_T_ABORT))
                                abortShardReplies = true;
                        }
                    }

                    if (!missingShardReplies) {
                        if (!abortShardReplies) // Commit if all shards have replied and their reply is to commit
                            finalResponse = ResponseType.ACCEPTED_T_COMMIT;
                        else
                            finalResponse = ResponseType.ACCEPTED_T_ABORT;
                        logMsg(strLabel, strModule, "All shards replied; final response is " + finalResponse);
                        break;
                    }

                    if (firstAttempt) {
                        Thread.sleep(minWait);
                        waitedSoFar += minWait;
                        firstAttempt = false;
                    } else {
                        Thread.sleep(timeoutIncrement);
                        waitedSoFar += timeoutIncrement;
                    }

                    if (waitedSoFar > maxWait) // We are about to exit this loop and haven't yet heard from all shards
                    {
                        logMsg(strLabel, strModule, "Timed out waiting for all shard replies. ABORT.");
                        finalResponse = ResponseType.ACCEPTED_T_ABORT;
                    }
                }
            }
        } catch (Exception e) {
            logMsg(strLabel, strModule, "Transaction ID " + transactionID + " experienced Exception " + e.getMessage());
            finalResponse = ResponseType.ACCEPT_T_SYSTEM_ERROR;
        } finally {
            // Clean up
            for (int shard : targetShards) {
                int client = shardToClientAsynch.containsKey(shard) ? shardToClientAsynch.get(shard) : -1;
                int req = shardToReq.containsKey(shard) ? shardToReq.get(shard) : -1;
                String key = getKeyAsynchReplies(client, req, reqType.toString());
                asynchReplies.remove(key);
            }

            return finalResponse;
        }
    }


    // This class is used to track responses from replicas of a shard to ACCEPT_T messages
    //
    private class ReplyListenerAsynchQuorum implements ReplyListener {
        AsynchServiceProxy client;
        private int shardID;
        private int replyQuorum; // size of the reply quorum
        private TOMMessage replies[];
        private int receivedReplies; // Number of received replies
        private String strModule;


        private Comparator<byte[]> comparator = new Comparator<byte[]>() {
            @Override
            public int compare(byte[] o1, byte[] o2) {
                return Arrays.equals(o1, o2) ? 0 : -1;
            }
        };

        private Extractor extractor = new Extractor() {
            @Override
            public TOMMessage extractResponse(TOMMessage[] replies, int sameContent, int lastReceived) {
                return replies[lastReceived];
            }
        };

        private ReplyListenerAsynchQuorum(int shardID) {
            this.shardID = shardID;
            replyQuorum = getReplyQuorum(shardID);
            client = clientProxyAsynch.get(shardID);
            replies = new TOMMessage[client.getViewManager().getCurrentViewN()];
            receivedReplies = 0;
            strModule = "ReplyListenerAsynchQuorum: ";
        }


        @Override
        public void replyReceived(RequestContext context, TOMMessage reply) {
            //System.out.println("New reply received by client ID "+client.getProcessId()+" from  "+reply.getSender());
            StringBuilder builder = new StringBuilder();
            builder.append("[RequestContext] id: " + context.getReqId() + " type: " + context.getRequestType());
            builder.append("[TOMMessage reply] sender id: " + reply.getSender() + " Hash content: " + Arrays.toString(reply.getContent()));
            //System.out.println("ACCEPT_T: New reply received from shard ID"+shardID+": "+builder.toString());

            // When to give reply to the application layer

            int pos = client.getViewManager().getCurrentViewPos(reply.getSender());

            if (pos >= 0) { //only consider messages from replicas

                int sameContent = 1;

                if (replies[pos] == null) {
                    receivedReplies++;
                }
                replies[pos] = reply;

                // Compare the reply just received, to the others
                for (int i = 0; i < replies.length; i++) {

                    if ((i != pos || client.getViewManager().getCurrentViewN() == 1) && replies[i] != null
                            && (comparator.compare(replies[i].getContent(), reply.getContent()) == 0)) {
                        sameContent++;
                        if (sameContent >= replyQuorum) {
                            String key = shardToClientAsynch.get(shardID) + ";" + context.getReqId() + ";" + context.getRequestType();
                            TOMMessage shardResponse = extractor.extractResponse(replies, sameContent, pos);
                            asynchReplies.put(key, shardResponse);
                            //
                            //try {
                            //    System.out.println("ACCEPT_T [replyReceived]: Final reply from shard ID" + shardID + ": " + new String(reply.getContent(), "UTF-8"));
                            //}
                            //catch(Exception e) {
                            //    System.out.println("ACCEPT_T [replyReceived]: Exception in printing final reply of shard ID "+shardID);
                            //}
                            // Bano: For accepted_* msgs, here on receiving each message we should check if the criteria
                            //       for considering the transaction to be committed is satisfied, in which case we shoul
                            //       create any objects, and update a global data structure. The client can regularly check
                            //       this data structure to find out which transactions have been committed
                            // Bano: For prepared_* msgs, here on receiving each message we should check if the criteria
                            //       for sending accept_* msg to shards is satisfied

                            //System.out.println("ACCEPT_T: [RequestContext] clean request context id: " + context.getReqId());
                            client.cleanAsynchRequest(context.getReqId());
                        }
                    }
                }
            }
        }
    }
    */

    private int getReplyQuorum(int shardID) {
        AsynchServiceProxy c = clientProxyAsynch.get(shardID);

        if (c.getViewManager().getStaticConf().isBFT()) {
            return (int) Math.ceil((c.getViewManager().getCurrentViewN()
                    + c.getViewManager().getCurrentViewF()) / 2) + 1;
        } else {
            //return (int) Math.ceil((c.getViewManager().getCurrentViewN()) / 2) + 1;
            // For unordered asynch requests, a single reply is fine.
            // Where such messages are used, either the reply doesn't matter
            // (broadcastBFTDecision) or only a single replica is expected to
            // reply (e.g., BFTInitiator in submitTransaction).
            return 1;
        }
    }

    /*

    // This version of ReplyListener is useful for cases where we don't expect to process replies from
    // all (or a quorum of) the replicas in the shard, but rather the reply of only one special replica.
    // Not using this class right now, but useful to have for future use.

    private class ReplyListenerAsynchSingle implements ReplyListener {
        AsynchServiceProxy client;
        private int shardID;
        private TOMMessage replies[];
        private String strModule;

        private Comparator<byte[]> comparator = new Comparator<byte[]>() {
            @Override
            public int compare(byte[] o1, byte[] o2) {
                return Arrays.equals(o1, o2) ? 0 : -1;
            }
        };

        private Extractor extractor = new Extractor() {
            @Override
            public TOMMessage extractResponse(TOMMessage[] replies, int sameContent, int lastReceived) {
                return replies[lastReceived];
            }
        };

        private ReplyListenerAsynchSingle(int shardID) {
            this.shardID = shardID;
            client = clientProxyAsynch.get(shardID);
            replies = new TOMMessage[client.getViewManager().getCurrentViewN()];
            strModule = "AsynchReplyListenerSingle: ";
        }

        @Override
        public void replyReceived(RequestContext context, TOMMessage reply) {

            // When to give reply to the application layer
            int pos = client.getViewManager().getCurrentViewPos(reply.getSender());

            if (pos >= 0) { //only consider messages from replicas

                replies[pos] = reply;
                String strReply = null;
                try {
                    strReply = new String(reply.getContent(), "UTF-8");
                    // Ignore dummy responses, only capture response from the BFTInitiator (which is non-dummy)
                    if (!strReply.equals(ResponseType.DUMMY)) {
                        String key = shardToClientAsynch.get(shardID) + ";" + context.getReqId() + ";" + context.getRequestType();
                        int dummyVal = -1;
                        TOMMessage shardResponse = extractor.extractResponse(replies, dummyVal, pos);
                        asynchReplies.put(key, shardResponse);
                        client.cleanAsynchRequest(context.getReqId());
                    }
                } catch (Exception e) {
                    logMsg(strLabel, strModule, "Exception in printing final reply of shard ID " + shardID);
                }

            }
        }

    }
    */

    void logMsg(String id, String module, String msg) {
        System.out.println(id + " " + System.currentTimeMillis() + " [thread-" + Thread.currentThread().getId() + "] " + module + msg);
    }
}




