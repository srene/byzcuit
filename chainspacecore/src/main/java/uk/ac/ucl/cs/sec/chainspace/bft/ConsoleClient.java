package uk.ac.ucl.cs.sec.chainspace.bft;
import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

import bftsmart.tom.ServiceReplica;
import uk.ac.ucl.cs.sec.chainspace.AbortTransactionException;
import uk.ac.ucl.cs.sec.chainspace.CSTransaction;
import uk.ac.ucl.cs.sec.chainspace.Store;

import java.util.Scanner;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.List;

public class ConsoleClient {

    static HashMap<String,String> configData;
    static String shardConfigFile; // Contains info about shards and corresponding config files.
    // This info should be passed on the client class.
    static int thisClient;

    private static boolean loadConfiguration() {
        boolean done = true;

        if(configData.containsKey(ClientConfig.thisClient))
            thisClient = Integer.parseInt(configData.get(ClientConfig.thisClient));
        else {
            System.out.println("Could not find configuration for thisClient.");
            done = false;
        }

        if(configData.containsKey(ClientConfig.shardConfigFile))
            shardConfigFile = configData.get(ClientConfig.shardConfigFile);
        else {
            System.out.println("Could not find configuration for shardConfigFile.");
            done = false;
        }

        return done;
    }

    private static boolean readConfiguration(String configFile) {
        try {
            BufferedReader lineReader = new BufferedReader(new FileReader(configFile));
            String line;
            int countLine = 0;
            int limit = 2; //Split a line into two tokens, the key and value

            while ((line = lineReader.readLine()) != null) {
                countLine++;
                String[] tokens = line.split("\\s+",limit);

                if(tokens.length == 2) {
                    String token = tokens[0];
                    String value = tokens[1];
                    configData.put(token, value);
                }
                else
                    System.out.println("Skipping Line # "+countLine+" in config file: Insufficient tokens");
            }
            lineReader.close();
            return true;
        } catch (Exception e) {
            System.out.println("There was an exception reading configuration file: "+ e.toString());
            return false;
        }

    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: ConsoleClient <config file path>");
            System.exit(0);
        }

        String configFile = args[0];

        configData = new HashMap<String,String>(); // will be filled with config data by readConfiguration()
        readConfiguration(configFile);
        if(!loadConfiguration()) {
            System.out.println("Could not load configuration. Now exiting.");
            System.exit(0);
        }

        MapClient client = new MapClient(shardConfigFile,0,0); // Create clients for talking with other shards
        client.defaultShardID = 0;

        Scanner sc = new Scanner(System.in);
        Scanner console = new Scanner(System.in);

        while (true) {

            client.defaultShardID = 0;

            System.out.println("Select an option:");

            //System.out.println("18. CREATE ACCOUNT");
            //System.out.println("19. TRANSACTION PAYMENT");

            System.out.println("1. REPLAY ATTACK ON PHASE 1");
            System.out.println("2. REPLAY ATTACK ON PHASE 2");


            int cmd = sc.nextInt();
            if(cmd == 1)
                cmd = RequestType.REPLAY_ATTACK_1;
            else if(cmd == 2)
                cmd = RequestType.REPLAY_ATTACK_2;

            List<AccountObject> accounts;
            Transaction t;
            AccountObject accObj;
            String result, key, input, payer, payee, strPayment, amount, accId, accId_bob, accId_alice;
            int balance;


            switch (cmd) {

                case RequestType.CREATE_ACCOUNT:
                    System.out.println("Doing CREATE_ACCOUNT");

                    accounts = new ArrayList<>();

                    System.out.println("Enter (unique) Account ID:");
                    accId = console.nextLine();

                    System.out.println("Enter account balance:");
                    input = console.nextLine();
                    balance = Integer.parseInt(input);

                    accObj = new AccountObject(accId, balance);
                    accounts.add(accObj);

                    System.out.println("Type 'q' to stop, or anything else to continue:");
                    input = console.nextLine();

                    while (!(input.equalsIgnoreCase("q"))) {

                        System.out.println("Enter (unique) Account ID:");
                        accId = console.nextLine();

                        System.out.println("Enter account balance:");
                        input = console.nextLine();
                        balance = Integer.parseInt(input);

                        accObj = new AccountObject(accId, balance);
                        accounts.add(accObj);

                        System.out.println("Type 'q' to stop, or anything else to continue:");
                        input = console.nextLine();
                    }

                    client.createAccounts(accounts);

                    break;
                case RequestType.TRANSACTION_PAYMENT:

                    t = new Transaction();
                    t.operation = "pay";

                    System.out.println("Enter transaction ID:");
                    input = console.nextLine();
                    t.addID(input);

                    System.out.println("Enter payer's account ID:");
                    payer = console.nextLine();

                    System.out.println("Enter payee's account ID:");
                    payee = console.nextLine();

                    System.out.println("Enter the amount to transfer:");
                    amount = console.nextLine();

                    strPayment = payer + ";" + payee + ";" + amount;

                    System.out.println("Transaction to be submitted is: ");
                    t.print();

                    result = client.submitTransaction(t, RequestType.TRANSACTION_PAYMENT);
                    System.out.println("Transaction status: " + result);
                    break;

                case RequestType.REPLAY_ATTACK_1:
                    // ========= Creating accounts =========

                    accounts = new ArrayList<>();

                    System.out.println("Creating accounts for 'alice' and 'bob'.");

                    accId_alice = "alice";
                    balance = 10;
                    accObj = new AccountObject(accId_alice, balance);
                    accounts.add(accObj);

                    accId_bob = "bob";
                    balance = 30;
                    accObj = new AccountObject(accId_bob, balance);
                    accounts.add(accObj);

                    client.createAccounts(accounts);

                    // Give some time for the accounts to be created
                    try {
                        Thread.sleep(5000);
                    }
                    catch(Exception e) {
                        System.out.println("Could not sleep!"+e.getMessage());
                    }

                    System.out.println("Now submitting transaction...");

                    // ========= Payment =========

                    t = new Transaction();
                    t.operation = "pay";
                    t.inputs.add(accId_alice);
                    t.inputs.add(accId_bob);

                    t.addID("22");
                    payer = "bob";
                    payee = "alice";
                    amount = "5";

                    strPayment = payer + ";" + payee + ";" + amount;
                    t.command = strPayment;
                    t.operation = "pay";

                    System.out.println("Transaction to be submitted is:");
                    t.print();

                    result = client.submitTransaction(t, RequestType.TRANSACTION_PAYMENT);
                    //System.out.println("Transaction status: " + result);
                    break;

                case RequestType.REPLAY_ATTACK_2:
                    // ========= Creating accounts =========

                    accounts = new ArrayList<>();

                    System.out.println("Creating account for 'alice' and 'bob'.");

                    String accId_charlie = "charlie";
                    balance = 10;
                    accObj = new AccountObject(accId_charlie, balance);
                    accounts.add(accObj);

                    String accId_dave = "dave";
                    balance = 30;
                    accObj = new AccountObject(accId_dave, balance);
                    accounts.add(accObj);

                    client.createAccounts(accounts);

                    // Give some time for the accounts to be created
                    try {
                        Thread.sleep(5000);
                    }
                    catch(Exception e) {
                        System.out.println("Could not sleep!"+e.getMessage());
                    }

                    System.out.println("Now submitting transaction: Transfer 1 coin from 'charlie' to 'dave'");

                    // ========= Payment =========

                    t = new Transaction();
                    t.operation = "pay";
                    t.inputs.add(accId_charlie);
                    t.inputs.add(accId_dave);

                    t.addID("23");
                    payer = "charlie";
                    payee = "dave";
                    amount = "5";

                    strPayment = payer + ";" + payee + ";" + amount;
                    t.command = strPayment;
                    t.operation = "pay";

                    /*
                    System.out.println("Transaction to be submitted is:");
                    t.print();
                     */

                    result = client.submitTransaction(t, RequestType.TRANSACTION_PAYMENT);

                    //System.out.println("Transaction status: " + result);

                    // ========= Creating account for Charlie again =========

                    if(result.equals(ResponseType.ACCEPTED_T_COMMIT)) {
                        System.out.println("\nTransferred 5 coins from 'charlie' to 'dave'.");
                        System.out.println("The account 'charlie' with balance 10 has been deactivated, and a new account 'charlie_NEW' with balance 5 has been created on shard 0.");
                        System.out.println("The account 'dave' with balance 30 has been deactivated, and a new account 'dave_NEW' with balance 35 has been created on shard 1.");

                        accounts = new ArrayList<>();

                        System.out.println("Creating account for 'charlie' again. This transaction " +
                                "should be aborted because an account object with same ID has been spent. " +
                                "However, as the spent 'charlie' account has been pruned, this transaction " +
                                "will go through, creating fresh 10 coins for 'charlie' out of thin air.\n");

                        balance = 10;
                        accObj = new AccountObject(accId_charlie, balance);
                        accounts.add(accObj);

                        client.createAccounts(accounts);
                    }
                    break;

                default:
                    System.out.println("Not a valid option.");

            }

        }
    }

}