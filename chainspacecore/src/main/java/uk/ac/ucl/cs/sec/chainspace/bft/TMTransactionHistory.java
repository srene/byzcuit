package uk.ac.ucl.cs.sec.chainspace.bft;

/**
 * Created by sheharbano on 23/01/2019.
 */
import java.util.HashMap;

public class TMTransactionHistory {
    HashMap<String, Boolean> hist_prepared_commit; // Indexed by shard ID, value is boolean to indicate
    // whether the shard responded with prepared_commit.
    // Note that if the response is prepared_abort, it
    // doesn't need to be stored here and TM will elicit
    // accept_abort and delete the transaction entry

    long ts_last_shard_msg; // Timestamp of the most recent prepared_commit msg for this transaction

    public TMTransactionHistory() {
        hist_prepared_commit = new  HashMap<>();
        ts_last_shard_msg = 0;
    }
}
