package uk.ac.ucl.cs.sec.chainspace.bft;

/**
 * Created by mus on 07/02/19.
 */
public class BFTUtils {
    public static int mapObjectToShard(String object, int numShards) {
        int shardID = Integer.parseInt(object) % numShards;
        System.out.println("Object " + object + " mapped to shard " + shardID);
        return shardID;
    }
}
