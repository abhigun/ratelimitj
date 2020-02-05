package es.moki.ratelimitj.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import es.moki.ratelimitj.inmemory.request.SavedKey;

import java.util.ArrayList;
import java.util.List;

/**
 * @author gunda.abhishek
 */
public class AerospikeCommands {
    private final AerospikeConnection aerospikeConnection;
    private final AerospikeConfig aerospikeConfig;
    public AerospikeCommands(AerospikeConnection aerospikeConnection){
        this.aerospikeConnection = aerospikeConnection;
        this.aerospikeConfig = aerospikeConnection.getConfig();
    }

    private AerospikeClient getClient(){
        return aerospikeConnection.getAerospikeClient();
    }

    private ClientPolicy getClientPolicy(){
        return aerospikeConnection.getClientPolicy();
    }

    public Key key(String key){
        return new Key(aerospikeConfig.getNamespace(),aerospikeConfig.getSessionSet(),key);
    }

    public Bin bin(String binName, Object value){
        return new Bin(binName, value);
    }

    public boolean isKeyExists(Key k){
        return getClient().exists(getClientPolicy().readPolicyDefault,k);
    }

    public void changeExpiry(Key k, int longestDuration){
        if(getClient().exists(getClientPolicy().readPolicyDefault,k)){
            WritePolicy writePolicy = new WritePolicy(getClientPolicy().writePolicyDefault);
            writePolicy.expiration = longestDuration;
            getClient().touch(writePolicy,k);
        }
    }

    public boolean deleteRecord(Key k){
        if(isKeyExists(k))
            return getClient().delete(getClientPolicy().writePolicyDefault,k);
        return false;
    }

    public Record getRecord(Key k){
        return getClient().get(getClientPolicy().readPolicyDefault,k);
    }

    public void deleteBin(Key k,String binName){
        Bin bin = Bin.asNull(binName);
        getClient().put(getClientPolicy().writePolicyDefault,k,bin);
    }

    public void updateCounts(Key k, List<SavedKey> savedKeys,int weight){
        for (SavedKey savedKey : savedKeys) {
            //update the current timestamp, count, and bucket count
            Bin tsbin = new Bin(savedKey.tsKey,savedKey.trimBefore);
            Bin countbin = new Bin(savedKey.countKey,weight);
            Bin bucketbin = new Bin(savedKey.countKey+savedKey.blockId,weight);
            getClient().operate(getClientPolicy().writePolicyDefault,k, Operation.put(tsbin),Operation.add(countbin),Operation.add(bucketbin));
        }
    }

    public long updateWindowCount(SavedKey savedKey,Key k, long decrement){
        Bin countBin = bin(savedKey.countKey, -decrement);
        return getClient().operate(getClientPolicy().writePolicyDefault,k, Operation.add(countBin),Operation.get(savedKey.countKey)).getLong(savedKey.countKey);
    }

    /**
     * @param k Record Key
     * @param bins List of bins to be deleted
     * Performs a batch Operation with a single call, instead of making multiple Database calls improving the performance
     */
    public void deleteBins(Key k, List<String> bins){
        List<Operation> operations = new ArrayList<>();

        bins.stream().forEach(bin ->{
            Bin temp = Bin.asNull(bin);
            operations.add(Operation.put(temp));
        });
        getClient().operate(getClientPolicy().writePolicyDefault,k,operations.toArray(new Operation[0]));
    }

}
