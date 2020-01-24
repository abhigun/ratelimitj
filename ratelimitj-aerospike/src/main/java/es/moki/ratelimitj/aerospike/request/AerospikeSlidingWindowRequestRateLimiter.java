package es.moki.ratelimitj.aerospike.request;
import com.aerospike.client.*;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import es.moki.ratelimitj.aerospike.AerospikeConfig;
import es.moki.ratelimitj.core.limiter.request.DefaultRequestLimitRulesSupplier;
import es.moki.ratelimitj.core.limiter.request.RequestLimitRule;
import es.moki.ratelimitj.core.limiter.request.RequestRateLimiter;
import es.moki.ratelimitj.core.time.TimeSupplier;
import es.moki.ratelimitj.inmemory.request.SavedKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.*;
import static es.moki.ratelimitj.core.RateLimitUtils.coalesce;
import static java.util.Objects.requireNonNull;


@ThreadSafe
public class AerospikeSlidingWindowRequestRateLimiter implements RequestRateLimiter {

    private static final Logger LOG = LoggerFactory.getLogger(AerospikeSlidingWindowRequestRateLimiter.class);
    private final DefaultRequestLimitRulesSupplier rulesSupplier;
    private final TimeSupplier timeSupplier;
    private final AerospikeClient aerospikeClient;
    private final ClientPolicy clientPolicy;
    private final AerospikeConfig aerospikeConfig;

    public AerospikeSlidingWindowRequestRateLimiter(AerospikeClient aerospikeClient, Set<RequestLimitRule> rules, TimeSupplier timeSupplier, AerospikeConfig aerospikeConfig){
        requireNonNull(aerospikeClient, "Aerospike Client can not be null");
        requireNonNull(aerospikeConfig, "Aerospike Config can not be null");
        requireNonNull(rules, "rules can not be null");
        if (rules.isEmpty()) {
            throw new IllegalArgumentException("at least one rule must be provided");
        }
        requireNonNull(rules, "time supplier can not be null");

        this.aerospikeClient = aerospikeClient;
        this.rulesSupplier = new DefaultRequestLimitRulesSupplier(rules);
        this.timeSupplier = timeSupplier;
        this.clientPolicy = new ClientPolicy();
        this.aerospikeConfig = aerospikeConfig;
    }
    @Override
    public boolean overLimitWhenIncremented(String key) {
        return overLimitWhenIncremented(key,1);
    }

    @Override
    public boolean overLimitWhenIncremented(String key, int weight) {

        return eqOrGeLimit(key,weight,true);
    }

    @Override
    public boolean geLimitWhenIncremented(String key) {

        return geLimitWhenIncremented(key,1);
    }

    @Override
    public boolean geLimitWhenIncremented(String key, int weight) {
        return eqOrGeLimit(key,weight,false);
    }

    @Override
    public boolean resetLimit(String key) {
        Key k = new Key(aerospikeConfig.getNamespace(),aerospikeConfig.getSessionSet(),key);
        if(aerospikeClient.exists(clientPolicy.readPolicyDefault,k)){
            aerospikeClient.delete(clientPolicy.writePolicyDefault,k);
            return true;
        }
        return false;

    }

    /**
     * @param k current Key value of the window
     * @param longestDuration expiry time in seconds
     */
    private void changeExpiry(Key k, int longestDuration){
        if(aerospikeClient.exists(clientPolicy.readPolicyDefault,k)){
            WritePolicy writePolicy = new WritePolicy(clientPolicy.writePolicyDefault);
            writePolicy.expiration = longestDuration;
            aerospikeClient.touch(writePolicy,k);
        }
    }

    private Record getRecord(Key k){
        return aerospikeClient.get(clientPolicy.readPolicyDefault,k);
    }

    private void deleteBin(Key k,String binName){
        Bin bin = Bin.asNull(binName);
        aerospikeClient.put(clientPolicy.writePolicyDefault,k,bin);
    }

    private boolean eqOrGeLimit(String key, int weight,boolean strictlyGreater) {
        final long now = timeSupplier.get();
        final Set<RequestLimitRule> rules = rulesSupplier.getRules(key);

        final int longestDuration = rules.stream().map(RequestLimitRule::getDurationSeconds).reduce(Integer::max).orElse(0);

        List<SavedKey> savedKeys = new ArrayList<>(rules.size());
        Key k = new Key(aerospikeConfig.getNamespace(),aerospikeConfig.getSessionSet(),key);
        Record record = getRecord(k);

        boolean geLimit = false;

        for(RequestLimitRule rule: rules){
            SavedKey savedKey = new SavedKey(now, rule.getDurationSeconds(), rule.getPrecisionSeconds());
            savedKeys.add(savedKey);
            Long oldTs = null;

            if(record != null)
                oldTs = record.getLong(savedKey.tsKey);

            oldTs = oldTs != null ? oldTs : savedKey.trimBefore;

            if (oldTs > now) {
                // don't write in the past
                return true;
            }

            // discover what needs to be cleaned up
            long decr = 0;
            List<String> dele = new ArrayList<>();
            long trim = Math.min(savedKey.trimBefore, oldTs + savedKey.blocks);

            for (long oldBlock = oldTs; oldBlock <= trim - 1; oldBlock++) {
                String bkey = savedKey.countKey + oldBlock;
                Long bcount = null;
                if(record != null)
                    bcount = record.getLong(bkey);
                if (bcount != null) {
                    decr = decr + bcount;
                    dele.add(bkey);
                }
            }

            Long cur = null;
            if (!dele.isEmpty()) {
                for(String binname : dele) {
                    deleteBin(k,binname);
                }
//                final long decrement = decr;
                Bin countbin = new Bin(savedKey.countKey,-decr);

//          Instead of making multiple DB calls makes all the operations in a single connection
                cur = aerospikeClient.operate(clientPolicy.writePolicyDefault,k, Operation.add(countbin),Operation.get(savedKey.countKey)).getLong(savedKey.countKey);

            } else {
                if(record != null)
                    cur = record.getLong(savedKey.countKey);
            }

            // check our limits
            long count = coalesce(cur, 0L) + weight;
            if (count > rule.getLimit()) {
                return true; // over limit, don't record request
            } else if (!strictlyGreater && count == rule.getLimit()) {
                geLimit = true; // at limit, do record request
            }
        }

//         there is enough resources, update the counts
        for (SavedKey savedKey : savedKeys) {
            //update the current timestamp, count, and bucket count
            Bin tsbin = new Bin(savedKey.tsKey,savedKey.trimBefore);
            Bin countbin = new Bin(savedKey.countKey,weight);
            Bin bucketbin = new Bin(savedKey.countKey+savedKey.blockId,weight);
            aerospikeClient.operate(clientPolicy.writePolicyDefault,k,Operation.put(tsbin),Operation.add(countbin),Operation.add(bucketbin));

        }
        // Extend the expiry of the key
        changeExpiry(k,longestDuration);
        return geLimit;

    }
}
