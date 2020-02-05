package es.moki.ratelimitj.aerospike.request;
import com.aerospike.client.*;
import es.moki.ratelimitj.aerospike.AerospikeCommands;
import es.moki.ratelimitj.aerospike.AerospikeConnection;
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
    private final AerospikeConnection aerospikeConnection;
    private final AerospikeCommands aerospikeCommands;

    public AerospikeSlidingWindowRequestRateLimiter(AerospikeConnection aerospikeConnection, Set<RequestLimitRule> rules, TimeSupplier timeSupplier){
        requireNonNull(rules, "rules can not be null");
        if (rules.isEmpty()) {
            throw new IllegalArgumentException("at least one rule must be provided");
        }
        requireNonNull(rules, "time supplier can not be null");

        this.rulesSupplier = new DefaultRequestLimitRulesSupplier(rules);
        this.timeSupplier = timeSupplier;
        this.aerospikeConnection = aerospikeConnection;
        this.aerospikeCommands = new AerospikeCommands(this.aerospikeConnection);
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
        Key k = aerospikeCommands.key(key);
        return aerospikeCommands.deleteRecord(k);
    }


    private boolean eqOrGeLimit(String key, int weight,boolean strictlyGreater) {
        final long now = timeSupplier.get();
        final Set<RequestLimitRule> rules = rulesSupplier.getRules(key);

        final int longestDuration = rules.stream().map(RequestLimitRule::getDurationSeconds).reduce(Integer::max).orElse(0);

        List<SavedKey> savedKeys = new ArrayList<>(rules.size());

        Key k = aerospikeCommands.key(key);
        Record record = aerospikeCommands.getRecord(k);

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
            List<String> outOfWindowBins = new ArrayList<>();

            long trim = Math.min(savedKey.trimBefore, oldTs + savedKey.blocks);

            for (long oldBlock = oldTs; oldBlock <= trim - 1; oldBlock++) {
                String bkey = savedKey.countKey + oldBlock;
                Long bcount = null;
                if(record != null)
                    bcount = record.getLong(bkey);
                if (bcount != null) {
                    decr = decr + bcount;
                    outOfWindowBins.add(bkey);
                }
            }

            Long cur = null;
            if (!outOfWindowBins.isEmpty()) {
                // Delete the outofWindowBins TODO: Can be an Asynchronous deletion
                aerospikeCommands.deleteBins(k,outOfWindowBins);
                // Update the window count as to the cleaned up Bins
                cur = aerospikeCommands.updateAndGet(savedKey.countKey,k, -decr);
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

        // there is enough resources, update the counts
        aerospikeCommands.updateCounts(k,savedKeys,weight);

        // Extend the expiry of the key
        aerospikeCommands.changeExpiry(k,longestDuration);

        return geLimit;

    }
}
