package es.moki.ratelimitj.aerospike.time;

import es.moki.ratelimitj.core.time.TimeSupplier;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * @author gunda.abhishek
 * @created 23/01/2020
 */

/**
 * Aerospike Bins have a limitation of 15 Chars on Names
 * Since we are using the timestamps as the window identifiers the normal timestamp will be a constraint
 * this class uses a specific Datetime stamp as a referral to calculate the current time instead of the standard UTC time
 */
public class AerospikeTimeSupplier implements TimeSupplier {

    @Override
    public CompletionStage<Long> getAsync() {
        return CompletableFuture.completedFuture(get());
    }

    @Override
    public Mono<Long> getReactive() {
        return Mono.just(get());
    }

    /**
     * @return Returns the current time in seconds elapsed from the REFERRAL_TIMESTAMP
     */
    @Override
    public long get() {
        return Math.subtractExact((System.currentTimeMillis()/1000),TimeConstant.REFERRAL_TIMESTAMP.getTimestamp());
    }

    enum TimeConstant{
        // Referral timestamp in the vicinity of current system time to limit the time counter to < 15chars
        REFERRAL_TIMESTAMP(1578915202);

        private long timestamp;
        TimeConstant(long timestamp){
            this.timestamp = timestamp;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }
}
