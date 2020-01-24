package es.moki.ratelimitj.aerospike.time;

import es.moki.ratelimitj.core.time.TimeSupplier;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author gunda.abhishek
 * @created 23/01/2020
 * @project ratelimitj
 */
public class ASTimeBanditSupplier implements TimeSupplier {

    private AerospikeTimeSupplier aerospikeTimeSupplier = new AerospikeTimeSupplier();

    private final AtomicLong time = new AtomicLong(aerospikeTimeSupplier.get());

    public long addUnixSeconds(long seconds){
        return time.addAndGet(seconds);
    }

    @Override
    public CompletionStage<Long> getAsync() {
        return CompletableFuture.completedFuture(get());
    }

    @Override
    public Mono<Long> getReactive() {
        return Mono.just(get());
    }


    @Override
    public long get() {
        return time.get();
    }
}
