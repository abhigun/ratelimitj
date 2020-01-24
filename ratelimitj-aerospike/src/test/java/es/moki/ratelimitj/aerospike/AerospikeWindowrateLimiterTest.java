package es.moki.ratelimitj.aerospike;

import com.aerospike.client.AerospikeClient;
import com.google.common.collect.ImmutableSet;
import es.moki.ratelimitj.aerospike.request.AerospikeSlidingWindowRequestRateLimiter;
import es.moki.ratelimitj.aerospike.time.ASTimeBanditSupplier;
import es.moki.ratelimitj.core.limiter.request.RequestLimitRule;
import es.moki.ratelimitj.core.limiter.request.RequestRateLimiter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
public class AerospikeWindowrateLimiterTest {

    private static AerospikeClient aerospikeClient;
    private static AerospikeConfig aerospikeConfig;
    private static ASTimeBanditSupplier asTimeBanditSupplier;

    @BeforeAll
    static void BeforeAll(){
        AerospikeTestFactory aerospikeTestFactory = new AerospikeTestFactory();
        aerospikeClient = aerospikeTestFactory.getClient();
        aerospikeConfig = aerospikeTestFactory.getConfig();
        asTimeBanditSupplier = new ASTimeBanditSupplier();
    }

    @AfterEach
    void AfterEach(){
        aerospikeClient.truncate(null,aerospikeConfig.getNamespace(),aerospikeConfig.getSessionSet(),null);
    }

    @AfterAll
    static void AfterAll(){
        aerospikeClient.close();
    }

    @Test
    void shouldLimitSingleWindowSync()  {

        ImmutableSet<RequestLimitRule> rules = ImmutableSet.of(RequestLimitRule.of(Duration.ofSeconds(10), 5));
        RequestRateLimiter requestRateLimiter = getRateLimiter(rules);

        IntStream.rangeClosed(1, 5).forEach(value -> {
            asTimeBanditSupplier.addUnixSeconds(1);
            assertThat(requestRateLimiter.overLimitWhenIncremented("ip:127.0.1.1")).isFalse();
        });

        assertThat(requestRateLimiter.overLimitWhenIncremented("ip:127.0.1.1")).isTrue();
    }

    private RequestRateLimiter getRateLimiter(Set<RequestLimitRule> rules) {
        return new AerospikeSlidingWindowRequestRateLimiter(aerospikeClient,rules,asTimeBanditSupplier, aerospikeConfig);
    }
}
