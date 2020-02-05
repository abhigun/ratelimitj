package es.moki.ratelimitj.aerospike;

import com.aerospike.client.AerospikeClient;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import es.moki.ratelimitj.aerospike.request.AerospikeSlidingWindowRequestRateLimiter;
import es.moki.ratelimitj.aerospike.time.ASTimeBanditSupplier;
import es.moki.ratelimitj.core.limiter.request.RequestLimitRule;
import es.moki.ratelimitj.core.limiter.request.RequestRateLimiter;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
public class AerospikeWindowRateLimiterTest {

    private static AerospikeConnection aerospikeConnection;
    private static AerospikeConfig aerospikeConfig;
    private static ASTimeBanditSupplier timeBandit;

    @BeforeAll
    static void BeforeAll(){
        AerospikeTestFactory aerospikeTestFactory = new AerospikeTestFactory();
        aerospikeConnection = aerospikeTestFactory.getConnection();
        aerospikeConfig = aerospikeTestFactory.getConfig();
        timeBandit = new ASTimeBanditSupplier();
    }

    @AfterEach
    void AfterEach(){
        aerospikeConnection.getAerospikeClient().truncate(null,aerospikeConfig.getNamespace(),aerospikeConfig.getSessionSet(),null);
    }

    @AfterAll
    static void AfterAll(){
        aerospikeConnection.stop();
    }

    @Test
    void shouldLimitSingleWindowSync()  {

        ImmutableSet<RequestLimitRule> rules = ImmutableSet.of(RequestLimitRule.of(Duration.ofSeconds(10), 5));
        RequestRateLimiter requestRateLimiter = getRateLimiter(rules);

        IntStream.rangeClosed(1, 5).forEach(value -> {
            timeBandit.addUnixSeconds(1);
            assertThat(requestRateLimiter.overLimitWhenIncremented("ip:127.0.1.1")).isFalse();
        });

        assertThat(requestRateLimiter.overLimitWhenIncremented("ip:127.0.1.1")).isTrue();
    }

    @Test
    void shouldGeLimitSingleWindowSync() {

        ImmutableSet<RequestLimitRule> rules = ImmutableSet.of(RequestLimitRule.of(Duration.ofSeconds(10), 5));
        RequestRateLimiter requestRateLimiter = getRateLimiter(rules);

        IntStream.rangeClosed(1, 4).forEach(value -> {
            timeBandit.addUnixSeconds(1);
            assertThat(requestRateLimiter.geLimitWhenIncremented("ip:127.0.1.2")).isFalse();
        });

        assertThat(requestRateLimiter.geLimitWhenIncremented("ip:127.0.1.2")).isTrue();
    }

    @Test
    void shouldLimitWithWeightSingleWindowSync() {

        ImmutableSet<RequestLimitRule> rules = ImmutableSet.of(RequestLimitRule.of(Duration.ofSeconds(10), 10));
        RequestRateLimiter requestRateLimiter = getRateLimiter(rules);

        IntStream.rangeClosed(1, 5).forEach(value -> {
            timeBandit.addUnixSeconds(1);
            assertThat(requestRateLimiter.overLimitWhenIncremented("ip:127.0.1.2", 2)).isFalse();
        });

        assertThat(requestRateLimiter.overLimitWhenIncremented("ip:127.0.1.2", 2)).isTrue();
    }

    @Test
    void shouldLimitSingleWindowSyncWithMultipleKeys() {

        ImmutableSet<RequestLimitRule> rules = ImmutableSet.of(RequestLimitRule.of(Duration.ofSeconds(100), 5));
        RequestRateLimiter requestRateLimiter = getRateLimiter(rules);

        IntStream.rangeClosed(1, 5).forEach(value -> {
            timeBandit.addUnixSeconds(10);
            IntStream.rangeClosed(1, 10).forEach(
                    keySuffix -> assertThat(requestRateLimiter.overLimitWhenIncremented("ip:127.0.0." + keySuffix)).isFalse());
        });

        IntStream.rangeClosed(1, 10).forEach(
                keySuffix -> assertThat(requestRateLimiter.overLimitWhenIncremented("ip:127.0.0." + keySuffix)).isTrue());

        timeBandit.addUnixSeconds(50);
        IntStream.rangeClosed(1, 10).forEach(
                keySuffix -> assertThat(requestRateLimiter.overLimitWhenIncremented("ip:127.0.0." + keySuffix)).isFalse());
    }

    @Test
    void shouldLimitSingleWindowSyncWithKeySpecificRules() {

        RequestLimitRule rule1 = RequestLimitRule.of(Duration.ofSeconds(100), 5).matchingKeys("ip:127.9.0.0");
        RequestLimitRule rule2 = RequestLimitRule.of(Duration.ofSeconds(100), 10);

        RequestRateLimiter requestRateLimiter = getRateLimiter(ImmutableSet.of(rule1, rule2));

        IntStream.rangeClosed(1, 5).forEach(value -> {
            timeBandit.addUnixSeconds(10);
            assertThat(requestRateLimiter.overLimitWhenIncremented("ip:127.9.0.0")).isFalse();
        });
        assertThat(requestRateLimiter.overLimitWhenIncremented("ip:127.9.0.0")).isTrue();

        IntStream.rangeClosed(1, 10).forEach(value -> assertThat(requestRateLimiter.overLimitWhenIncremented("ip:127.9.1.0")).isFalse());
        assertThat(requestRateLimiter.overLimitWhenIncremented("ip:127.9.1.0")).isTrue();
    }

    @Test
    void shouldResetLimit() {
        ImmutableSet<RequestLimitRule> rules = ImmutableSet.of(RequestLimitRule.of(Duration.ofSeconds(60), 1));
        RequestRateLimiter requestRateLimiter = getRateLimiter(rules);

        String key = "ip:127.1.0.1";
        assertThat(requestRateLimiter.overLimitWhenIncremented(key)).isFalse();
        assertThat(requestRateLimiter.overLimitWhenIncremented(key)).isTrue();

        assertThat(requestRateLimiter.resetLimit(key)).isTrue();
        assertThat(requestRateLimiter.resetLimit(key)).isFalse();

        assertThat(requestRateLimiter.overLimitWhenIncremented(key)).isFalse();
    }


    @Test
    void shouldRateLimitOverTime() {
        RequestLimitRule rule1 = RequestLimitRule.of(Duration.ofSeconds(5), 250).withPrecision(Duration.ofSeconds(1)).matchingKeys("ip:127.3.9.3");
        RequestRateLimiter requestRateLimiter = getRateLimiter(ImmutableSet.of(rule1));
        AtomicLong timeOfLastOperation = new AtomicLong();

        IntStream.rangeClosed(1, 50).forEach(loop -> {

            IntStream.rangeClosed(1, 250).forEach(value -> {
                timeBandit.addUnixTimeMilliSeconds(14L);
                boolean overLimit = requestRateLimiter.overLimitWhenIncremented("ip:127.3.9.3");
                if (overLimit) {
                    long timeSinceLastOperation = timeBandit.get() - timeOfLastOperation.get();
                    assertThat(timeSinceLastOperation).isLessThan(3);
                } else {
                    timeOfLastOperation.set(timeBandit.get());
                }
            });

        });
    }

    @Test @Disabled
    void shouldPreventThunderingHerdWithPrecision() {

        RequestLimitRule rule1 = RequestLimitRule.of(Duration.ofSeconds(5), 250).withPrecision(Duration.ofSeconds(1)).matchingKeys("ip:127.9.9.9");
        RequestRateLimiter requestRateLimiter = getRateLimiter(ImmutableSet.of(rule1));
        Map<Long, Integer> underPerSecond = new LinkedHashMap<>();
        Map<Long, Integer> overPerSecond = new HashMap<>();

        IntStream.rangeClosed(1, 50).forEach(loop -> {

            IntStream.rangeClosed(1, 250).forEach(value -> {
                timeBandit.addUnixTimeMilliSeconds(14L);
                boolean overLimit = requestRateLimiter.overLimitWhenIncremented("ip:127.9.9.9");
                if (!overLimit) {
                    underPerSecond.merge(timeBandit.get(), 1, Integer::sum);
                } else {
                    overPerSecond.merge(timeBandit.get(), 1, Integer::sum);
                }
            });

        });

        Set<Long> allSeconds = Sets.newTreeSet(Sets.union(underPerSecond.keySet(), overPerSecond.keySet()));

        allSeconds.forEach((k)->System.out.println("Time seconds : " + k + " under count : " + underPerSecond.get(k) + " over count : " + overPerSecond.get(k)));
    }


    private RequestRateLimiter getRateLimiter(Set<RequestLimitRule> rules) {
        return new AerospikeSlidingWindowRequestRateLimiter(aerospikeConnection,rules,timeBandit);
    }
}
