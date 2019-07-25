package es.moki.ratelimitj.hazelcast;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import es.moki.ratelimitj.core.limiter.request.RequestLimitRule;
import es.moki.ratelimitj.core.limiter.request.RequestRateLimiter;
import es.moki.ratelimitj.core.time.TimeSupplier;
import es.moki.ratelimitj.test.limiter.request.AbstractSyncRequestRateLimiterTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

import java.util.Set;

import static es.moki.ratelimitj.hazelcast.HazelcastTestFactory.newStandaloneHazelcastInstance;


public class HazelcastSlidingWindowSyncRequestRateLimiterTest extends AbstractSyncRequestRateLimiterTest {

    private static HazelcastInstance hz;

    @BeforeAll
    static void beforeAll() {
        hz = newStandaloneHazelcastInstance();
    }

    @AfterAll
    static void afterAll() {
        hz.shutdown();
    }

    @AfterEach
    void afterEach() {

        hz.getDistributedObjects().forEach(DistributedObject::destroy);
    }

    @Override
    protected RequestRateLimiter getRateLimiter(Set<RequestLimitRule> rules, TimeSupplier timeSupplier) {
        return new HazelcastSlidingWindowRequestRateLimiter(hz, rules, timeSupplier);
    }

}
