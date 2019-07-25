package es.moki.ratelimitj.aerospike;

import com.aerospike.client.AerospikeClient;
import es.moki.ratelimitj.aerospike.request.AerospikeConfig;
import es.moki.ratelimitj.aerospike.request.AerospikeSlidingWindowRequestRateLimiter;
import es.moki.ratelimitj.core.limiter.request.RequestLimitRule;
import es.moki.ratelimitj.core.limiter.request.RequestRateLimiter;
import es.moki.ratelimitj.core.time.TimeSupplier;
import es.moki.ratelimitj.test.limiter.request.AbstractSyncRequestRateLimiterTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import java.util.Set;

public class AerospikeWindowrateLimiterTest extends AbstractSyncRequestRateLimiterTest {

    private static AerospikeClient aerospikeClient;
    private static AerospikeConfig aerospikeConfig;

    @BeforeAll
    static void BeforeAll(){
        AerospikeTestFactory aerospikeTestFactory = new AerospikeTestFactory();
        aerospikeClient = aerospikeTestFactory.getClient();
        aerospikeConfig = aerospikeTestFactory.getConfig();
    }

    @AfterEach
    void AfterEach(){
        aerospikeClient.truncate(null,aerospikeConfig.getNamespace(),aerospikeConfig.getSessionSet(),null);
    }

    @AfterAll
    static void AfterAll(){

        aerospikeClient.close();
    }


    @Override
    protected RequestRateLimiter getRateLimiter(Set<RequestLimitRule> rules, TimeSupplier timeSupplier) {
        return new AerospikeSlidingWindowRequestRateLimiter(aerospikeClient,rules,timeSupplier, aerospikeConfig);
    }
}
