package es.moki.ratelimitj.aerospike.time;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author gunda.abhishek
 * @created 05/02/2020
 */
public class ASTimeSupplierTest {
    private static AerospikeTimeSupplier aerospikeTimeSupplier;

    @BeforeAll
    static void beforeAll(){
        aerospikeTimeSupplier = new AerospikeTimeSupplier();
    }

    @Test
    void shouldGetSystemCurrentTime() {
        Long time = aerospikeTimeSupplier.get();
        assertThat(time).isNotNull().isNotNegative().isNotZero();
    }

    @Test
    void shouldGetAsyncSystemCurrentTime() throws Exception {
        Long time = aerospikeTimeSupplier.getAsync().toCompletableFuture().get();
        assertThat(time).isNotNull().isNotNegative().isNotZero();
    }

    @Test
    void shouldGetReactiveSystemCurrentTime() {
        Long time = aerospikeTimeSupplier.getReactive().block();
        assertThat(time).isNotNull().isNotNegative().isNotZero();
    }
}
