package es.moki.ratelimitj.aerospike;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * @author gunda.abhishek
 * @created 05/02/2020
 */
public class ASCommandsTest {

    private static AerospikeCommands aerospikeCommands;
    private static AerospikeConnection aerospikeConnection;
    private static AerospikeConfig aerospikeConfig;
    @BeforeAll
    static void beforeAll(){
        aerospikeConfig = new AerospikeConfig("127.0.0.1",5,null,null,null,100,0,5,"test","ratelimiter",60,false);
        aerospikeConnection = new AerospikeConnection(aerospikeConfig);
        aerospikeCommands = new AerospikeCommands(aerospikeConnection);
    }

    @AfterAll
    static void afterAll(){
        aerospikeConnection.stop();
    }

    @AfterEach
    void afterEach(){
        aerospikeConnection.getAerospikeClient().truncate(null,aerospikeConfig.getNamespace(),aerospikeConfig.getSessionSet(),null);
    }

    @Test
    void createKey(){

    }

    @Test
    void createBin(){

    }

    @Test
    void getRecord(){

    }

    @Test
    void getBin(){

    }

    @Test
    void deleteBin(){

    }

    @Test
    void deleteRecord(){

    }

    @Test
    void

}
