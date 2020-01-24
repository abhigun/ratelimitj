package es.moki.ratelimitj.aerospike;

import com.aerospike.client.AerospikeClient;


public class AerospikeTestFactory {

    private final AerospikeConfig aerospikeConfig;
    private final AerospikeConnection aerospikeConnection;

    public AerospikeTestFactory() {
        this.aerospikeConfig = new AerospikeConfig("127.0.0.1",5,null,null,null,100,0,5,"test","ratelimiter",60,false);
        this.aerospikeConnection = new AerospikeConnection(aerospikeConfig);
    }

    public AerospikeClient getClient(){
        return aerospikeConnection.getAerospikeClient();
    }

    public AerospikeConfig getConfig(){
        return this.aerospikeConfig;
    }

}
