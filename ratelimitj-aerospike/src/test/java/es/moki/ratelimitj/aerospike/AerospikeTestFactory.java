package es.moki.ratelimitj.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.policy.*;

import es.moki.ratelimitj.aerospike.request.AerospikeConfig;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;


public class AerospikeTestFactory {

    private final AerospikeConfig aerospikeConfig;
    private  AerospikeClient aerospikeClient;

    public AerospikeTestFactory() {
        this.aerospikeConfig = new AerospikeConfig("127.0.0.1",5,"gandalf_rw","gandalfrolling","phonepeaerospike",100,0,5,"test","ratelimiter","gandalf_instrument_verification","gandalf_rate_limitor",60);
        this.aerospikeClient = getStandAloneConnection(this.aerospikeConfig);
    }

    public AerospikeClient getClient(){
        if(this.aerospikeClient == null){
            return getStandAloneConnection(this.aerospikeConfig);
        }
        return this.aerospikeClient;
    }
    public AerospikeConfig getConfig(){

        return this.aerospikeConfig;
    }
    private AerospikeClient getStandAloneConnection(AerospikeConfig aerospikeConfig){
        List<Host> hosts = new ArrayList<>();
        Host host = new Host(aerospikeConfig.getHost(),3000);
        hosts.add(host);

        Policy readPolicy = new Policy();
        readPolicy.maxRetries = aerospikeConfig.getRetries();
        readPolicy.consistencyLevel = ConsistencyLevel.CONSISTENCY_ONE;
//        readPolicy.replica = Replica.MASTER_PROLES;
        readPolicy.sleepBetweenRetries = aerospikeConfig.getSleepBetweenRetries();
        readPolicy.sendKey = true;

        WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.REPLACE;
        writePolicy.maxRetries = aerospikeConfig.getRetries();
        writePolicy.consistencyLevel = ConsistencyLevel.CONSISTENCY_ALL;
//        writePolicy.replica = Replica.MASTER_PROLES;
        writePolicy.sleepBetweenRetries = aerospikeConfig.getSleepBetweenRetries();
        writePolicy.commitLevel = CommitLevel.COMMIT_ALL;
        writePolicy.sendKey = true;


        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.maxConnsPerNode = aerospikeConfig.getMaxConnectionsPerNode();
        clientPolicy.readPolicyDefault = readPolicy;
        clientPolicy.writePolicyDefault = writePolicy;
        clientPolicy.failIfNotConnected = true;
//        clientPolicy.requestProleReplicas = true;
//        clientPolicy.threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
//        clientPolicy.tlsPolicy = new TlsPolicy();
//        clientPolicy.user = aerospikeConfig.getUser();
//        clientPolicy.password = aerospikeConfig.getPassword();

        return new AerospikeClient(clientPolicy,hosts.toArray(new Host[hosts.size()]));
    }
}
