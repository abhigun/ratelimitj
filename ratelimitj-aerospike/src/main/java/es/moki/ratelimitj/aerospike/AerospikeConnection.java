package es.moki.ratelimitj.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.policy.*;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * @author gunda.abhishek
 * @created 24/01/2020
 */


public class AerospikeConnection {

    private AerospikeClient aerospikeClient;
    private ClientPolicy clientPolicy;

    private AerospikeConfig aerospikeConfig;

    public AerospikeConnection(AerospikeConfig aerospikeConfig){
        this.aerospikeConfig = aerospikeConfig;
        initialize();
    }

    public void initialize(){
        List<Host> hosts = new ArrayList<>();
        Host host = new Host(aerospikeConfig.getHost(), 3000);
        hosts.add(host);

        Policy readPolicy = new Policy();
        readPolicy.maxRetries = aerospikeConfig.getRetries();
        readPolicy.consistencyLevel = ConsistencyLevel.CONSISTENCY_ONE;
        readPolicy.sleepBetweenRetries = aerospikeConfig.getSleepBetweenRetries();
        readPolicy.sendKey = true;

        WritePolicy writePolicy = new WritePolicy();
        writePolicy.maxRetries = aerospikeConfig.getRetries();
        writePolicy.consistencyLevel = ConsistencyLevel.CONSISTENCY_ALL;
        writePolicy.sleepBetweenRetries = aerospikeConfig.getSleepBetweenRetries();
        writePolicy.commitLevel = CommitLevel.COMMIT_ALL;
        writePolicy.sendKey = true;


        clientPolicy = new ClientPolicy();
        clientPolicy.maxConnsPerNode = aerospikeConfig.getMaxConnectionsPerNode();
        clientPolicy.readPolicyDefault = readPolicy;
        clientPolicy.writePolicyDefault = writePolicy;
        clientPolicy.failIfNotConnected = true;
        if(aerospikeConfig.isTLSEnabled()){
            clientPolicy.tlsPolicy = new TlsPolicy();
            clientPolicy.user = aerospikeConfig.getUser();
            clientPolicy.password = aerospikeConfig.getPassword();
        }

        aerospikeClient = new AerospikeClient(clientPolicy,hosts.toArray(new Host[hosts.size()]));
    }

    public AerospikeClient getAerospikeClient() {
        requireNonNull(aerospikeClient, "Aerospike Client can not be null, Call Initiialize() to set the connection");
        return aerospikeClient;
    }

    public ClientPolicy getClientPolicy() {
        requireNonNull(clientPolicy, "ClientPolicy can not be null,Call Initiialize() to set the connection");
        return clientPolicy;
    }

    public AerospikeConfig getConfig(){
        return aerospikeConfig;
    }

    public void stop(){
        requireNonNull(aerospikeClient, "Aerospike Client can not be null, Call Initiialize() to set the connection");
        aerospikeClient.close();
    }
}
