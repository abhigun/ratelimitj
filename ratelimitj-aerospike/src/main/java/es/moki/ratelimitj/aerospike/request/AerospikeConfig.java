package es.moki.ratelimitj.aerospike.request;
import lombok.AllArgsConstructor;
import lombok.Getter;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
@AllArgsConstructor
@Getter
public class AerospikeConfig {
    
    private String host;


    private int retries;

    
    private String user;

    
    private String password;

    
    private String tlsName;

    
    private int sleepBetweenRetries;

    
    private int timeout;

    
    private int maxConnectionsPerNode;

    
    private String namespace;

    
    private String sessionSet;
    
    private int tokenExpiry;
}
