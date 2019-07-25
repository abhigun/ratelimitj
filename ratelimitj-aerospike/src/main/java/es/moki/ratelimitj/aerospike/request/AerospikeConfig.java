package es.moki.ratelimitj.aerospike.request;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
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
    
    private String instrumentVerificationSet;
    
    private String rateLimiterSet;

    
    private int tokenExpiry;


    public AerospikeConfig(String host, int retries, String user, String password, String tlsName, int sleepBetweenRetries, int timeout, int maxConnectionsPerNode, String namespace, String sessionSet, String instrumentVerificationSet, String rateLimiterSet, int tokenExpiry) {
        this.host = host;
        this.retries = retries;
        this.user = user;
        this.password = password;
        this.tlsName = tlsName;
        this.sleepBetweenRetries = sleepBetweenRetries;
        this.timeout = timeout;
        this.maxConnectionsPerNode = maxConnectionsPerNode;
        this.namespace = namespace;
        this.sessionSet = sessionSet;
        this.instrumentVerificationSet = instrumentVerificationSet;
        this.rateLimiterSet = rateLimiterSet;
        this.tokenExpiry = tokenExpiry;
    }
    public String getHost() {
        return host;
    }

    public int getRetries() {
        return retries;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getTlsName() {
        return tlsName;
    }

    public int getSleepBetweenRetries() {
        return sleepBetweenRetries;
    }

    public int getTimeout() {
        return timeout;
    }

    public int getMaxConnectionsPerNode() {
        return maxConnectionsPerNode;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getSessionSet() {
        return sessionSet;
    }

    public String getInstrumentVerificationSet() {
        return instrumentVerificationSet;
    }

    public String getRateLimiterSet() {
        return rateLimiterSet;
    }

    public int getTokenExpiry() {
        return tokenExpiry;
    }

}
