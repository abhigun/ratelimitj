package es.moki.ratelimitj.aerospike;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import es.moki.ratelimitj.aerospike.time.AerospikeTimeSupplier;
import es.moki.ratelimitj.inmemory.request.SavedKey;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author gunda.abhishek
 * @created 05/02/2020
 */
public class ASCommandsTest {

    private static AerospikeCommands aerospikeCommands;
    private static AerospikeConnection aerospikeConnection;
    private static AerospikeConfig aerospikeConfig;
    private static AerospikeTimeSupplier aerospikeTimeSupplier;

    private static final String KEY = "key";
    private static final String STRING_BIN = "string";
    private static final String INTEGER_BIN = "integer";
    private static final String LONG_BIN = "long";


    @BeforeAll
    static void beforeAll(){
        aerospikeConfig = new AerospikeConfig("127.0.0.1",5,null,null,null,100,0,5,"test","ratelimiter",60,false);
        aerospikeConnection = new AerospikeConnection(aerospikeConfig);
        aerospikeCommands = new AerospikeCommands(aerospikeConnection);
        aerospikeTimeSupplier = new AerospikeTimeSupplier();
    }

    @AfterAll
    static void afterAll(){
        aerospikeConnection.stop();
    }
    @AfterEach
    void afterEach(){
        aerospikeConnection.getAerospikeClient().truncate(null,aerospikeConfig.getNamespace(),aerospikeConfig.getSessionSet(),null);
    }
    private void createRecord(Key key){
        Bin stringBin = aerospikeCommands.bin(STRING_BIN,"default");
        Bin intBin = aerospikeCommands.bin(INTEGER_BIN,1);
        Bin longBin = aerospikeCommands.bin(LONG_BIN,100000);
        aerospikeCommands.save(key,stringBin,intBin,longBin);
    }
    @Test
    void createKey(){
        Key k = aerospikeCommands.key(KEY);
        assertThat(k).isNotNull();
    }

    @Test
    void createBin(){
        Bin bin = aerospikeCommands.bin(STRING_BIN,"someval");
        assertThat(bin).isNotNull();
    }


    @Test
    void getRecord(){
        Key k = aerospikeCommands.key(KEY);
        createRecord(k);
        Record record = aerospikeCommands.getRecord(k);
        assertThat(record).isNotNull();
        assertThat(record.bins).hasSize(3);
        assertThat(record.getString(STRING_BIN)).isNotNull().isNotEmpty().isEqualTo("default");
        assertThat(record.getInt(INTEGER_BIN)).isNotNull().isEqualTo(1);
        assertThat(record.getLong(LONG_BIN)).isNotNull().isEqualTo(100000);
    }

    @Test
    void deleteBin(){
        Key k = aerospikeCommands.key(KEY);
        createRecord(k);
        aerospikeCommands.deleteBin(k,STRING_BIN);
        Record record = aerospikeCommands.getRecord(k);
        assertThat(record.bins).hasSize(2);
        assertThat(record.getString(STRING_BIN)).isNullOrEmpty();
    }

    @Test
    void deleteRecord(){
        Key k = aerospikeCommands.key(KEY);
        createRecord(k);
        aerospikeCommands.deleteRecord(k);
        assertThat(aerospikeCommands.getRecord(k)).isNull();
    }

    @Test
    void deleteBins(){
        Key k = aerospikeCommands.key(KEY);
        createRecord(k);
        List<String> binNames = new ArrayList<>(Arrays.asList(STRING_BIN,INTEGER_BIN));
        aerospikeCommands.deleteBins(k,binNames);
        Record record = aerospikeCommands.getRecord(k);
        assertThat(record.bins).hasSize(1);
        assertThat(record.getString(STRING_BIN)).isNullOrEmpty();
    }

    @Test
    void updateAndGet(){
        Key k = aerospikeCommands.key(KEY);
        createRecord(k);
        aerospikeCommands.updateAndGet(INTEGER_BIN,k,5);
        assertThat(aerospikeCommands.getRecord(k).getInt(INTEGER_BIN)).isNotNull().isEqualTo(6);
        aerospikeCommands.updateAndGet(INTEGER_BIN,k, -6);
        assertThat(aerospikeCommands.getRecord(k).getInt(INTEGER_BIN)).isNotNull().isEqualTo(0);

        // Shouldn't be using bins with string values under Operation.add()
        assertThrows(AerospikeException.class, ()->aerospikeCommands.updateAndGet(STRING_BIN,k,10));
    }

    @Test
    void updateCounts(){
        Key k = aerospikeCommands.key(KEY);
        List<SavedKey> savedKeys = new ArrayList<>();
        savedKeys.add(new SavedKey(aerospikeTimeSupplier.get(),10,10));
        savedKeys.add(new SavedKey(aerospikeTimeSupplier.get(),10,5));

        aerospikeCommands.updateCounts(k,savedKeys,1);
        Record record = aerospikeCommands.getRecord(k);
        assertThat(record).isNotNull();
    }

}
