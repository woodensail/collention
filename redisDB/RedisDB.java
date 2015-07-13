import javafx.util.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.jedis.JedisConnection;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.*;

/**
 * Created by sail on 2015/5/14.
 */
@Component
public class RedisDB implements MemoryDB {

    @Autowired
    private JedisConnectionFactory jcf;

    private final String lockIdField = "lock-id";
    private final String lockPrefix = "lock-locker-";
    private long lockExpireMillis = 800;
    private long lockRetryMillis = 0;
    private long lockTimeoutMillis = 1000;


    private JedisConnection jConn;
    private Serializer serializer = new JacksonSerializer();
    private boolean enableMulti = false;
    private boolean multiStatus = false;
    private Long lockId = null;

    @Override
    public <T> T execute(Executor<T> executor) {
        Assert.notNull(executor, "Executor object must not be null");
        if (null == jConn) {
            jConn = jcf.getConnection();
            multiStatus = false;
        }
        if (enableMulti && !multiStatus) {
            multi();
        }
        T result = executor.executor(jConn);
        return result;
    }

    @Override
    public void setValue(Object k, Object v) {
        final byte[] rawK = serialize(k);
        final byte[] rawV = serialize(v);
        execute(new Executor() {
            public Object executor(RedisConnection connection) throws DataAccessException {
                connection.set(rawK, rawV);
                return null;
            }
        });
    }

    @Override
    public Boolean setValueNX(Object k, Object v) {
        final byte[] rawK = serialize(k);
        final byte[] rawV = serialize(v);
        return execute(new Executor<Boolean>() {
            public Boolean executor(RedisConnection connection) throws DataAccessException {
                return connection.setNX(rawK, rawV);
            }
        });
    }

    @Override
    public <T> T getValue(Object k, Class<T> vClazz) {
        final byte[] rawK = serialize(k);
        byte[] rawV = execute(new Executor<byte[]>() {
            public byte[] executor(RedisConnection connection) throws DataAccessException {
                return connection.get(rawK);
            }
        });
        return deserialize(rawV, vClazz);
    }

    @Override
    public void setValues(Map map) {
        if (map.isEmpty()) {
            return;
        }
        final List<Pair<byte[], byte[]>> rawMap = serializeMapToList(map);
        execute(new Executor() {
            public Object executor(RedisConnection connection) throws DataAccessException {
                for (Pair<byte[], byte[]> pair : rawMap) {
                    connection.set(pair.getKey(), pair.getValue());
                }
                return null;
            }
        });
    }

    @Override
    public <T> List<T> getValues(Collection<Object> keys, Class<T> vClazz) {
        if (keys.isEmpty()) {
            return new ArrayList<>();
        }
        final List<byte[]> rawK = serializeCollection(keys);
        List<byte[]> results = execute(new Executor<List<byte[]>>() {
            public List<byte[]> executor(RedisConnection connection) throws DataAccessException {
                return connection.mGet(rawK.toArray(new byte[][]{}));
            }
        });
        return deserializeCollection(results, vClazz);
    }

    @Override
    public Long del(Object k) {
        final byte[] rawK = serialize(k);
        return execute(new Executor<Long>() {
            public Long executor(RedisConnection connection) throws DataAccessException {
                return connection.del(rawK);
            }
        });
    }

    @Override
    public Long dels(Collection<Object> keys) {
        if (keys.isEmpty()) {
            return 0L;
        }
        final List<byte[]> rawK = serializeCollection(keys);
        return execute(new Executor<Long>() {
            public Long executor(RedisConnection connection) throws DataAccessException {
                return connection.del(rawK.toArray(new byte[][]{}));
            }
        });
    }

    @Override
    public Boolean setHash(Object k, Object hk, Object hv) {
        final byte[] rawK = serialize(k);
        final byte[] rawHK = serialize(hk);
        final byte[] rawHV = serialize(hv);
        return execute(new Executor<Boolean>() {
            public Boolean executor(RedisConnection connection) throws DataAccessException {
                return connection.hSet(rawK, rawHK, rawHV);
            }
        });
    }

    @Override
    public <T> T getHash(Object k, Object hk, Class<T> hvClazz) {
        final byte[] rawK = serialize(k);
        final byte[] rawHK = serialize(hk);
        byte[] rawHV = execute(new Executor<byte[]>() {
            public byte[] executor(RedisConnection connection) throws DataAccessException {
                return connection.hGet(rawK, rawHK);
            }
        });
        return deserialize(rawHV, hvClazz);
    }

    @Override
    public <K, V> Map<K, V> getHashAll(Object k, Class<K> hkClazz, Class<V> hvClazz) {
        final byte[] rawK = serialize(k);
        Map<byte[], byte[]> result = execute(new Executor<Map<byte[], byte[]>>() {
            public Map<byte[], byte[]> executor(RedisConnection connection) throws DataAccessException {
                return connection.hGetAll(rawK);
            }
        });
        return deserializeMap(result, hkClazz, hvClazz);
    }

    @Override
    public Long delHash(Object k, Object hk) {
        final byte[] rawK = serialize(k);
        final byte[] rawHK = serialize(hk);
        return execute(new Executor<Long>() {
            public Long executor(RedisConnection connection) throws DataAccessException {
                return connection.hDel(rawK, rawHK);
            }
        });
    }

    @Override
    public void setHashs(Object k, Map map) {
        if (map.isEmpty()) {
            return;
        }
        final byte[] rawK = serialize(k);
        final List<Pair<byte[], byte[]>> rawMap = serializeMapToList(map);
        execute(new Executor() {
            public Object executor(RedisConnection connection) throws DataAccessException {
                for (Pair<byte[], byte[]> pair : rawMap) {
                    connection.hSet(rawK, pair.getKey(), pair.getValue());
                }
                return null;
            }
        });
    }

    @Override
    public <T> List<T> getHashs(Object k, Collection<Object> hks, Class<T> hvClazz) {
        if (hks.isEmpty()) {
            return new ArrayList<>();
        }
        final byte[] rawK = serialize(k);
        final List<byte[]> rawHK = serializeCollection(hks);
        List<byte[]> results = execute(new Executor<List<byte[]>>() {
            public List<byte[]> executor(RedisConnection connection) throws DataAccessException {
                return connection.hMGet(rawK, rawHK.toArray(new byte[][]{}));
            }
        });
        return deserializeCollection(results, hvClazz);
    }

    @Override
    public Long delHashs(Object k, Collection<Object> hks) {
        if (hks.isEmpty()) {
            return 0L;
        }
        final byte[] rawK = serialize(k);
        final List<byte[]> rawHK = serializeCollection(hks);
        return execute(new Executor<Long>() {
            public Long executor(RedisConnection connection) throws DataAccessException {
                return connection.hDel(rawK, rawHK.toArray(new byte[][]{}));
            }
        });
    }

    @Override
    public Long setSet(Object k, Object v) {
        final byte[] rawK = serialize(k);
        final byte[] rawV = serialize(v);
        return execute(new Executor<Long>() {
            public Long executor(RedisConnection connection) throws DataAccessException {
                return connection.sAdd(rawK, rawV);

            }
        });
    }

    @Override
    public void multi() {
        if (!multiStatus) {
            multiStatus = true;
            if (null != jConn) {
                jConn.multi();
            }
        }
    }


    @Override
    public List<Object> exec() {
        if (multiStatus) {
            multiStatus = false;
            if (null != jConn) {
                return jConn.exec();
            }
        }
        return null;
    }


    @Override
    public void discard() {
        multiStatus = false;
        if (null != jConn) {
            jConn.discard();
        }
    }

    @Override
    public void watch(Object k) {
        final byte[] rawK = serialize(k);
        execute(new Executor<Object>() {
            public Object executor(RedisConnection connection) throws DataAccessException {
                connection.watch(rawK);
                return null;
            }
        });
    }

    @Override
    public void unwatch() {
        execute(new Executor<Object>() {
            public Object executor(RedisConnection connection) throws DataAccessException {
                connection.unwatch();
                return null;
            }
        });
    }

    @Override
    public Long incr(Object k) {
        final byte[] rawK = serialize(k);
        return execute(new Executor<Long>() {
            public Long executor(RedisConnection connection) throws DataAccessException {
                return connection.incr(rawK);
            }
        });
    }

    @Override
    public Boolean pExpire(Object k, long millis) {
        final byte[] rawK = serialize(k);
        final long m = millis;
        return execute(new Executor<Boolean>() {
            public Boolean executor(RedisConnection connection) throws DataAccessException {
                return connection.pExpire(rawK, m);
            }
        });
    }

    @Override
    public Long pTtl(Object k) {
        final byte[] rawK = serialize(k);
        return execute(new Executor<Long>() {
            public Long executor(RedisConnection connection) throws DataAccessException {
                return connection.pTtl(rawK);
            }
        });
    }

    @Override
    public boolean lock(String k) {
        if (multiStatus) {
            return false;
        }
        if (null == lockId) {
            lockId = incr(lockIdField);
        }
        String key = lockPrefix + k;
        if (setValueNX(key, lockId)) {
            pExpire(key, lockExpireMillis);
            watch(key);
            return true;
        } else {
            if (pTtl(k) < 0) {
                pExpire(key, lockExpireMillis);
            }
            if (-1 == lockRetryMillis) {
                return false;
            } else {
                long timeOutLimit = new Date().getTime() + lockTimeoutMillis;
                while (true) {
                    long timeLeft = timeOutLimit - new Date().getTime();
                    if (0 >= timeLeft) {
                        return false;
                    }
                    try {
                        long ttl = pTtl(key) + 1;
                        ttl = 0 == ttl ? 1 : ttl;
                        long sleepTime = 0 == lockRetryMillis ? ttl : lockRetryMillis;
                        sleepTime = sleepTime < timeLeft ? sleepTime : timeLeft;
                        Thread.sleep(sleepTime);
                        if (setValueNX(key, lockId)) {
                            pExpire(key, lockExpireMillis);
                            watch(key);
                            return true;
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        return false;
                    }
                }
            }
        }
    }

    @Override
    public void unlock(String k) {
        String key = lockPrefix + k;
        watch(key);
        if (Objects.equals(getValue(key, Long.class), lockId)) {
            multi();
            del(key);
            exec();
        }
    }

    private byte[] serialize(Object value) {
        return serializer.serialize(value);
    }

    private List<byte[]> serializeCollection(Collection values) {
        List<byte[]> rawValues = new ArrayList<>();
        int i = 0;
        for (Object value : values) {
            rawValues.add(serialize(value));
        }
        return rawValues;
    }

    private List<Pair<byte[], byte[]>> serializeMapToList(Map valueMap) {
        List<Pair<byte[], byte[]>> rawList = new ArrayList<>();
        for (Object o : valueMap.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            rawList.add(new Pair<>(serialize(entry.getKey()), serialize(entry.getValue())));
        }
        return rawList;
    }

    private <T> T deserialize(byte[] bytes, Class<T> javaType) {
        return serializer.deserialize(bytes, javaType);
    }

    private <T> List<T> deserializeCollection(Collection<byte[]> bytesArray, Class<T> javaType) {
        List<T> objectList = new ArrayList<>();
        for (byte[] bytes : bytesArray) {
            objectList.add(deserialize(bytes, javaType));
        }
        return objectList;
    }

    private <K, V> Map<K, V> deserializeMap(Map<byte[], byte[]> rawMap, Class<K> hkClass, Class<V> hvClass) {
        Map<K, V> valueMap = new HashMap<>();
        rawMap.forEach((k, v) -> valueMap.put(deserialize(k, hkClass), deserialize(v, hvClass)));
        return valueMap;
    }

    public void enableMulti(boolean flag) {
        enableMulti = flag;
    }
}
