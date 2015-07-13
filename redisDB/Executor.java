import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;

/**
 * Created by sail on 2015/5/20.
 */
public interface Executor<T> {
    T executor(RedisConnection connection) throws DataAccessException;
}
