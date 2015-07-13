import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.data.redis.serializer.SerializationException;
import org.springframework.data.redis.serializer.SerializationUtils;

import java.nio.charset.Charset;

/**
 * Created by sail on 2015/5/20.
 */
public class JacksonSerializer implements Serializer {

    static final byte[] EMPTY_ARRAY = new byte[0];

    static boolean isEmpty(byte[] data) {
        return (data == null || data.length == 0);
    }

    private ObjectMapper objectMapper = new ObjectMapper();
    private final Charset charset = Charset.forName("UTF8");

    @Override
    public byte[] serialize(Object o) throws SerializationException {
        if (o == null) {
            return EMPTY_ARRAY;
        }
        try {
            if(String.class==o.getClass()) {
                return ((String)o).getBytes(charset);
            }else{
                return this.objectMapper.writeValueAsBytes(o);
            }
        } catch (Exception ex) {
            throw new SerializationException("Could not write JSON: " + ex.getMessage(), ex);
        }
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> javaType) throws SerializationException {
        if (isEmpty(bytes)) {
            return null;
        }
        try {
            if(String.class==javaType) {
                return (T)new String(bytes, charset);
            }else {
                return this.objectMapper.readValue(bytes, 0, bytes.length, javaType);
            }
        } catch (Exception ex) {
            throw new SerializationException("Could not read JSON: " + ex.getMessage(), ex);
        }
    }
}
