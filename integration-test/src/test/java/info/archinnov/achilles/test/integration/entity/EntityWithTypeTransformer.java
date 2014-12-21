package info.archinnov.achilles.test.integration.entity;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.codec.Codec;
import info.archinnov.achilles.exception.AchillesTranscodingException;
import info.archinnov.achilles.type.NamingStrategy;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Entity(table = EntityWithTypeTransformer.TABLE_NAME)
public class EntityWithTypeTransformer {

    public static final String TABLE_NAME = "type_transforming";

    @PartitionKey
    private Long id;

    @TypeTransformer(valueCodecClass = LongToString.class)
    @Column
    private Long longToString;

    @TypeTransformer(valueCodecClass = LongToString.class)
    @Column
    private List<Long> myList;

    @TypeTransformer(valueCodecClass = LongToString.class)
    @Column
    private Set<Long> mySet;

    @TypeTransformer(keyCodecClass = LongToString.class)
    @Column
    private Map<Long,String> keyMap;

    @TypeTransformer(valueCodecClass = LongToString.class)
    @Column
    private Map<Integer,Long> valueMap;

    @TypeTransformer(keyCodecClass = LongToString.class, valueCodecClass = EnumToString.class)
    @Column
    private Map<Long,NamingStrategy> keyValueMap;

    public EntityWithTypeTransformer() {
    }

    public EntityWithTypeTransformer(Long id, Long longToString, List<Long> myList, Set<Long> mySet, Map<Long, String> keyMap, Map<Integer, Long> valueMap, Map<Long, NamingStrategy> keyValueMap) {
        this.id = id;
        this.longToString = longToString;
        this.myList = myList;
        this.mySet = mySet;
        this.keyMap = keyMap;
        this.valueMap = valueMap;
        this.keyValueMap = keyValueMap;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getLongToString() {
        return longToString;
    }

    public void setLongToString(Long longToString) {
        this.longToString = longToString;
    }

    public List<Long> getMyList() {
        return myList;
    }

    public void setMyList(List<Long> myList) {
        this.myList = myList;
    }

    public Set<Long> getMySet() {
        return mySet;
    }

    public void setMySet(Set<Long> mySet) {
        this.mySet = mySet;
    }

    public Map<Long, String> getKeyMap() {
        return keyMap;
    }

    public void setKeyMap(Map<Long, String> keyMap) {
        this.keyMap = keyMap;
    }

    public Map<Integer, Long> getValueMap() {
        return valueMap;
    }

    public void setValueMap(Map<Integer, Long> valueMap) {
        this.valueMap = valueMap;
    }

    public Map<Long, NamingStrategy> getKeyValueMap() {
        return keyValueMap;
    }

    public void setKeyValueMap(Map<Long, NamingStrategy> keyValueMap) {
        this.keyValueMap = keyValueMap;
    }

    public static class LongToString implements Codec<Long, String> {

        @Override
        public Class<Long> sourceType() {
            return Long.class;
        }

        @Override
        public Class<String> targetType() {
            return String.class;
        }

        @Override
        public String encode(Long fromJava) throws AchillesTranscodingException {
            return fromJava.toString();
        }

        @Override
        public Long decode(String fromCassandra) throws AchillesTranscodingException {
            return Long.parseLong(fromCassandra);
        }
    }

    public static class EnumToString implements Codec<NamingStrategy, String> {

        @Override
        public Class<NamingStrategy> sourceType() {
            return NamingStrategy.class;
        }

        @Override
        public Class<String> targetType() {
            return String.class;
        }

        @Override
        public String encode(NamingStrategy fromJava) throws AchillesTranscodingException {
            return fromJava.name();
        }

        @Override
        public NamingStrategy decode(String fromCassandra) throws AchillesTranscodingException {
            return NamingStrategy.valueOf(fromCassandra);
        }
    }
}
