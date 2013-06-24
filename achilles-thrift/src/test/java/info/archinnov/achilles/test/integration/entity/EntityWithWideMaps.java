package info.archinnov.achilles.test.integration.entity;

import info.archinnov.achilles.annotations.CompoundKey;
import info.archinnov.achilles.annotations.Order;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithEnumCompoundKey.Type;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithObjectValue.Holder;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.WideMap;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;

/**
 * EntityWithWideMaps
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
public class EntityWithWideMaps {

    @Id
    private Long id;

    @Column(table = "clustered")
    private WideMap<Key, String> wideMap;

    @Column(table = "clustered_with_counter_value")
    private WideMap<String, Counter> counterWideMap;

    @Column(table = "clustered_with_enum_compound")
    private WideMap<Type, String> wideMapWithEnumKey;

    @JoinColumn(table = "clustered_with_join_value")
    @OneToMany(cascade = CascadeType.ALL)
    private WideMap<String, User> joinWideMap;

    @Column(table = "clustered_with_object_value")
    private WideMap<String, Holder> objectWideMap;

    public EntityWithWideMaps() {
    }

    public EntityWithWideMaps(Long id) {
        this.id = id;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public WideMap<Key, String> getWideMap() {
        return wideMap;
    }

    public void setWideMap(WideMap<Key, String> wideMap) {
        this.wideMap = wideMap;
    }

    public WideMap<String, Counter> getCounterWideMap() {
        return counterWideMap;
    }

    public void setCounterWideMap(WideMap<String, Counter> counterWideMap) {
        this.counterWideMap = counterWideMap;
    }

    public WideMap<Type, String> getWideMapWithEnumKey() {
        return wideMapWithEnumKey;
    }

    public void setWideMapWithEnumKey(WideMap<Type, String> wideMapWithEnumKey) {
        this.wideMapWithEnumKey = wideMapWithEnumKey;
    }

    public WideMap<String, User> getJoinWideMap() {
        return joinWideMap;
    }

    public void setJoinWideMap(WideMap<String, User> joinWideMap) {
        this.joinWideMap = joinWideMap;
    }

    public WideMap<String, Holder> getObjectWideMap() {
        return objectWideMap;
    }

    public void setObjectWideMap(WideMap<String, Holder> objectWideMap) {
        this.objectWideMap = objectWideMap;
    }

    @CompoundKey
    public static class Key
    {
        @Order(1)
        private Integer count;

        @Order(2)
        private String name;

        public Key() {
        }

        public Key(Integer count, String name) {
            this.count = count;
            this.name = name;
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

    }
}
