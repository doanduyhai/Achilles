package info.archinnov.achilles.entity.metadata;

import info.archinnov.achilles.entity.metadata.util.PropertyTypeExclude;
import info.archinnov.achilles.entity.metadata.util.PropertyTypeFilter;
import java.util.Set;
import com.google.common.collect.Sets;

/**
 * PropertyType
 * 
 * @author DuyHai DOAN
 * 
 */
public enum PropertyType {

    ID(5), //
    EMBEDDED_ID(10), //
    SIMPLE(10), //
    LIST(10), //
    SET(10), //
    MAP(10), //
    LAZY_SIMPLE(30), //
    COUNTER(30), //
    LAZY_LIST(30), //
    LAZY_SET(30), //
    LAZY_MAP(30), //
    WIDE_MAP(30), //
    COUNTER_WIDE_MAP(30), //
    JOIN_SIMPLE(30), //
    JOIN_LIST(30), //
    JOIN_SET(30), //
    JOIN_MAP(30), //
    JOIN_WIDE_MAP(30);

    private final int flag;

    PropertyType(int flag) {
        this.flag = flag;
    }

    public byte[] flag() {
        return new byte[] { (byte) flag };
    }

    public boolean isLazy() {
        return (this == COUNTER //
                || this == LAZY_SIMPLE //
                || this == LAZY_LIST //
                || this == LAZY_SET //
                || this == LAZY_MAP //
                || this == WIDE_MAP //
                || this == COUNTER_WIDE_MAP //
                || this == JOIN_SIMPLE //
                || this == JOIN_LIST //
                || this == JOIN_SET //
                || this == JOIN_MAP //
        || this == JOIN_WIDE_MAP);
    }

    public boolean isJoin() {
        return (this == JOIN_SIMPLE //
                || this == JOIN_LIST //
                || this == JOIN_SET //
                || this == JOIN_MAP //
        || this == JOIN_WIDE_MAP);
    }

    public boolean isWideMap() {
        return (this == WIDE_MAP //
                || this == COUNTER_WIDE_MAP //
        || this == JOIN_WIDE_MAP);
    }

    public boolean isCounter() {
        return (this == COUNTER //
        || this == COUNTER_WIDE_MAP);
    }

    public boolean isProxyType() {
        return (this == COUNTER //
                || this == COUNTER_WIDE_MAP //
                || this == WIDE_MAP //
        || this == JOIN_WIDE_MAP);
    }

    public boolean isEmbeddedId() {
        return this == EMBEDDED_ID;
    }

    public static PropertyType[] nonProxyJoinTypes() {
        return new PropertyType[] { JOIN_SIMPLE, JOIN_LIST, JOIN_SET, JOIN_MAP };
    }

    public boolean isValidClusteredValueType() {
        return (this == SIMPLE || this == JOIN_SIMPLE || this == COUNTER);
    }

    public static PropertyTypeFilter joinPropertyType = new PropertyTypeFilter(nonProxyJoinTypes());
    public static PropertyTypeFilter joinSimpleType = new PropertyTypeFilter(JOIN_SIMPLE);
    public static PropertyTypeFilter joinCollectionType = new PropertyTypeFilter(JOIN_LIST, JOIN_SET);
    public static PropertyTypeFilter joinMapType = new PropertyTypeFilter(JOIN_MAP);
    public static PropertyTypeFilter wideMapType = new PropertyTypeFilter(WIDE_MAP, JOIN_WIDE_MAP, COUNTER_WIDE_MAP);
    public static PropertyTypeFilter isProxyType = new PropertyTypeFilter(COUNTER, COUNTER_WIDE_MAP, WIDE_MAP,
            JOIN_WIDE_MAP);

    public static PropertyTypeFilter eagerType = new PropertyTypeFilter(ID, EMBEDDED_ID, SIMPLE, LIST, SET, MAP);
    public static PropertyTypeFilter lazyNonProxyType = new PropertyTypeFilter(SIMPLE, LAZY_SIMPLE, LIST, LAZY_LIST,
            SET, LAZY_SET, MAP, LAZY_MAP, JOIN_SIMPLE, JOIN_LIST, JOIN_SET, JOIN_MAP);

    public static PropertyTypeExclude excludeIdType = new PropertyTypeExclude(ID, EMBEDDED_ID);

    public static PropertyTypeExclude excludeProxyType = new PropertyTypeExclude(COUNTER, COUNTER_WIDE_MAP, WIDE_MAP,
            JOIN_WIDE_MAP);

    public static Set<PropertyType> multiValuesNonProxyTypes = Sets.newHashSet(LIST, LAZY_LIST, SET, LAZY_SET, MAP,
            LAZY_MAP, JOIN_LIST, JOIN_SET, JOIN_MAP);

}
