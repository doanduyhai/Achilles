package info.archinnov.achilles.entity.metadata;

import static org.fest.assertions.api.Assertions.assertThat;
import org.junit.Test;

/**
 * PropertyTypeTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class PropertyTypeTest {

    @Test
    public void should_test_is_lazy() throws Exception {
        assertThat(PropertyType.COUNTER.isLazy()).isTrue();
        assertThat(PropertyType.LAZY_SIMPLE.isLazy()).isTrue();
        assertThat(PropertyType.LAZY_LIST.isLazy()).isTrue();
        assertThat(PropertyType.LAZY_SET.isLazy()).isTrue();
        assertThat(PropertyType.LAZY_MAP.isLazy()).isTrue();
        assertThat(PropertyType.WIDE_MAP.isLazy()).isTrue();
        assertThat(PropertyType.COUNTER_WIDE_MAP.isLazy()).isTrue();
        assertThat(PropertyType.JOIN_SIMPLE.isLazy()).isTrue();
        assertThat(PropertyType.JOIN_LIST.isLazy()).isTrue();
        assertThat(PropertyType.JOIN_SET.isLazy()).isTrue();
        assertThat(PropertyType.JOIN_MAP.isLazy()).isTrue();
        assertThat(PropertyType.JOIN_WIDE_MAP.isLazy()).isTrue();

        assertThat(PropertyType.ID.isLazy()).isFalse();
        assertThat(PropertyType.SIMPLE.isLazy()).isFalse();
        assertThat(PropertyType.LIST.isLazy()).isFalse();
        assertThat(PropertyType.MAP.isLazy()).isFalse();
        assertThat(PropertyType.EMBEDDED_ID.isLazy()).isFalse();
    }

    @Test
    public void should_test_is_join_column() throws Exception {
        assertThat(PropertyType.ID.isJoin()).isFalse();
        assertThat(PropertyType.SIMPLE.isJoin()).isFalse();
        assertThat(PropertyType.LIST.isJoin()).isFalse();
        assertThat(PropertyType.MAP.isJoin()).isFalse();
        assertThat(PropertyType.COUNTER.isJoin()).isFalse();
        assertThat(PropertyType.LAZY_SIMPLE.isJoin()).isFalse();
        assertThat(PropertyType.LAZY_LIST.isJoin()).isFalse();
        assertThat(PropertyType.LAZY_SET.isJoin()).isFalse();
        assertThat(PropertyType.LAZY_MAP.isJoin()).isFalse();
        assertThat(PropertyType.WIDE_MAP.isJoin()).isFalse();
        assertThat(PropertyType.COUNTER_WIDE_MAP.isJoin()).isFalse();
        assertThat(PropertyType.EMBEDDED_ID.isJoin()).isFalse();

        assertThat(PropertyType.JOIN_SIMPLE.isJoin()).isTrue();
        assertThat(PropertyType.JOIN_LIST.isJoin()).isTrue();
        assertThat(PropertyType.JOIN_SET.isJoin()).isTrue();
        assertThat(PropertyType.JOIN_MAP.isJoin()).isTrue();
        assertThat(PropertyType.JOIN_WIDE_MAP.isJoin()).isTrue();
    }

    @Test
    public void should_test_is_wide_map() throws Exception {
        assertThat(PropertyType.ID.isWideMap()).isFalse();
        assertThat(PropertyType.SIMPLE.isWideMap()).isFalse();
        assertThat(PropertyType.LIST.isWideMap()).isFalse();
        assertThat(PropertyType.MAP.isWideMap()).isFalse();
        assertThat(PropertyType.COUNTER.isWideMap()).isFalse();
        assertThat(PropertyType.LAZY_SIMPLE.isWideMap()).isFalse();
        assertThat(PropertyType.LAZY_LIST.isWideMap()).isFalse();
        assertThat(PropertyType.LAZY_SET.isWideMap()).isFalse();
        assertThat(PropertyType.LAZY_MAP.isWideMap()).isFalse();
        assertThat(PropertyType.WIDE_MAP.isWideMap()).isTrue();
        assertThat(PropertyType.COUNTER_WIDE_MAP.isWideMap()).isTrue();
        assertThat(PropertyType.JOIN_SIMPLE.isWideMap()).isFalse();
        assertThat(PropertyType.JOIN_LIST.isWideMap()).isFalse();
        assertThat(PropertyType.JOIN_SET.isWideMap()).isFalse();
        assertThat(PropertyType.JOIN_MAP.isWideMap()).isFalse();
        assertThat(PropertyType.JOIN_WIDE_MAP.isWideMap()).isTrue();
        assertThat(PropertyType.EMBEDDED_ID.isWideMap()).isFalse();
    }

    @Test
    public void should_test_is_counter() throws Exception {
        assertThat(PropertyType.ID.isCounter()).isFalse();
        assertThat(PropertyType.SIMPLE.isCounter()).isFalse();
        assertThat(PropertyType.LIST.isCounter()).isFalse();
        assertThat(PropertyType.MAP.isCounter()).isFalse();
        assertThat(PropertyType.COUNTER.isCounter()).isTrue();
        assertThat(PropertyType.LAZY_SIMPLE.isCounter()).isFalse();
        assertThat(PropertyType.LAZY_LIST.isCounter()).isFalse();
        assertThat(PropertyType.LAZY_SET.isCounter()).isFalse();
        assertThat(PropertyType.LAZY_MAP.isCounter()).isFalse();
        assertThat(PropertyType.WIDE_MAP.isCounter()).isFalse();
        assertThat(PropertyType.COUNTER_WIDE_MAP.isCounter()).isTrue();
        assertThat(PropertyType.JOIN_SIMPLE.isCounter()).isFalse();
        assertThat(PropertyType.JOIN_LIST.isCounter()).isFalse();
        assertThat(PropertyType.JOIN_SET.isCounter()).isFalse();
        assertThat(PropertyType.JOIN_MAP.isCounter()).isFalse();
        assertThat(PropertyType.JOIN_WIDE_MAP.isCounter()).isFalse();
        assertThat(PropertyType.EMBEDDED_ID.isCounter()).isFalse();
    }

    @Test
    public void should_test_is_proxy_type() throws Exception {
        assertThat(PropertyType.ID.isProxyType()).isFalse();
        assertThat(PropertyType.SIMPLE.isProxyType()).isFalse();
        assertThat(PropertyType.LIST.isProxyType()).isFalse();
        assertThat(PropertyType.MAP.isProxyType()).isFalse();
        assertThat(PropertyType.COUNTER.isProxyType()).isTrue();
        assertThat(PropertyType.LAZY_SIMPLE.isProxyType()).isFalse();
        assertThat(PropertyType.LAZY_LIST.isProxyType()).isFalse();
        assertThat(PropertyType.LAZY_SET.isProxyType()).isFalse();
        assertThat(PropertyType.LAZY_MAP.isProxyType()).isFalse();
        assertThat(PropertyType.WIDE_MAP.isProxyType()).isTrue();
        assertThat(PropertyType.COUNTER_WIDE_MAP.isProxyType()).isTrue();
        assertThat(PropertyType.JOIN_SIMPLE.isProxyType()).isFalse();
        assertThat(PropertyType.JOIN_LIST.isProxyType()).isFalse();
        assertThat(PropertyType.JOIN_SET.isProxyType()).isFalse();
        assertThat(PropertyType.JOIN_MAP.isProxyType()).isFalse();
        assertThat(PropertyType.JOIN_WIDE_MAP.isProxyType()).isTrue();
        assertThat(PropertyType.EMBEDDED_ID.isProxyType()).isFalse();
    }

    @Test
    public void should_test_is_multikey() throws Exception {
        assertThat(PropertyType.COUNTER.isEmbeddedId()).isFalse();
        assertThat(PropertyType.LAZY_SIMPLE.isEmbeddedId()).isFalse();
        assertThat(PropertyType.LAZY_LIST.isEmbeddedId()).isFalse();
        assertThat(PropertyType.LAZY_SET.isEmbeddedId()).isFalse();
        assertThat(PropertyType.LAZY_MAP.isEmbeddedId()).isFalse();
        assertThat(PropertyType.WIDE_MAP.isEmbeddedId()).isFalse();
        assertThat(PropertyType.COUNTER_WIDE_MAP.isEmbeddedId()).isFalse();
        assertThat(PropertyType.JOIN_SIMPLE.isEmbeddedId()).isFalse();
        assertThat(PropertyType.JOIN_LIST.isEmbeddedId()).isFalse();
        assertThat(PropertyType.JOIN_SET.isEmbeddedId()).isFalse();
        assertThat(PropertyType.JOIN_MAP.isEmbeddedId()).isFalse();
        assertThat(PropertyType.JOIN_WIDE_MAP.isEmbeddedId()).isFalse();

        assertThat(PropertyType.ID.isEmbeddedId()).isFalse();
        assertThat(PropertyType.SIMPLE.isEmbeddedId()).isFalse();
        assertThat(PropertyType.LIST.isEmbeddedId()).isFalse();
        assertThat(PropertyType.MAP.isEmbeddedId()).isFalse();
        assertThat(PropertyType.EMBEDDED_ID.isEmbeddedId()).isTrue();
    }

    @Test
    public void should_test_is_valid_clustered_value() throws Exception {
        assertThat(PropertyType.COUNTER.isValidClusteredValueType()).isTrue();
        assertThat(PropertyType.LAZY_SIMPLE.isValidClusteredValueType()).isFalse();
        assertThat(PropertyType.LAZY_LIST.isValidClusteredValueType()).isFalse();
        assertThat(PropertyType.LAZY_SET.isValidClusteredValueType()).isFalse();
        assertThat(PropertyType.LAZY_MAP.isValidClusteredValueType()).isFalse();
        assertThat(PropertyType.WIDE_MAP.isValidClusteredValueType()).isFalse();
        assertThat(PropertyType.COUNTER_WIDE_MAP.isValidClusteredValueType()).isFalse();
        assertThat(PropertyType.JOIN_SIMPLE.isValidClusteredValueType()).isTrue();
        assertThat(PropertyType.JOIN_LIST.isValidClusteredValueType()).isFalse();
        assertThat(PropertyType.JOIN_SET.isValidClusteredValueType()).isFalse();
        assertThat(PropertyType.JOIN_MAP.isValidClusteredValueType()).isFalse();
        assertThat(PropertyType.JOIN_WIDE_MAP.isValidClusteredValueType()).isFalse();

        assertThat(PropertyType.ID.isValidClusteredValueType()).isFalse();
        assertThat(PropertyType.SIMPLE.isValidClusteredValueType()).isTrue();
        assertThat(PropertyType.LIST.isValidClusteredValueType()).isFalse();
        assertThat(PropertyType.MAP.isValidClusteredValueType()).isFalse();
        assertThat(PropertyType.EMBEDDED_ID.isValidClusteredValueType()).isFalse();
    }
}
