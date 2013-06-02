package info.archinnov.achilles.entity.metadata;

import static org.fest.assertions.api.Assertions.*;
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

        assertThat(PropertyType.SERIAL_VERSION_UID.isLazy()).isFalse();
        assertThat(PropertyType.SIMPLE.isLazy()).isFalse();
        assertThat(PropertyType.LIST.isLazy()).isFalse();
        assertThat(PropertyType.MAP.isLazy()).isFalse();
        assertThat(PropertyType.CLUSTERED_KEY.isLazy()).isFalse();
    }

    @Test
    public void should_test_is_join_column() throws Exception {
        assertThat(PropertyType.SERIAL_VERSION_UID.isJoinColumn()).isFalse();
        assertThat(PropertyType.SIMPLE.isJoinColumn()).isFalse();
        assertThat(PropertyType.LIST.isJoinColumn()).isFalse();
        assertThat(PropertyType.MAP.isJoinColumn()).isFalse();
        assertThat(PropertyType.COUNTER.isJoinColumn()).isFalse();
        assertThat(PropertyType.LAZY_SIMPLE.isJoinColumn()).isFalse();
        assertThat(PropertyType.LAZY_LIST.isJoinColumn()).isFalse();
        assertThat(PropertyType.LAZY_SET.isJoinColumn()).isFalse();
        assertThat(PropertyType.LAZY_MAP.isJoinColumn()).isFalse();
        assertThat(PropertyType.WIDE_MAP.isJoinColumn()).isFalse();
        assertThat(PropertyType.COUNTER_WIDE_MAP.isJoinColumn()).isFalse();
        assertThat(PropertyType.CLUSTERED_KEY.isJoinColumn()).isFalse();

        assertThat(PropertyType.JOIN_SIMPLE.isJoinColumn()).isTrue();
        assertThat(PropertyType.JOIN_LIST.isJoinColumn()).isTrue();
        assertThat(PropertyType.JOIN_SET.isJoinColumn()).isTrue();
        assertThat(PropertyType.JOIN_MAP.isJoinColumn()).isTrue();
        assertThat(PropertyType.JOIN_WIDE_MAP.isJoinColumn()).isTrue();
    }

    @Test
    public void should_test_is_wide_map() throws Exception {
        assertThat(PropertyType.SERIAL_VERSION_UID.isWideMap()).isFalse();
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
        assertThat(PropertyType.CLUSTERED_KEY.isWideMap()).isFalse();
    }

    @Test
    public void should_test_is_counter() throws Exception {
        assertThat(PropertyType.SERIAL_VERSION_UID.isCounter()).isFalse();
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
        assertThat(PropertyType.CLUSTERED_KEY.isCounter()).isFalse();
    }

    @Test
    public void should_test_is_proxy_type() throws Exception {
        assertThat(PropertyType.SERIAL_VERSION_UID.isProxyType()).isFalse();
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
        assertThat(PropertyType.CLUSTERED_KEY.isProxyType()).isFalse();
    }

    @Test
    public void should_test_is_multikey() throws Exception {
        assertThat(PropertyType.COUNTER.isClusteredKey()).isFalse();
        assertThat(PropertyType.LAZY_SIMPLE.isClusteredKey()).isFalse();
        assertThat(PropertyType.LAZY_LIST.isClusteredKey()).isFalse();
        assertThat(PropertyType.LAZY_SET.isClusteredKey()).isFalse();
        assertThat(PropertyType.LAZY_MAP.isClusteredKey()).isFalse();
        assertThat(PropertyType.WIDE_MAP.isClusteredKey()).isFalse();
        assertThat(PropertyType.COUNTER_WIDE_MAP.isClusteredKey()).isFalse();
        assertThat(PropertyType.JOIN_SIMPLE.isClusteredKey()).isFalse();
        assertThat(PropertyType.JOIN_LIST.isClusteredKey()).isFalse();
        assertThat(PropertyType.JOIN_SET.isClusteredKey()).isFalse();
        assertThat(PropertyType.JOIN_MAP.isClusteredKey()).isFalse();
        assertThat(PropertyType.JOIN_WIDE_MAP.isClusteredKey()).isFalse();

        assertThat(PropertyType.SERIAL_VERSION_UID.isClusteredKey()).isFalse();
        assertThat(PropertyType.SIMPLE.isClusteredKey()).isFalse();
        assertThat(PropertyType.LIST.isClusteredKey()).isFalse();
        assertThat(PropertyType.MAP.isClusteredKey()).isFalse();
        assertThat(PropertyType.CLUSTERED_KEY.isClusteredKey()).isTrue();
    }
}
