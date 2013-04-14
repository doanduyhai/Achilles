package info.archinnov.achilles.entity.metadata;

import static org.fest.assertions.api.Assertions.assertThat;

import org.junit.Test;

/**
 * PropertyTypeTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class PropertyTypeTest
{

	@Test
	public void should_test_is_lazy() throws Exception
	{
		assertThat(PropertyType.COUNTER.isLazy()).isTrue();
		assertThat(PropertyType.LAZY_SIMPLE.isLazy()).isTrue();
		assertThat(PropertyType.LAZY_LIST.isLazy()).isTrue();
		assertThat(PropertyType.LAZY_SET.isLazy()).isTrue();
		assertThat(PropertyType.LAZY_MAP.isLazy()).isTrue();
		assertThat(PropertyType.WIDE_MAP.isLazy()).isTrue();
		assertThat(PropertyType.WIDE_MAP_COUNTER.isLazy()).isTrue();
		assertThat(PropertyType.JOIN_SIMPLE.isLazy()).isTrue();
		assertThat(PropertyType.JOIN_LIST.isLazy()).isTrue();
		assertThat(PropertyType.JOIN_SET.isLazy()).isTrue();
		assertThat(PropertyType.JOIN_MAP.isLazy()).isTrue();
		assertThat(PropertyType.JOIN_WIDE_MAP.isLazy()).isTrue();

		assertThat(PropertyType.SERIAL_VERSION_UID.isLazy()).isFalse();
		assertThat(PropertyType.SIMPLE.isLazy()).isFalse();
		assertThat(PropertyType.LIST.isLazy()).isFalse();
		assertThat(PropertyType.MAP.isLazy()).isFalse();
	}

	@Test
	public void should_test_is_join_column() throws Exception
	{
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
		assertThat(PropertyType.WIDE_MAP_COUNTER.isJoinColumn()).isFalse();

		assertThat(PropertyType.JOIN_SIMPLE.isJoinColumn()).isTrue();
		assertThat(PropertyType.JOIN_LIST.isJoinColumn()).isTrue();
		assertThat(PropertyType.JOIN_SET.isJoinColumn()).isTrue();
		assertThat(PropertyType.JOIN_MAP.isJoinColumn()).isTrue();
		assertThat(PropertyType.JOIN_WIDE_MAP.isJoinColumn()).isTrue();
	}

	@Test
	public void should_test_is_wide_map() throws Exception
	{
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
		assertThat(PropertyType.WIDE_MAP_COUNTER.isWideMap()).isTrue();
		assertThat(PropertyType.JOIN_SIMPLE.isWideMap()).isFalse();
		assertThat(PropertyType.JOIN_LIST.isWideMap()).isFalse();
		assertThat(PropertyType.JOIN_SET.isWideMap()).isFalse();
		assertThat(PropertyType.JOIN_MAP.isWideMap()).isFalse();
		assertThat(PropertyType.JOIN_WIDE_MAP.isWideMap()).isTrue();
	}
}
