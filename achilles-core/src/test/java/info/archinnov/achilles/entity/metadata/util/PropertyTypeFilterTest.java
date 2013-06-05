package info.archinnov.achilles.entity.metadata.util;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.metadata.PropertyMeta;

import java.util.Arrays;

import org.junit.Test;

import testBuilders.PropertyMetaTestBuilder;

import com.google.common.collect.Collections2;

/**
 * PropertyTypeFilterTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class PropertyTypeFilterTest
{

	@Test
	public void should_filter_by_types() throws Exception
	{
		PropertyTypeFilter filter = new PropertyTypeFilter(COUNTER, SIMPLE);

		PropertyMeta<?, ?> pm1 = PropertyMetaTestBuilder.valueClass(String.class).type(SET).build();

		PropertyMeta<?, ?> pm2 = PropertyMetaTestBuilder
				.valueClass(String.class)
				.type(SIMPLE)
				.build();

		PropertyMeta<?, ?> pm3 = PropertyMetaTestBuilder
				.valueClass(String.class)
				.type(WIDE_MAP)
				.build();

		assertThat(Collections2.filter(Arrays.asList(pm1, pm2), filter)).containsOnly(pm2);
		assertThat(Collections2.filter(Arrays.asList(pm1, pm3), filter)).isEmpty();
	}
}
