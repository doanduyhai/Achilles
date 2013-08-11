package info.archinnov.achilles.entity.metadata.util;

import static javax.persistence.CascadeType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;

import java.util.Arrays;

import org.junit.Test;


import com.google.common.collect.Collections2;

/**
 * CascadeMergeFilterTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class CascadeMergeFilterTest
{
	@Test
	public void should_filter_cascade_merge_or_all() throws Exception
	{
		CascadeMergeFilter filter = new CascadeMergeFilter();
		PropertyMeta pm1 = PropertyMetaTestBuilder
				.valueClass(String.class)
				.field("name")
				.cascadeTypes(MERGE, PERSIST)
				.build();
		PropertyMeta pm2 = PropertyMetaTestBuilder
				.valueClass(String.class)
				.field("name")
				.cascadeTypes(REMOVE, REFRESH)
				.build();

		assertThat(Collections2.filter(Arrays.asList(pm1), filter)).containsExactly(pm1);
		assertThat(Collections2.filter(Arrays.asList(pm2), filter)).isEmpty();
	}
}
