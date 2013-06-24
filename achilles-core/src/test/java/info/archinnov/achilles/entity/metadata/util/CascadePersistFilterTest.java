package info.archinnov.achilles.entity.metadata.util;

import static javax.persistence.CascadeType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;

import java.util.Arrays;

import org.junit.Test;


import com.google.common.collect.Collections2;

/**
 * CascadePersistFilterTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class CascadePersistFilterTest
{

	@Test
	public void should_filter_cascade_persist_or_all() throws Exception
	{
		CascadePersistFilter filter = new CascadePersistFilter();
		PropertyMeta<Void, String> pm1 = PropertyMetaTestBuilder
				.valueClass(String.class)
				.field("name")
				.cascadeTypes(MERGE, ALL)
				.build();
		PropertyMeta<Void, String> pm2 = PropertyMetaTestBuilder
				.valueClass(String.class)
				.field("name")
				.cascadeTypes(REMOVE, REFRESH)
				.build();

		assertThat(Collections2.filter(Arrays.asList(pm1), filter)).containsExactly(pm1);
		assertThat(Collections2.filter(Arrays.asList(pm2), filter)).isEmpty();
	}
}
