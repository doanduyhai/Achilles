package info.archinnov.achilles.entity.metadata;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.entity.type.Pair;

import org.junit.Test;

import testBuilders.PropertyMetaTestBuilder;

/**
 * CounterPropertiesTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class CounterPropertiesTest
{
	@Test
	public void should_to_string() throws Exception
	{
		PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, Long.class) //
				.field("id").type(PropertyType.SIMPLE)
				//
				//
				.consistencyLevels(
						new Pair<ConsistencyLevel, ConsistencyLevel>(ConsistencyLevel.ALL,
								ConsistencyLevel.ALL))//
				.build();
		CounterProperties props = new CounterProperties("fqcn", idMeta);

		assertThat(props.toString()).isEqualTo(
				"CounterProperties [fqcn=fqcn, idMeta=" + idMeta.toString() + "]");
	}
}
