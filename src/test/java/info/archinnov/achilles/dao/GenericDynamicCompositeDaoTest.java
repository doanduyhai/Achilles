package info.archinnov.achilles.dao;

import static info.archinnov.achilles.entity.metadata.PropertyType.END_EAGER;
import static info.archinnov.achilles.entity.metadata.PropertyType.START_EAGER;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.serializer.SerializerUtils;
import me.prettyprint.cassandra.model.ExecutingKeyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.mutation.Mutator;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * GenericDynamicCompositeDaoTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class GenericDynamicCompositeDaoTest
{

	@InjectMocks
	private GenericDynamicCompositeDao<Long> dao = new GenericDynamicCompositeDao<Long>();

	@Mock
	private ExecutingKeyspace keyspace;

	@Mock
	private final Serializer<Long> serializer = SerializerUtils.LONG_SRZ;

	@Test
	public void should_build_mutator() throws Exception
	{

		Mutator<Long> mutator = dao.buildMutator();
		assertThat(mutator).isNotNull();
	}

	@Test
	public void should_build_start_composite_for_eager_fetch() throws Exception
	{

		DynamicComposite comp = dao.startCompositeForEagerFetch;

		assertThat(comp.getComponent(0).getValue()).isEqualTo(START_EAGER.flag());
		assertThat(comp.getComponent(0).getEquality()).isSameAs(ComponentEquality.EQUAL);
	}

	@Test
	public void should_build_end_composite_for_eager_fetch() throws Exception
	{

		DynamicComposite comp = dao.endCompositeForEagerFetch;

		assertThat(comp.getComponent(0).getValue()).isEqualTo(END_EAGER.flag());
		assertThat(comp.getComponent(0).getEquality()).isSameAs(
				ComponentEquality.GREATER_THAN_EQUAL);
	}
}
