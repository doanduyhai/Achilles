package info.archinnov.achilles.entity.context;

import static info.archinnov.achilles.entity.type.ConsistencyLevel.ONE;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.entity.context.AchillesFlushContext.FlushType;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.type.ConsistencyLevel;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * AchillesPersistenceContextTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class AchillesPersistenceContextTest
{
	@Mock
	private AchillesPersistenceContext context;

	@Mock
	private EntityMeta entityMeta;

	@Mock
	private AchillesFlushContext flushContext;

	@Before
	public void setUp()
	{
		doCallRealMethod().when(context).setEntityMeta(any(EntityMeta.class));
		doCallRealMethod().when(context).setFlushContext(any(AchillesFlushContext.class));

		context.setEntityMeta(entityMeta);
		context.setFlushContext(flushContext);
	}

	@Test
	public void should_return_wide_row() throws Exception
	{
		doCallRealMethod().when(context).isWideRow();

		when(entityMeta.isWideRow()).thenReturn(true);
		assertThat(context.isWideRow()).isTrue();
	}

	@Test
	public void should_return_column_family_name() throws Exception
	{
		doCallRealMethod().when(context).getTableName();

		when(entityMeta.getTableName()).thenReturn("table");
		assertThat(context.getTableName()).isEqualTo("table");
	}

	@Test
	public void should_return_true_for_is_batch_mode() throws Exception
	{
		doCallRealMethod().when(context).isBatchMode();

		when(flushContext.type()).thenReturn(FlushType.BATCH);
		assertThat(context.isBatchMode()).isTrue();
	}

	@Test
	public void should_return_false_for_is_batch_mode() throws Exception
	{
		doCallRealMethod().when(context).isBatchMode();

		when(flushContext.type()).thenReturn(FlushType.IMMEDIATE);
		assertThat(context.isBatchMode()).isFalse();
	}

	@Test
	public void should_call_flush() throws Exception
	{
		doCallRealMethod().when(context).flush();

		context.flush();

		verify(flushContext).flush();
	}

	@Test
	public void should_call_end_batch() throws Exception
	{
		doCallRealMethod().when(context).endBatch();

		context.endBatch();

		verify(flushContext).endBatch();
	}

	@Test
	public void should_set_read_cl() throws Exception
	{
		doCallRealMethod().when(context).setReadConsistencyLevel(any(ConsistencyLevel.class));

		context.setReadConsistencyLevel(ONE);

		verify(flushContext).setReadConsistencyLevel(ONE);
	}

	@Test
	public void should_set_write_cl() throws Exception
	{
		doCallRealMethod().when(context).setWriteConsistencyLevel(any(ConsistencyLevel.class));

		context.setWriteConsistencyLevel(ONE);

		verify(flushContext).setWriteConsistencyLevel(ONE);
	}

	@Test
	public void should_reinit_cls() throws Exception
	{
		doCallRealMethod().when(context).reinitConsistencyLevels();

		context.reinitConsistencyLevels();

		verify(flushContext).reinitConsistencyLevels();
	}

	@Test
	public void should_clean_flush_context() throws Exception
	{
		doCallRealMethod().when(context).cleanUpFlushContext();

		context.cleanUpFlushContext();

		verify(flushContext).cleanUp();
	}
}
