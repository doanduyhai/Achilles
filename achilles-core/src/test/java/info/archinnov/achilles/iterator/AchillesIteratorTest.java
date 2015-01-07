/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.iterator;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.Iterator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import com.datastax.driver.core.Row;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.persistence.operations.EntityMapper;
import info.archinnov.achilles.internal.persistence.operations.EntityProxifier;
import info.archinnov.achilles.internal.reflection.RowMethodInvoker;
import info.archinnov.achilles.query.slice.SliceQueryProperties;
import info.archinnov.achilles.test.mapping.entity.ClusteredEntity;

@RunWith(MockitoJUnitRunner.class)
public class AchillesIteratorTest {

    private AchillesIterator<ClusteredEntity> sliceIterator;

    @Mock
    private EntityMapper mapper;

    @Mock
    private RowMethodInvoker cqlInvoker;

    @Mock
    private EntityProxifier proxifier;

    @Mock
    private SliceQueryProperties<ClusteredEntity> sliceQuery;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PersistenceContext context;

    @Mock
    private PersistenceContext.EntityFacade entityFacade;

    @Mock
    private Iterator<Row> iterator;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EntityMeta meta;

    @Before
    public void setUp() {
        when(sliceQuery.getEntityClass()).thenReturn(ClusteredEntity.class);
        when(context.getEntityFacade()).thenReturn(entityFacade);

        sliceIterator = new AchillesIterator<>(meta, context, iterator);

        Whitebox.setInternalState(sliceIterator, "mapper", mapper);
        Whitebox.setInternalState(sliceIterator, "proxifier", proxifier);
    }

    @Test
    public void should_return_true_for_has_next() throws Exception {
        when(iterator.hasNext()).thenReturn(true);

        assertThat(sliceIterator.hasNext()).isTrue();
    }

    @Test
    public void should_return_false_for_has_next_when_no_more_data() throws Exception {
        when(iterator.hasNext()).thenReturn(false);
        assertThat(sliceIterator.hasNext()).isFalse();
    }

    @Test
    public void should_get_next_clustered_entity() throws Exception {
        ClusteredEntity entity = new ClusteredEntity();
        Row row = mock(Row.class);

        when(meta.<ClusteredEntity>getEntityClass()).thenReturn(ClusteredEntity.class);
        when(meta.forOperations().instanciate()).thenReturn(entity);

        when(iterator.next()).thenReturn(row);

        when(cqlInvoker.invokeOnRowForType(row, String.class, "name")).thenReturn("name1");

        when(context.duplicate(entity)).thenReturn(context);
        when(proxifier.buildProxyWithAllFieldsLoadedExceptCounters(entity, entityFacade)).thenReturn(entity);

        ClusteredEntity actual = sliceIterator.next();

        assertThat(actual).isSameAs(entity);
        verify(meta.forInterception()).intercept(entity, Event.POST_LOAD);
        verify(mapper).setNonCounterPropertiesToEntity(row, meta, entity);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void should_exception_when_calling_remove() throws Exception {
        sliceIterator.remove();
    }
}
