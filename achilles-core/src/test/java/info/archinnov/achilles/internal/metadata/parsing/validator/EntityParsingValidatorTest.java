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
package info.archinnov.achilles.internal.metadata.parsing.validator;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.Arrays;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.parsing.context.EntityParsingContext;
import info.archinnov.achilles.internal.metadata.parsing.validator.EntityParsingValidator;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Answers;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

public class EntityParsingValidatorTest {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	private EntityParsingValidator validator = new EntityParsingValidator();

	@Test
	public void should_exception_when_no_id_meta() throws Exception {
		exception.expect(AchillesBeanMappingException.class);
		exception.expectMessage("The entity '" + CompleteBean.class.getCanonicalName()
				+ "' should have at least one field with info.archinnov.achilles.annotations.Id/info.archinnov.achilles.annotations.EmbeddedId annotation");
		validator.validateHasIdMeta(CompleteBean.class, null);
	}

    @Test
    public void should_exception_when_static_column_on_non_clustered_entity() throws Exception {
        //Given
        PropertyMeta idMeta = mock(PropertyMeta.class);
        PropertyMeta pm = mock(PropertyMeta.class);
        EntityMeta entityMeta = mock(EntityMeta.class, RETURNS_DEEP_STUBS);
        when(entityMeta.getPropertyMetas().values()).thenReturn(Arrays.asList(pm));
        when(entityMeta.getClassName()).thenReturn("myEntity");
        when(idMeta.isClustered()).thenReturn(false);
        when(pm.isStaticColumn()).thenReturn(true);

        //When //Then
        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage("The entity class 'myEntity' cannot have a static column because it does not declare any clustering column");

        validator.validateStaticColumns(entityMeta, idMeta);
    }

    @Test
    public void should_exception_when_static_counter_column_on_non_clustered_counter_entity() throws Exception {
        //Given
        PropertyMeta idMeta = mock(PropertyMeta.class);
        PropertyMeta pm = mock(PropertyMeta.class);
        EntityMeta entityMeta = mock(EntityMeta.class, RETURNS_DEEP_STUBS);
        when(entityMeta.getPropertyMetas().values()).thenReturn(Arrays.asList(pm));
        when(entityMeta.getClassName()).thenReturn("myEntity");
        when(entityMeta.isClusteredCounter()).thenReturn(false);
        when(idMeta.isClustered()).thenReturn(true);
        when(pm.isStaticColumn()).thenReturn(true);
        when(pm.isCounter()).thenReturn(true);

        //When //Then
        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage("The entity class 'myEntity' cannot have a static counter column if it is not a clustered entity with only counter properties");

        validator.validateStaticColumns(entityMeta, idMeta);
    }
}
