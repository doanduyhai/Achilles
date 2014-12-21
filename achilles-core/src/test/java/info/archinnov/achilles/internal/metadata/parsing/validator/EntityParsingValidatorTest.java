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
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class EntityParsingValidatorTest {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	private EntityParsingValidator validator = new EntityParsingValidator();

	@Test
	public void should_exception_when_no_id_meta() throws Exception {
		exception.expect(AchillesBeanMappingException.class);
		exception.expectMessage("The entity '" + CompleteBean.class.getCanonicalName()
				+ "' should have at least one field with info.archinnov.achilles.annotations.PartitionKey/info.archinnov.achilles.annotations.CompoundPrimaryKey annotation");
		validator.validateHasIdMeta(CompleteBean.class, null);
	}

    @Test
    public void should_exception_when_static_column_on_non_clustered_entity() throws Exception {
        //Given
        PropertyMeta idMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        PropertyMeta pm = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        EntityMeta entityMeta = mock(EntityMeta.class, RETURNS_DEEP_STUBS);
        when(entityMeta.getPropertyMetas().values()).thenReturn(Arrays.asList(pm));
        when(pm.structure().isStaticColumn()).thenReturn(true);
        when(entityMeta.getClassName()).thenReturn("myEntity");
        when(idMeta.structure().isClustered()).thenReturn(false);

        //When //Then
        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage("The entity class 'myEntity' cannot have a static column because it does not declare any clustering column");

        validator.validateStaticColumns(entityMeta, idMeta);
    }

    @Test
    public void should_exception_when_clustered_counter_entity_has_only_static_columns() throws Exception {
        //Given
        PropertyMeta idMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        PropertyMeta pm1 = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        PropertyMeta pm2 = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        EntityMeta entityMeta = mock(EntityMeta.class, RETURNS_DEEP_STUBS);
        when(entityMeta.getPropertyMetas().values()).thenReturn(Arrays.asList(pm1,pm2));
        when(entityMeta.getAllMetasExceptId().size()).thenReturn(2);
        when(entityMeta.getClassName()).thenReturn("myEntity");
        when(entityMeta.structure().isClusteredCounter()).thenReturn(true);
        when(idMeta.structure().isClustered()).thenReturn(true);
        when(pm1.structure().isStaticColumn()).thenReturn(true);
        when(pm2.structure().isStaticColumn()).thenReturn(true);
        when(pm1.structure().isCounter()).thenReturn(true);
        when(pm2.structure().isCounter()).thenReturn(true);

        //When //Then
        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage("The entity class 'myEntity' is a clustered counter and thus cannot have only static counter column");

        validator.validateStaticColumns(entityMeta, idMeta);
    }
}
