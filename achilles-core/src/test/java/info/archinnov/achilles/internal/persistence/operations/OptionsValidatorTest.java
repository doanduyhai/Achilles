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

package info.archinnov.achilles.internal.persistence.operations;

import static info.archinnov.achilles.type.OptionsBuilder.ifConditions;
import static info.archinnov.achilles.type.OptionsBuilder.ifNotExists;
import static info.archinnov.achilles.type.OptionsBuilder.withTtl;
import static org.mockito.Mockito.verify;
import java.util.HashMap;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.Options;

@RunWith(MockitoJUnitRunner.class)
public class OptionsValidatorTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @InjectMocks
    private OptionsValidator optionsValidator;

    @Mock
    private EntityValidator entityValidator;

    private Map<Class<?>, EntityMeta> entityMetaMap = new HashMap<>();

    @Test
    public void should_validate_options_for_insert() throws Exception {
        //Given
        CompleteBean entity = new CompleteBean();

        //When
        optionsValidator.validateOptionsForUpsert(entity, entityMetaMap, withTtl(10));

        //Then
        verify(entityValidator).validateNotClusteredCounter(entity, entityMetaMap);
    }

    @Test
    public void should_exception_when_inserting_with_timestamp_and_cas_condition() throws Exception {
        //Given
        CompleteBean entity = new CompleteBean();

        exception.expect(AchillesException.class);
        exception.expectMessage("Cannot provide custom timestamp for CAS insert operations");

        //When
        optionsValidator.validateOptionsForUpsert(entity, entityMetaMap, ifNotExists().withTimestamp(100L));
    }


}
