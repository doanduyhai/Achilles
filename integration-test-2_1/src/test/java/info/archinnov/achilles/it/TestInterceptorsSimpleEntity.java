/*
 * Copyright (C) 2012-2021 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package info.archinnov.achilles.it;

import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.SimpleEntity_Manager;
import info.archinnov.achilles.internals.entities.SimpleEntity;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;
import info.archinnov.achilles.type.interceptor.Event;
import info.archinnov.achilles.type.interceptor.Interceptor;

public class TestInterceptorsSimpleEntity {


    public static Interceptor<SimpleEntity> preInsert = new Interceptor<SimpleEntity>() {
        @Override
        public boolean acceptEntity(Class<?> entityClass) {
            return entityClass.equals(SimpleEntity.class);
        }

        @Override
        public void onEvent(SimpleEntity entity, Event event) {
            entity.setValue("preInsert_" + entity.getValue());
        }

        @Override
        public List<Event> interceptOnEvents() {
            return asList(Event.PRE_INSERT);
        }
    };

    public static Interceptor<SimpleEntity> postInsert = new Interceptor<SimpleEntity>() {
        @Override
        public boolean acceptEntity(Class<?> entityClass) {
            return entityClass.equals(SimpleEntity.class);
        }

        @Override
        public void onEvent(SimpleEntity entity, Event event) {
            entity.setValue("postInsert_" + entity.getValue());
        }

        @Override
        public List<Event> interceptOnEvents() {
            return asList(Event.POST_INSERT);
        }
    };

    public static Interceptor<SimpleEntity> preUpdate = new Interceptor<SimpleEntity>() {
        @Override
        public boolean acceptEntity(Class<?> entityClass) {
            return entityClass.equals(SimpleEntity.class);
        }

        @Override
        public void onEvent(SimpleEntity entity, Event event) {
            entity.setValue("preUpdate_" + entity.getValue());
        }

        @Override
        public List<Event> interceptOnEvents() {
            return asList(Event.PRE_UPDATE);
        }
    };

    public static Interceptor<SimpleEntity> postUpdate = new Interceptor<SimpleEntity>() {
        @Override
        public boolean acceptEntity(Class<?> entityClass) {
            return entityClass.equals(SimpleEntity.class);
        }

        @Override
        public void onEvent(SimpleEntity entity, Event event) {
            entity.setValue("postUpdate_" + entity.getValue());
        }

        @Override
        public List<Event> interceptOnEvents() {
            return asList(Event.POST_UPDATE);
        }
    };

    public static Interceptor<SimpleEntity> preDelete = new Interceptor<SimpleEntity>() {
        @Override
        public boolean acceptEntity(Class<?> entityClass) {
            return entityClass.equals(SimpleEntity.class);
        }

        @Override
        public void onEvent(SimpleEntity entity, Event event) {
            entity.setValue("preDelete_" + entity.getValue());
        }

        @Override
        public List<Event> interceptOnEvents() {
            return asList(Event.PRE_DELETE);
        }
    };


    public static Interceptor<SimpleEntity> postDelete = new Interceptor<SimpleEntity>() {
        @Override
        public boolean acceptEntity(Class<?> entityClass) {
            return entityClass.equals(SimpleEntity.class);
        }

        @Override
        public void onEvent(SimpleEntity entity, Event event) {
            entity.setValue("postDelete_" + entity.getValue());
        }

        @Override
        public List<Event> interceptOnEvents() {
            return asList(Event.POST_DELETE);
        }
    };

    public static Interceptor<SimpleEntity> postLoad = new Interceptor<SimpleEntity>() {
        @Override
        public boolean acceptEntity(Class<?> entityClass) {
            return entityClass.equals(SimpleEntity.class);
        }

        @Override
        public void onEvent(SimpleEntity entity, Event event) {
            entity.setValue("postLoad_" + entity.getValue());
        }

        @Override
        public List<Event> interceptOnEvents() {
            return asList(Event.POST_LOAD);
        }
    };


    @Rule
    public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(SimpleEntity.class)
            .truncateBeforeAndAfterTest()
            .build((cluster, statementsCache) -> ManagerFactoryBuilder
                    .builder(cluster)
                    .withManagedEntityClasses(SimpleEntity.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                    .withEventInterceptors(asList(preInsert, postInsert, preUpdate, postUpdate, preDelete, postDelete, postLoad))
                    .build());

    private Session session = resource.getNativeSession();
    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();
    private SimpleEntity_Manager manager = resource.getManagerFactory().forSimpleEntity();

    @Test
    public void should_trigger_for_insert() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = new Date();
        final SimpleEntity entity = new SimpleEntity(id, date, "value");

        //When
        manager
                .crud()
                .insert(entity)
                .execute();

        //Then
        assertThat(entity.getValue()).isEqualTo("postInsert_preInsert_value");

        final List<Row> rows = session.execute("SELECT * FROM simple WHERE id = " + id).all();
        assertThat(rows).hasSize(1);

        final Row row = rows.get(0);
        assertThat(row.getString("value")).isEqualTo("preInsert_value");
    }

    @Test
    public void should_trigger_for_update() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();

        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        final SimpleEntity entity = new SimpleEntity(id, date, "value");

        //When
        manager
                .crud()
                .update(entity)
                .execute();

        //Then
        assertThat(entity.getValue()).isEqualTo("postUpdate_preUpdate_value");

        final List<Row> rows = session.execute("SELECT * FROM simple WHERE id = " + id).all();
        assertThat(rows).hasSize(1);

        final Row row = rows.get(0);
        assertThat(row.getString("value")).isEqualTo("preUpdate_value");
    }

    @Test
    public void should_trigger_for_find() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));
        final Date date = buildDateKey();

        //When
        final SimpleEntity actual = manager
                .crud()
                .findById(id, date)
                .get();

        //Then
        assertThat(actual).isNotNull();
        assertThat(actual.getValue()).isEqualTo("postLoad_0 AM");
    }

    @Test
    public void should_trigger_for_delete() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        final SimpleEntity entity = new SimpleEntity(id, date, "value");
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        manager.crud()
                .delete(entity)
                .execute();

        //Then
        assertThat(entity.getValue()).isEqualTo("postDelete_preDelete_value");
    }

    @Test
    public void should_trigger_for_dsl_select() throws Exception {
        //Given
        final Map<String, Object> values = new HashMap<>();
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        values.put("id", id);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        final Date date1 = dateFormat.parse("2015-10-01 00:00:00 GMT");
        final Date date9 = dateFormat.parse("2015-10-09 00:00:00 GMT");
        values.put("date1", "'2015-10-01 00:00:00+0000'");
        values.put("date2", "'2015-10-02 00:00:00+0000'");
        values.put("date3", "'2015-10-03 00:00:00+0000'");
        values.put("date4", "'2015-10-04 00:00:00+0000'");
        values.put("date5", "'2015-10-05 00:00:00+0000'");
        values.put("date6", "'2015-10-06 00:00:00+0000'");
        values.put("date7", "'2015-10-07 00:00:00+0000'");
        values.put("date8", "'2015-10-08 00:00:00+0000'");
        values.put("date9", "'2015-10-09 00:00:00+0000'");
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_many_rows.cql", values);


        //When
        final List<SimpleEntity> actuals = manager
                .dsl()
                .select()
                .consistencyList()
                .simpleSet()
                .simpleMap()
                .value()
                .simpleMap()
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .date().Gte_And_Lt(date1, date9)
                .getList();

        //Then
        assertThat(actuals).hasSize(8);
        assertThat(actuals.get(0).getValue()).isEqualTo("postLoad_id - date1");
        assertThat(actuals.get(1).getValue()).isEqualTo("postLoad_id - date2");
        assertThat(actuals.get(2).getValue()).isEqualTo("postLoad_id - date3");
        assertThat(actuals.get(3).getValue()).isEqualTo("postLoad_id - date4");
        assertThat(actuals.get(4).getValue()).isEqualTo("postLoad_id - date5");
        assertThat(actuals.get(5).getValue()).isEqualTo("postLoad_id - date6");
        assertThat(actuals.get(6).getValue()).isEqualTo("postLoad_id - date7");
        assertThat(actuals.get(7).getValue()).isEqualTo("postLoad_id - date8");
    }

    @Test
    public void should_trigger_for_dsl_iterator() throws Exception {
        //Given
        final Map<String, Object> values = new HashMap<>();
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        values.put("id", id);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        final Date date1 = dateFormat.parse("2015-10-01 00:00:00 GMT");
        final Date date9 = dateFormat.parse("2015-10-09 00:00:00 GMT");
        values.put("date1", "'2015-10-01 00:00:00+0000'");
        values.put("date2", "'2015-10-02 00:00:00+0000'");
        values.put("date3", "'2015-10-03 00:00:00+0000'");
        values.put("date4", "'2015-10-04 00:00:00+0000'");
        values.put("date5", "'2015-10-05 00:00:00+0000'");
        values.put("date6", "'2015-10-06 00:00:00+0000'");
        values.put("date7", "'2015-10-07 00:00:00+0000'");
        values.put("date8", "'2015-10-08 00:00:00+0000'");
        values.put("date9", "'2015-10-09 00:00:00+0000'");
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_many_rows.cql", values);


        //When
        final Iterator<SimpleEntity> actuals = manager
                .dsl()
                .select()
                .consistencyList()
                .simpleSet()
                .simpleMap()
                .value()
                .simpleMap()
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .date().Gte_And_Lt(date1, date9)
                .iterator();

        //Then
        assertThat(actuals.next().getValue()).isEqualTo("postLoad_id - date1");
        assertThat(actuals.next().getValue()).isEqualTo("postLoad_id - date2");
        assertThat(actuals.next().getValue()).isEqualTo("postLoad_id - date3");
        assertThat(actuals.next().getValue()).isEqualTo("postLoad_id - date4");
        assertThat(actuals.next().getValue()).isEqualTo("postLoad_id - date5");
        assertThat(actuals.next().getValue()).isEqualTo("postLoad_id - date6");
        assertThat(actuals.next().getValue()).isEqualTo("postLoad_id - date7");
        assertThat(actuals.next().getValue()).isEqualTo("postLoad_id - date8");
        assertThat(actuals.hasNext()).isFalse();
    }

    private Date buildDateKey() throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        return dateFormat.parse("2015-10-01 00:00:00 GMT");
    }
}
