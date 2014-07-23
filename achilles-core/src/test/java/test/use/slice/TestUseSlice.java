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

package test.use.slice;

import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder;
import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder.Sorting;
import static info.archinnov.achilles.type.ConsistencyLevel.ALL;
import static info.archinnov.achilles.type.ConsistencyLevel.LOCAL_QUORUM;
import static org.mockito.Mockito.when;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.persistence.operations.SliceQueryExecutor;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.query.slice.DeleteDSL;
import info.archinnov.achilles.query.slice.IterateDSL;
import info.archinnov.achilles.query.slice.SelectDSL;
import info.archinnov.achilles.query.slice.SliceQueryBuilder;

@RunWith(MockitoJUnitRunner.class)
public class TestUseSlice {

    @Mock
    private PersistenceManager manager;

    @Mock
    private SliceQueryExecutor executor;

    @Mock
    private EntityMeta entityMeta;

    @Before
    public void setUp() {
        when(entityMeta.getClusteringOrders()).thenReturn(Arrays.asList(new ClusteringOrder("col", Sorting.ASC)));
        when(entityMeta.getPartitionKeysSize()).thenReturn(2);
        when(entityMeta.getClusteringKeysSize()).thenReturn(3);

    }

    @Test
    public void should_get_from_partition_key() throws Exception {


       /*
        CREATE TABLE article_rating (
            article_id uuid,
            rating int, // from 1 to 10
            date int, //format YYYYMMDDHHMMSS
            login text,
            comment text,
            PRIMARY KEY(article_id,rating,date));
        */

        UUID articleId = UUID.randomUUID();
        final Date now = new Date();
        final Date tomorrow = new Date();

        List<ArticleRating> articleRatings = manager.sliceQuery(ArticleRating.class)
                .forSelect()
                .withPartitionComponents(articleId)
                .limit(20)
                .get();

        //SELECT * FROM article_rating WHERE article_id=... ORDER BY rating ASC LIMIT 20

        manager.sliceQuery(ArticleRating.class)
                .forSelect()
                .withPartitionComponents(articleId)
                .get(20);

        //SELECT * FROM article_rating WHERE article_id=... ORDER BY rating ASC LIMIT 20

        manager.sliceQuery(ArticleRating.class)
                .forSelect()
                .withPartitionComponents(articleId)
                .noLimit()
                .get();

        //SELECT * FROM article_rating WHERE article_id=... ORDER BY rating ASC

        manager.sliceQuery(ArticleRating.class)
                .forSelect()
                .withPartitionComponents(articleId)
                .getOne();

        //SELECT * FROM article_rating WHERE article_id=... ORDER BY rating ASC LIMIT 1

        manager.sliceQuery(ArticleRating.class)
                .forSelect()
                .withPartitionComponents(articleId)
                .orderByDescending()
                .getOne();

        //SELECT * FROM article_rating WHERE article_id=... ORDER BY rating DESC LIMIT 1

        manager.sliceQuery(ArticleRating.class)
                .forSelect()
                .withPartitionComponents(articleId)
                .getOneMatching(2,now);

        //SELECT * FROM article_rating WHERE article_id=... AND rating=2 AND date=... ORDER BY rating ASC LIMIT 1

        manager.sliceQuery(ArticleRating.class)
                .forSelect()
                .withPartitionComponents(articleId)
                .getFirstMatching(10,8, now);

        //SELECT * FROM article_rating WHERE article_id=... AND rating=8 AND date=... ORDER BY rating ASC LIMIT 10

        manager.sliceQuery(ArticleRating.class)
                .forSelect()
                .withPartitionComponents(articleId)
                .getLastMatching(10, 8, now);

        //SELECT * FROM article_rating WHERE article_id=... AND rating=8 AND date=... ORDER BY rating DESC LIMIT 10


        manager.sliceQuery(ArticleRating.class)
                .forSelect()
                .withPartitionComponents(articleId)
                .fromClusterings(2)
                .get(5);

        //SELECT * FROM article_rating WHERE article_id=... AND rating>=2 AND date=... ORDER BY rating ASC LIMIT 5


        manager.sliceQuery(ArticleRating.class)
                .forSelect()
                .withPartitionComponents(articleId)
                .fromClusterings(2, now)
                .fromExclusiveToInclusiveBounds()
                .orderByDescending()
                .get(5);

        //SELECT * FROM article_rating WHERE article_id=... AND (rating,date)>(2,...) ORDER BY rating DESC LIMIT 5


        manager.sliceQuery(ArticleRating.class)
                .forSelect()
                .withPartitionComponents(articleId)
                .fromClusterings(2, now)
                .toClusterings(4)
                .fromExclusiveToInclusiveBounds()
                .get(100);

        //SELECT * FROM article_rating WHERE article_id=... AND (rating,date)>(2,...) AND (rating)<=4 ORDER BY rating ASC LIMIT 5


        manager.sliceQuery(ArticleRating.class)
                .forSelect()
                .withPartitionComponents(articleId)
                .withClusterings(2)
                .get(12);

        //SELECT * FROM article_rating WHERE article_id=... AND rating=2 ORDER BY rating ASC LIMIT 12


        manager.sliceQuery(ArticleRating.class)
                .forSelect()
                .withPartitionComponents(articleId)
                .withClusterings(2)
                .andClusteringsIN(now,tomorrow)
                .get();

        //SELECT * FROM article_rating WHERE article_id=... AND rating=2 AND date IN (... , ...) ORDER BY rating ASC LIMIT 100


        /*************************************/


        final Iterator<ArticleRating> iterator = manager.sliceQuery(ArticleRating.class)
                .forIteration()
                .withPartitionComponents(articleId)
                .iterator(12);

        //SELECT * FROM article_rating WHERE article_id=... ORDER BY rating ASC LIMIT 100


        manager.sliceQuery(ArticleRating.class)
                .forIteration()
                .withPartitionComponents(articleId)
                .iteratorWithMatching(2);

        //SELECT * FROM article_rating WHERE article_id=... AND rating=2 ORDER BY rating ASC LIMIT 100


        manager.sliceQuery(ArticleRating.class)
                .forIteration()
                .withPartitionComponents(articleId)
                .fromClusterings(2, now)
                .toClusterings(4)
                .noLimit()
                .iterator(20);

        //SELECT * FROM article_rating WHERE article_id=... AND (rating,date)>=(2,...) AND (rating)<=4 ORDER BY rating ASC;


        /*************************************/

        manager.sliceQuery(ArticleRating.class)
                .forDelete()
                .withPartitionComponents(articleId)
                .delete();

        //DELETE FROM article_rating WHERE article_id=...

        manager.sliceQuery(ArticleRating.class)
                .forDelete()
                .withPartitionComponents(articleId)
                .deleteMatching(2);

        //DELETE FROM article_rating WHERE article_id=... AND rating=2


        manager.sliceQuery(ArticleRating.class)
                .forDelete()
                .withPartitionComponents(articleId)
                .deleteMatching(2,now);

        //DELETE FROM article_rating WHERE article_id=... AND rating=2 AND date=...
    }

    @Test
    public void should_test_composite() throws Exception {
        /*
            CREATE TABLE messages (
                user_id bigint,
                year int, // format YYYY
                message_id timeuuid,
                interlocutor_user_id bigint,
                interlocutor_user_name text,
                content text,
                PRIMARY KEY((user_id,year),message_id));
        */

        UUID february = UUID.randomUUID();
        UUID march = UUID.randomUUID();

        final Iterator<MessageEntity> iterator = manager.sliceQuery(MessageEntity.class)
                .forIteration()
                .withPartitionComponents(10L)
                .andPartitionComponentsIN(2013, 2014)
                .fromClusterings(february)
                .toClusterings(march)
                .fromInclusiveToExclusiveBounds()
                .noLimit()
                .iterator();

        //SELECT * FROM messages WHERE user_id=10 AND year IN (2013,2014) AND message_id>=february AND message_id<march ORDER BY message_id ASC;
    }

    @Test
    public void should_test_parameters() throws Exception {
        UUID articleId = UUID.randomUUID();

        manager.sliceQuery(ArticleRating.class)
                .forSelect()
                .withPartitionComponents(articleId)
                .withConsistency(LOCAL_QUORUM);


        manager.sliceQuery(ArticleRating.class)
                .forSelect()
                .withPartitionComponents(articleId)
                .withExclusiveBounds()
                .withInclusiveBounds()
                .fromExclusiveToInclusiveBounds()
                .fromInclusiveToExclusiveBounds();

        manager.sliceQuery(ArticleRating.class)
                .forSelect()
                .withPartitionComponents(articleId)
                .noLimit()
                .limit(10);

        manager.sliceQuery(ArticleRating.class)
                .forSelect()
                .withPartitionComponents(articleId)
                .orderByAscending()
                .orderByDescending();
    }


    @Test
    public void should_test_iterate() throws Exception {
        //Given
        final IterateDSL<String> builder = new SliceQueryBuilder<>(executor, String.class, entityMeta).forIteration();

        builder.withPartitionComponents("a")
                .andPartitionComponentsIN("A", "B")
                .limit(3)
                .fromClusterings("a", "b")
                .limit(5)
                .toClusterings("c", "d")
                .withConsistency(ALL);

        System.out.println("\n**************\n");

        builder.withPartitionComponents("a")
                .limit(3)
                .withClusterings("a", "b").andClusteringsIN("c", "d")
                .withConsistency(ALL)
                .orderByDescending();
    }

    @Test
    public void should_test_remove() throws Exception {

        final DeleteDSL<String> builder = new SliceQueryBuilder<>(executor, String.class, entityMeta).forDelete();

        builder.withPartitionComponents("a").andPartitionComponentsIN("A", "B");
    }

    @Test
    public void should_test() throws Exception {

        final Child<String> child = new Child(String.class);

        child.limit(3).doSomething().limit(2).thenSomething();

    }


}
