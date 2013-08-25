package info.archinnov.achilles.test.integration.tests;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.junit.AchillesInternalThriftResource;
import info.archinnov.achilles.proxy.wrapper.CounterBuilder;
import info.archinnov.achilles.proxy.wrapper.CounterBuilder.CounterImpl;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.integration.entity.Tweet;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Rule;
import org.junit.Test;

public class InitializeIT {

    @Rule
    public AchillesInternalThriftResource resource = new AchillesInternalThriftResource(Steps.AFTER_TEST,
            "CompleteBean", "Tweet");

    private ThriftEntityManager em = resource.getEm();

    @Test
    public void should_initialize_lazy_properties() throws Exception
    {
        Tweet tweet = new Tweet();
        tweet.setId(UUIDGen.getTimeUUID());
        tweet.setContent("welcome");

        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId()
                .name("name")
                .label("label")
                .age(45L)
                .addFriends("foo", "bar")
                .welcomeTweet(tweet)
                .buid();

        entity.setVersion(CounterBuilder.incr(11L));

        em.persist(entity);

        CompleteBean foundEntity = em.find(CompleteBean.class, entity.getId());

        CompleteBean rawEntity = em.initAndUnwrap(foundEntity);

        assertThat(rawEntity.getName()).isEqualTo("name");
        assertThat(rawEntity.getLabel()).isEqualTo("label");
        assertThat(rawEntity.getAge()).isEqualTo(45L);
        assertThat(rawEntity.getFriends()).containsExactly("foo", "bar");
        assertThat(rawEntity.getWelcomeTweet().getContent()).isEqualTo("welcome");
        assertThat(rawEntity.getVersion()).isInstanceOf(CounterImpl.class);
        assertThat(rawEntity.getVersion().get()).isEqualTo(11L);
    }

    @Test
    public void should_initialize_counter_value() throws Exception
    {
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId()
                .name("name").buid();

        entity = em.merge(entity);

        entity.getVersion().incr(2L);

        CompleteBean foundEntity = em.find(CompleteBean.class, entity.getId());

        CompleteBean rawEntity = em.initAndUnwrap(foundEntity);

        assertThat(rawEntity.getVersion()).isInstanceOf(CounterImpl.class);
        assertThat(rawEntity.getVersion().get()).isEqualTo(2L);
    }
}
