package info.archinnov.achilles.entity.operations;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.proxy.EntityInterceptor;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;


import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

/**
 * AchillesEntityInitializerTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class EntityInitializerTest
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	private EntityInitializer initializer = new EntityInitializer();

	private final List<String> calledMethods = new ArrayList<String>();

	@Mock
	private EntityInterceptor<PersistenceContext, CompleteBean> interceptor;

	private CompleteBean bean = new CompleteBean()
	{
		

		@Override
		public Set<String> getFollowers()
		{
			calledMethods.add("getFollowers");
			return Sets.newHashSet("followers");
		}

	};

	@Test
	public void should_initialize_entity() throws Exception
	{

		Class<? extends CompleteBean> beanClass = bean.getClass();

		PropertyMeta<?, ?> nameMeta = new PropertyMeta<Void, String>();
		nameMeta.setEntityClassName("beanClass");
		nameMeta.setType(SIMPLE);
		nameMeta.setGetter(beanClass.getMethod("getName"));

		PropertyMeta<?, ?> friendsMeta = new PropertyMeta<Void, String>();
		friendsMeta.setEntityClassName("beanClass");
		friendsMeta.setType(LAZY_LIST);
		friendsMeta.setGetter(beanClass.getMethod("getFriends"));

		PropertyMeta<?, ?> followersMeta = new PropertyMeta<Void, String>();
		followersMeta.setEntityClassName("beanClass");
		followersMeta.setType(LAZY_SET);
		followersMeta.setGetter(beanClass.getMethod("getFollowers"));

		PropertyMeta<?, ?> tweetsMeta = new PropertyMeta<UUID, String>();
		tweetsMeta.setEntityClassName("beanClass");
		tweetsMeta.setType(WIDE_MAP);
		tweetsMeta.setGetter(beanClass.getMethod("getTweets"));

		Set<Method> alreadyLoaded = Sets.newHashSet(friendsMeta.getGetter(), nameMeta.getGetter());

		Map<Method, PropertyMeta<?, ?>> getterMetas = ImmutableMap.<Method, PropertyMeta<?, ?>> of(
				nameMeta.getGetter(), nameMeta,
				friendsMeta.getGetter(), friendsMeta,
				followersMeta.getGetter(), followersMeta,
				tweetsMeta.getGetter(), tweetsMeta);

		Map<String, PropertyMeta<?, ?>> allMetas = ImmutableMap.<String, PropertyMeta<?, ?>> of(
				"name", nameMeta,
				"friends", friendsMeta,
				"followers", followersMeta,
				"tweets", tweetsMeta);

		EntityMeta entityMeta = new EntityMeta();
		entityMeta.setPropertyMetas(allMetas);
		entityMeta.setGetterMetas(getterMetas);

		when(interceptor.getAlreadyLoaded()).thenReturn(alreadyLoaded);

		initializer.initializeEntity(bean, entityMeta, interceptor);

		assertThat(calledMethods).containsExactly("getFollowers");
	}

	@Test
	public void should_throw_exception_when_error_initializing() throws Exception
	{
		CompleteBean bean = new CompleteBean()
		{
			

			public Long getId()
			{
				throw new RuntimeException();
			}
		};

		PropertyMeta<Void, Long> pm = new PropertyMeta<Void, Long>();
		pm.setType(PropertyType.LAZY_SIMPLE);
		pm.setGetter(bean.getClass().getMethod("getId"));

		EntityMeta entityMeta = new EntityMeta();
		entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("id", pm));

		entityMeta.setGetterMetas(ImmutableMap.<Method, PropertyMeta<?, ?>> of(pm.getGetter(), pm));

		when(interceptor.getAlreadyLoaded()).thenReturn(new HashSet<Method>());

		exception.expect(AchillesException.class);
		initializer.initializeEntity(bean, entityMeta, interceptor);
	}

}
