package info.archinnov.achilles.wrapper;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.entity.context.AchillesPersistenceContext;
import info.archinnov.achilles.entity.manager.CompleteBeanTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.AchillesEntityProxifier;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import mapping.entity.CompleteBean;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.PropertyMetaTestBuilder;

/**
 * CollectionWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class CollectionWrapperTest
{
	@Mock
	private Map<Method, PropertyMeta<?, ?>> dirtyMap;

	private Method setter;

	@Mock
	private PropertyMeta<Void, String> propertyMeta;

	@Mock
	private PropertyMeta<Void, CompleteBean> joinPropertyMeta;

	@Mock
	private AchillesEntityProxifier proxifier;

	@Mock
	private AchillesConsistencyLevelPolicy policy;

	@Mock
	private AchillesPersistenceContext context;

	@Mock
	private AchillesPersistenceContext joinContext;

	private EntityMeta entityMeta;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	@Before
	public void setUp() throws Exception
	{
		setter = CompleteBean.class.getDeclaredMethod("setFriends", List.class);
		when(propertyMeta.type()).thenReturn(PropertyType.LIST);

		PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, Long.class)
				.field("id")
				.type(PropertyType.SIMPLE)
				.accessors()
				.build();

		entityMeta = new EntityMeta();
		entityMeta.setIdMeta(idMeta);
	}

	@Test
	public void should_mark_dirty_on_element_add() throws Exception
	{
		ArrayList<String> target = new ArrayList<String>();
		ListWrapper<String> wrapper = prepareListWrapper(target);
		when(proxifier.unproxy("a")).thenReturn("a");
		wrapper.add("a");

		assertThat(target).hasSize(1);
		assertThat(target.get(0)).isEqualTo("a");

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_on_element_add() throws Exception
	{
		ArrayList<String> target = new ArrayList<String>();
		ListWrapper<String> wrapper = prepareListWrapper(target);
		when(proxifier.unproxy("a")).thenReturn("a");
		when(dirtyMap.containsKey(setter)).thenReturn(true);
		wrapper.add("a");

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_add_all() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		ListWrapper<String> wrapper = prepareListWrapper(target);
		Collection<String> list = Arrays.asList("a", "b");

		wrapper.setProxifier(proxifier);

		when(proxifier.unproxy(any(Collection.class))).thenReturn((Collection) list);

		wrapper.addAll(list);

		verify(proxifier).unproxy(list);

		assertThat(target).hasSize(2);
		assertThat(target.get(0)).isEqualTo("a");
		assertThat(target.get(1)).isEqualTo("b");

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_on_empty_add_all() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		ListWrapper<String> wrapper = prepareListWrapper(target);
		wrapper.addAll(new ArrayList<String>());

		assertThat(target).hasSize(0);

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_clear() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		ListWrapper<String> wrapper = prepareListWrapper(target);
		wrapper.clear();

		assertThat(target).hasSize(0);

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_on_clear_when_empty() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		ListWrapper<String> wrapper = prepareListWrapper(target);
		wrapper.clear();

		assertThat(target).hasSize(0);

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_return_true_on_contains() throws Exception
	{
		ListWrapper<String> wrapper = prepareListWrapper(Arrays.asList("a", "b"));
		when(proxifier.unproxy("a")).thenReturn("a");
		assertThat(wrapper.contains("a")).isTrue();
	}

	@Test
	public void should_return_true_on_contains_all() throws Exception
	{
		ListWrapper<String> wrapper = prepareListWrapper(Arrays.asList("a", "b", "c", "d"));

		List<String> check = Arrays.asList("a", "c");
		when(proxifier.unproxy(check)).thenReturn(check);
		assertThat(wrapper.containsAll(check)).isTrue();
	}

	@Test
	public void should_return_true_on_empty_target() throws Exception
	{
		ListWrapper<String> wrapper = prepareListWrapper(new ArrayList<String>());
		assertThat(wrapper.isEmpty()).isTrue();
	}

	@Test
	public void should_mark_dirty_on_remove() throws Exception
	{
		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		ListWrapper<String> wrapper = prepareListWrapper(target);
		when(proxifier.unproxy("a")).thenReturn("a");
		wrapper.remove("a");

		assertThat(target).hasSize(1);
		assertThat(target.get(0)).isEqualTo("b");

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_on_remove_when_no_match() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		ListWrapper<String> wrapper = prepareListWrapper(target);
		wrapper.remove("c");

		assertThat(target).hasSize(2);
		assertThat(target.get(0)).isEqualTo("a");
		assertThat(target.get(1)).isEqualTo("b");

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_remove_all() throws Exception
	{

		List<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListWrapper<String> wrapper = prepareListWrapper(target);
		wrapper.setProxifier(proxifier);

		Collection<String> list = Arrays.asList("a", "c");
		when(proxifier.unproxy(any(Collection.class))).thenReturn((Collection) list);

		wrapper.removeAll(list);

		assertThat(target).hasSize(1);
		assertThat(target.get(0)).isEqualTo("b");

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_on_remove_all_when_no_match() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListWrapper<String> wrapper = prepareListWrapper(target);
		wrapper.removeAll(Arrays.asList("d", "e"));

		assertThat(target).hasSize(3);
		assertThat(target.get(0)).isEqualTo("a");
		assertThat(target.get(1)).isEqualTo("b");
		assertThat(target.get(2)).isEqualTo("c");

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_retain_all() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListWrapper<String> wrapper = prepareListWrapper(target);
		wrapper.setProxifier(proxifier);
		Collection<String> list = Arrays.asList("a", "c");
		when(proxifier.unproxy(any(Collection.class))).thenReturn((Collection) list);

		wrapper.retainAll(list);

		assertThat(target).hasSize(2);
		assertThat(target.get(0)).isEqualTo("a");
		assertThat(target.get(1)).isEqualTo("c");

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_on_retain_all_when_all_match() throws Exception
	{

		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListWrapper<String> wrapper = prepareListWrapper(target);
		wrapper.setProxifier(proxifier);
		Collection<String> list = Arrays.asList("a", "b", "c");
		when(proxifier.unproxy(any(Collection.class))).thenReturn((Collection) list);

		wrapper.retainAll(list);

		assertThat(target).hasSize(3);
		assertThat(target.get(0)).isEqualTo("a");
		assertThat(target.get(1)).isEqualTo("b");
		assertThat(target.get(2)).isEqualTo("c");

		verify(dirtyMap, never()).put(setter, propertyMeta);
	}

	@Test
	public void should_mark_dirty_on_iterator_remove() throws Exception
	{
		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListWrapper<String> wrapper = prepareListWrapper(target);

		Iterator<String> iteratorWrapper = wrapper.iterator();

		assertThat(iteratorWrapper).isInstanceOf(IteratorWrapper.class);

		iteratorWrapper.next();
		iteratorWrapper.remove();

		verify(dirtyMap).put(setter, propertyMeta);
	}

	@Test
	public void should_return_size() throws Exception
	{
		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListWrapper<String> wrapper = prepareListWrapper(target);
		assertThat(wrapper.size()).isEqualTo(3);
	}

	@Test
	public void should_return_array_for_join() throws Exception
	{
		ArrayList<CompleteBean> target = new ArrayList<CompleteBean>();
		CompleteBean bean1 = CompleteBeanTestBuilder.builder().randomId().buid();
		CompleteBean bean2 = CompleteBeanTestBuilder.builder().randomId().buid();
		CompleteBean bean3 = CompleteBeanTestBuilder.builder().randomId().buid();

		target.add(bean1);
		target.add(bean2);
		target.add(bean3);
		ListWrapper<CompleteBean> wrapper = prepareJoinListWrapper(target);

		when(joinPropertyMeta.type()).thenReturn(PropertyType.JOIN_LIST);
		when(joinPropertyMeta.joinMeta()).thenReturn(entityMeta);

		when(context.newPersistenceContext(eq(entityMeta), any())).thenReturn(joinContext);

		when(proxifier.buildProxy(bean1, joinContext)).thenReturn(bean1);
		when(proxifier.buildProxy(bean2, joinContext)).thenReturn(bean2);
		when(proxifier.buildProxy(bean3, joinContext)).thenReturn(bean3);

		assertThat(wrapper.toArray()).contains(bean1, bean2, bean3);
	}

	@Test
	public void should_return_array() throws Exception
	{
		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListWrapper<String> wrapper = prepareListWrapper(target);

		when(propertyMeta.type()).thenReturn(PropertyType.LIST);
		assertThat(wrapper.toArray()).contains("a", "b", "c");
	}

	@Test
	public void should_return_array_with_argument_for_join() throws Exception
	{
		ArrayList<CompleteBean> target = new ArrayList<CompleteBean>();
		CompleteBean bean1 = CompleteBeanTestBuilder.builder().randomId().buid();
		CompleteBean bean2 = CompleteBeanTestBuilder.builder().randomId().buid();
		CompleteBean bean3 = CompleteBeanTestBuilder.builder().randomId().buid();

		target.add(bean1);
		target.add(bean2);
		target.add(bean3);
		ListWrapper<CompleteBean> wrapper = prepareJoinListWrapper(target);

		when(joinPropertyMeta.type()).thenReturn(PropertyType.JOIN_LIST);
		when(joinPropertyMeta.joinMeta()).thenReturn(entityMeta);

		when(context.newPersistenceContext(eq(entityMeta), any())).thenReturn(joinContext);

		when(proxifier.buildProxy(bean1, joinContext)).thenReturn(bean1);
		when(proxifier.buildProxy(bean2, joinContext)).thenReturn(bean2);
		when(proxifier.buildProxy(bean3, joinContext)).thenReturn(bean3);

		assertThat(wrapper.toArray()).contains(bean1, bean2, bean3);

		assertThat(wrapper.toArray(new CompleteBean[]
		{
				bean1,
				bean2
		})).contains(bean1, bean2);
	}

	@Test
	public void should_return_array_with_argument() throws Exception
	{
		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		target.add("b");
		target.add("c");
		ListWrapper<String> wrapper = prepareListWrapper(target);

		when(propertyMeta.type()).thenReturn(PropertyType.LIST);
		assertThat(wrapper.toArray(new String[]
		{
				"a",
				"c"
		})).contains("a", "c");
	}

	@Test
	public void should_return_target() throws Exception
	{
		ArrayList<String> target = new ArrayList<String>();
		target.add("a");
		CollectionWrapper<String> wrapper = new CollectionWrapper<String>(target);
		assertThat(wrapper.getTarget()).isSameAs(target);
	}

	private ListWrapper<String> prepareListWrapper(List<String> target)
	{
		ListWrapper<String> wrapper = new ListWrapper<String>(target);
		wrapper.setDirtyMap(dirtyMap);
		wrapper.setSetter(setter);
		wrapper.setPropertyMeta(propertyMeta);
		wrapper.setProxifier(proxifier);
		wrapper.setContext(context);
		return wrapper;
	}

	private ListWrapper<CompleteBean> prepareJoinListWrapper(List<CompleteBean> target)
	{
		ListWrapper<CompleteBean> wrapper = new ListWrapper<CompleteBean>(target);
		wrapper.setDirtyMap(dirtyMap);
		wrapper.setSetter(setter);
		wrapper.setPropertyMeta(joinPropertyMeta);
		wrapper.setProxifier(proxifier);
		wrapper.setContext(context);
		return wrapper;
	}

}
