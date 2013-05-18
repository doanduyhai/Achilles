package info.archinnov.achilles.wrapper;

import info.archinnov.achilles.wrapper.builder.ListIteratorWrapperBuilder;
import info.archinnov.achilles.wrapper.builder.ListWrapperBuilder;

import java.util.Collection;
import java.util.List;
import java.util.ListIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ListWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class ListWrapper<V> extends CollectionWrapper<V> implements List<V>
{
	private static final Logger log = LoggerFactory.getLogger(ListWrapper.class);

	public ListWrapper(List<V> target) {
		super(target);
	}

	@Override
	public void add(int index, V arg1)
	{
		log.trace(
				"Mark list property {} of entity class {} dirty upon element addition at index {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName(), index);
		((List<V>) super.target).add(index, proxifier.unproxy(arg1));
		super.markDirty();
	}

	@Override
	public boolean addAll(int arg0, Collection<? extends V> arg1)
	{
		boolean result = ((List<V>) super.target).addAll(arg0, proxifier.unproxy(arg1));
		if (result)
		{
			log.trace("Mark list property {} of entity class {} dirty upon elements addition",
					propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
			super.markDirty();
		}
		return result;
	}

	@Override
	public V get(int index)
	{
		log.trace("Return element at index {} for list property {} of entity class {}", index,
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		V result = ((List<V>) super.target).get(index);
		if (isJoin())
		{
			return proxifier.buildProxy(result, joinContext(result));
		}
		else
		{
			return result;
		}
	}

	@Override
	public int indexOf(Object arg0)
	{
		return ((List<V>) super.target).indexOf(arg0);
	}

	@Override
	public int lastIndexOf(Object arg0)
	{
		return ((List<V>) super.target).lastIndexOf(arg0);
	}

	@Override
	public ListIterator<V> listIterator()
	{
		ListIterator<V> target = ((List<V>) super.target).listIterator();

		log.trace("Build iterator wrapper for list property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());

		return ListIteratorWrapperBuilder //
				.builder(context, target) //
				.dirtyMap(dirtyMap) //
				.setter(setter) //
				.propertyMeta(propertyMeta) //
				.proxifier(proxifier) //
				.build();
	}

	@Override
	public ListIterator<V> listIterator(int index)
	{
		ListIterator<V> target = ((List<V>) super.target).listIterator(index);

		log.trace("Build iterator wrapper for list property {} of entity class {} at index {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName(), index);

		return ListIteratorWrapperBuilder //
				.builder(context, target) //
				.dirtyMap(dirtyMap) //
				.setter(setter) //
				.propertyMeta(propertyMeta) //
				.proxifier(proxifier) //
				.build();
	}

	@Override
	public V remove(int index)
	{
		log.trace(
				"Mark list property {} of entity class {} dirty upon element removal at index {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName(), index);

		V result = ((List<V>) super.target).remove(index);
		super.markDirty();
		return result;
	}

	@Override
	public V set(int index, V arg1)
	{
		log.trace("Mark list property {} of entity class {} dirty upon element set at index {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());

		V result = ((List<V>) super.target).set(index, proxifier.unproxy(arg1));
		super.markDirty();
		return result;
	}

	@Override
	public List<V> subList(int from, int to)
	{
		List<V> target = ((List<V>) super.target).subList(from, to);

		log.trace(
				"Build sublist wrapper for list property {} of entity class {} between index {} and {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName(), from, to);

		return ListWrapperBuilder //
				.builder(context, target) //
				.dirtyMap(dirtyMap) //
				.setter(setter) //
				.propertyMeta(propertyMeta) //
				.proxifier(proxifier) //
				.build();
	}

	@Override
	public List<V> getTarget()
	{
		return ((List<V>) super.target);
	}

}
