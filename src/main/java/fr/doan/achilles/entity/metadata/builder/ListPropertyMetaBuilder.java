package fr.doan.achilles.entity.metadata.builder;

import fr.doan.achilles.entity.metadata.ListLazyPropertyMeta;
import fr.doan.achilles.entity.metadata.ListPropertyMeta;

public class ListPropertyMetaBuilder<V> extends SimplePropertyMetaBuilder<V>
{

	public static <V> ListPropertyMetaBuilder<V> listPropertyMetaBuilder(Class<V> valueClass)
	{
		return new ListPropertyMetaBuilder<V>(valueClass);
	}

	public ListPropertyMetaBuilder(Class<V> valueClass) {
		super(valueClass);
	}

	@Override
	public ListPropertyMeta<V> build()
	{

		ListPropertyMeta<V> meta;
		if (this.lazy)
		{
			meta = new ListLazyPropertyMeta<V>();
		}
		else
		{
			meta = new ListPropertyMeta<V>();
		}

		super.build(meta);
		return meta;
	}
}
