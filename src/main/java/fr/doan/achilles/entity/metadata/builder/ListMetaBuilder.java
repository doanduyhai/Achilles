package fr.doan.achilles.entity.metadata.builder;

import fr.doan.achilles.entity.metadata.ListLazyMeta;
import fr.doan.achilles.entity.metadata.ListMeta;

public class ListMetaBuilder<V> extends SimpleMetaBuilder<V>
{

	public static <V> ListMetaBuilder<V> listMetaBuilder(Class<V> valueClass)
	{
		return new ListMetaBuilder<V>(valueClass);
	}

	public ListMetaBuilder(Class<V> valueClass) {
		super(valueClass);
	}

	@Override
	public ListMeta<V> build()
	{

		ListMeta<V> meta;
		if (this.lazy)
		{
			meta = new ListLazyMeta<V>();
		}
		else
		{
			meta = new ListMeta<V>();
		}

		super.build(meta);
		return meta;
	}
}
