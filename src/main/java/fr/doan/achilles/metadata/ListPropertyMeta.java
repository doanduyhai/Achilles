package fr.doan.achilles.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import fr.doan.achilles.validation.Validator;

@SuppressWarnings("rawtypes")
public class ListPropertyMeta<V extends Serializable> extends SimplePropertyMeta<V>
{

	private Class<? extends List> listClass;

	public ListPropertyMeta(String name, Class<V> valueClazz, Class<? extends List> listClass) {
		super(name, valueClazz);
		Validator.validateNotNull(listClass, "listClazz");
		Validator.validateNoargsConstructor(listClass);
		if (listClass == List.class)
		{
			this.listClass = ArrayList.class;
		}
		else
		{
			this.listClass = listClass;
		}
	}

	@SuppressWarnings("unchecked")
	public List<V> newListInstance() throws InstantiationException, IllegalAccessException
	{
		return this.listClass.newInstance();
	}

	@Override
	public PropertyType propertyType()
	{
		return PropertyType.LIST;
	}
}
