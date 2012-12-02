package fr.doan.achilles.entity.type;

import java.io.Serializable;

public class KeyValueHolder implements Serializable
{

	public static final long serialVersionUID = 1L;

	private final Object key;
	private final Object value;

	public KeyValueHolder(Object key, Object value) {
		super();
		this.key = key;
		this.value = value;
	}

	public Object getKey()
	{
		return key;
	}

	public Object getValue()
	{
		return value;
	}
}
