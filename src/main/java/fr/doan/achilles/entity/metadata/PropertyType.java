package fr.doan.achilles.entity.metadata;

public enum PropertyType
{
	META(0),
	START_EAGER(1),
	SIMPLE(2),
	LIST(3),
	SET(4),
	MAP(5),
	END_EAGER(6),
	LAZY_SIMPLE(7),
	LAZY_LIST(8),
	LAZY_SET(9),
	LAZY_MAP(10),
	ITERATOR(11);

	private int flag;

	PropertyType(int flag) {
		this.flag = flag;
	}

	public byte[] flag()
	{
		return new byte[]
		{
			(byte) flag
		};
	}
}
