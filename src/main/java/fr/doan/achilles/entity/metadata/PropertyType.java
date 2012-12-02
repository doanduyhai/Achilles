package fr.doan.achilles.entity.metadata;

public enum PropertyType
{
	META(0, false),
	START_EAGER(1, false),
	SIMPLE(2, false),
	LIST(3, true),
	SET(4, true),
	MAP(5, true),
	END_EAGER(6, false),
	LAZY_SIMPLE(7, true),
	LAZY_LIST(8, true),
	LAZY_SET(9, true),
	LAZY_MAP(10, true),
	WIDE_MAP(11, true);

	private int flag;
	private boolean multiValue;

	PropertyType(int flag, boolean multiValue) {
		this.flag = flag;
		this.multiValue = multiValue;
	}

	public byte[] flag()
	{
		return new byte[]
		{
			(byte) flag
		};
	}

	public boolean isMultiValue()
	{
		return multiValue;
	}
}
