package fr.doan.achilles.entity.metadata;

public enum PropertyType
{
	META(0, false),
	START_EAGER(10, false),
	SIMPLE(20, false),
	LIST(30, true),
	SET(40, true),
	MAP(50, true),
	END_EAGER(60, false),
	LAZY_SIMPLE(70, true),
	LAZY_LIST(80, true),
	LAZY_SET(90, true),
	LAZY_MAP(100, true),
	WIDE_MAP(110, true),
	EXTERNAL_WIDE_MAP(120, true),
	JOIN_SIMPLE(130, false),
	JOIN_WIDE_MAP(140, true);

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
