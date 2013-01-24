package fr.doan.achilles.entity.metadata;

/**
 * PropertyType
 * 
 * @author DuyHai DOAN
 * 
 */
public enum PropertyType
{

	START_EAGER(0, false, false, false), //
	SERIAL_VERSION_UID(10, false, false, false), //
	SIMPLE(20, false, false, false), //
	LIST(30, true, false, false), //
	SET(40, true, false, false), //
	MAP(50, true, false, false), //
	END_EAGER(60, false, false, false), //
	LAZY_SIMPLE(70, false, true, false), //
	LAZY_LIST(70, true, true, false), //
	LAZY_SET(70, true, true, false), //
	LAZY_MAP(70, true, true, false), //
	WIDE_MAP(70, true, true, false), //
	EXTERNAL_WIDE_MAP(70, true, true, false), //
	JOIN_SIMPLE(70, false, true, true), //
	JOIN_WIDE_MAP(70, true, true, true), //
	EXTERNAL_JOIN_WIDE_MAP(70, true, true, true);

	private final int flag;
	private final boolean multiValue;
	private final boolean joinColumn;
	private final boolean lazy;

	PropertyType(int flag, boolean multiValue, boolean lazy, boolean joinColumn) {
		this.flag = flag;
		this.multiValue = multiValue;
		this.lazy = lazy;
		this.joinColumn = joinColumn;
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

	public boolean isLazy()
	{
		return lazy;
	}

	public boolean isJoinColumn()
	{
		return joinColumn;
	}
}
