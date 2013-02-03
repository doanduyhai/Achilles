package info.archinnov.achilles.entity.metadata;

/**
 * PropertyType
 * 
 * @author DuyHai DOAN
 * 
 */
public enum PropertyType
{

	START_EAGER(0, false, false), //
	SERIAL_VERSION_UID(10, false, false), //
	SIMPLE(20, false, false), //
	LIST(30, false, false), //
	SET(40, false, false), //
	MAP(50, false, false), //
	END_EAGER(60, false, false), //
	LAZY_SIMPLE(70, true, false), //
	LAZY_LIST(70, true, false), //
	LAZY_SET(70, true, false), //
	LAZY_MAP(70, true, false), //
	WIDE_MAP(70, true, false), //
	EXTERNAL_WIDE_MAP(70, true, false), //
	JOIN_SIMPLE(70, true, true), //
	JOIN_WIDE_MAP(70, true, true), //
	EXTERNAL_JOIN_WIDE_MAP(70, true, true);

	private final int flag;
	private final boolean joinColumn;
	private final boolean lazy;

	PropertyType(int flag, boolean lazy, boolean joinColumn) {
		this.flag = flag;
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

	public boolean isLazy()
	{
		return lazy;
	}

	public boolean isJoinColumn()
	{
		return joinColumn;
	}
}
