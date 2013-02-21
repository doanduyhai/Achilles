package info.archinnov.achilles.entity.metadata;

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
	LIST(30, false, false, false), //
	SET(40, false, false, false), //
	MAP(50, false, false, false), //
	END_EAGER(60, false, false, false), //
	LAZY_SIMPLE(70, true, false, false), //
	LAZY_LIST(70, true, false, false), //
	LAZY_SET(70, true, false, false), //
	LAZY_MAP(70, true, false, false), //
	WIDE_MAP(70, true, false, false), //
	EXTERNAL_WIDE_MAP(70, true, false, true), //
	JOIN_SIMPLE(70, true, true, false), //
	JOIN_LIST(70, true, true, false), //
	JOIN_SET(70, true, true, false), //
	JOIN_MAP(70, true, true, false), //
	JOIN_WIDE_MAP(70, true, true, false), //
	EXTERNAL_JOIN_WIDE_MAP(70, true, true, true);

	private final int flag;
	private final boolean joinColumn;
	private final boolean lazy;
	private final boolean external;

	PropertyType(int flag, boolean lazy, boolean joinColumn, boolean external) {
		this.flag = flag;
		this.lazy = lazy;
		this.joinColumn = joinColumn;
		this.external = external;
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

	public boolean isExternal()
	{
		return external;
	}
}
