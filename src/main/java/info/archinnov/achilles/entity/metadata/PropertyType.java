package info.archinnov.achilles.entity.metadata;

/**
 * PropertyType
 * 
 * @author DuyHai DOAN
 * 
 */
public enum PropertyType
{

	START_EAGER(0, false, false, false, false), //
	SERIAL_VERSION_UID(10, false, false, false, false), //
	SIMPLE(20, false, false, false, false), //
	LIST(30, false, false, false, false), //
	SET(40, false, false, false, false), //
	MAP(50, false, false, false, false), //
	END_EAGER(60, false, false, false, false), //
	LAZY_SIMPLE(70, true, false, false, false), //
	LAZY_LIST(70, true, false, false, false), //
	LAZY_SET(70, true, false, false, false), //
	LAZY_MAP(70, true, false, false, false), //
	WIDE_MAP(70, true, false, false, true), //
	EXTERNAL_WIDE_MAP(70, true, false, true, true), //
	JOIN_SIMPLE(70, true, true, false, false), //
	COUNTER(70, true, true, true, false), //
	JOIN_LIST(70, true, true, false, false), //
	JOIN_SET(70, true, true, false, false), //
	JOIN_MAP(70, true, true, false, false), //
	JOIN_WIDE_MAP(70, true, true, false, true), //
	EXTERNAL_JOIN_WIDE_MAP(70, true, true, true, true);

	private final int flag;
	private final boolean joinColumn;
	private final boolean lazy;
	private final boolean external;
	private final boolean wideMap;

	PropertyType(int flag, boolean lazy, boolean joinColumn, boolean external, boolean wideMap) {
		this.flag = flag;
		this.lazy = lazy;
		this.joinColumn = joinColumn;
		this.external = external;
		this.wideMap = wideMap;
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

	public boolean isWideMap()
	{
		return wideMap;
	}
}
