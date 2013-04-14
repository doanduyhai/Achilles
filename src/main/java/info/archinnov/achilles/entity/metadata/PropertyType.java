package info.archinnov.achilles.entity.metadata;

/**
 * PropertyType
 * 
 * @author DuyHai DOAN
 * 
 */
public enum PropertyType
{

	START_EAGER(0), //
	SERIAL_VERSION_UID(10), //
	SIMPLE(20), //
	LIST(30), //
	SET(40), //
	MAP(50), //
	END_EAGER(60), //
	LAZY_SIMPLE(70), //
	COUNTER(70), //
	LAZY_LIST(70), //
	LAZY_SET(70), //
	LAZY_MAP(70), //
	WIDE_MAP(70), //
	WIDE_MAP_COUNTER(70), //
	JOIN_SIMPLE(70), //
	JOIN_LIST(70), //
	JOIN_SET(70), //
	JOIN_MAP(70), //
	JOIN_WIDE_MAP(70);

	private final int flag;

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

	public boolean isLazy()
	{
		return (this == COUNTER //
				|| this == LAZY_SIMPLE //
				|| this == LAZY_LIST //
				|| this == LAZY_SET //
				|| this == LAZY_MAP //
				|| this == WIDE_MAP //
				|| this == WIDE_MAP_COUNTER //
				|| this == JOIN_SIMPLE //
				|| this == JOIN_LIST //
				|| this == JOIN_SET //
				|| this == JOIN_MAP //
		|| this == JOIN_WIDE_MAP);
	}

	public boolean isJoinColumn()
	{
		return (this == JOIN_SIMPLE //
				|| this == JOIN_LIST //
				|| this == JOIN_SET //
				|| this == JOIN_MAP //
		|| this == JOIN_WIDE_MAP);
	}

	public boolean isWideMap()
	{
		return (this == WIDE_MAP //
				|| this == WIDE_MAP_COUNTER //
		|| this == JOIN_WIDE_MAP);
	}
}
