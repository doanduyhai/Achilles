package fr.doan.achilles.entity.metadata;

/**
 * PropertyType
 * 
 * @author DuyHai DOAN
 * 
 */
public enum PropertyType
{

	START_EAGER(0, false), //
	SERIAL_VERSION_UID(10, false), //
	SIMPLE(20, false), //
	LIST(30, true), //
	SET(40, true), //
	MAP(50, true), //
	END_EAGER(60, false), //
	LAZY_SIMPLE(70, false), //
	LAZY_LIST(70, true), //
	LAZY_SET(70, true), //
	LAZY_MAP(70, true), //
	WIDE_MAP(70, true), //
	EXTERNAL_WIDE_MAP(70, true), //
	JOIN_SIMPLE(70, false), //
	JOIN_WIDE_MAP(70, true), //
	EXTERNAL_JOIN_WIDE_MAP(70, true);

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
