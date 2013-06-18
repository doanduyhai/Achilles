package info.archinnov.achilles.entity.operations;

import net.sf.cglib.transform.impl.InterceptFieldCallback;

/**
 * MyFieldInterceptor
 * 
 * @author DuyHai DOAN
 * 
 */
public class MyFieldInterceptor implements InterceptFieldCallback
{

	public MyFieldInterceptor() {}

	@Override
	public int writeInt(Object obj, String name, int oldValue, int newValue)
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public char writeChar(Object obj, String name, char oldValue, char newValue)
	{
		System.out.println("writeChar called with : " + newValue + ", old value = " + oldValue);
		return 0;
	}

	@Override
	public byte writeByte(Object obj, String name, byte oldValue, byte newValue)
	{
		System.out.println("writeByte");
		return 0;
	}

	@Override
	public boolean writeBoolean(Object obj, String name, boolean oldValue, boolean newValue)
	{
		System.out.println("writeBoolean");
		return false;
	}

	@Override
	public short writeShort(Object obj, String name, short oldValue, short newValue)
	{
		System.out.println("writeShort");
		return 0;
	}

	@Override
	public float writeFloat(Object obj, String name, float oldValue, float newValue)
	{
		System.out.println("writeFloat");
		return 0;
	}

	@Override
	public double writeDouble(Object obj, String name, double oldValue, double newValue)
	{
		System.out.println("writeDouble");
		return 0;
	}

	@Override
	public long writeLong(Object obj, String name, long oldValue, long newValue)
	{
		System.out.println("writeLong");
		return 0;
	}

	@Override
	public Object writeObject(Object obj, String name, Object oldValue, Object newValue)
	{
		System.out.println("writeObject");
		System.out.println("writeObject called with : " + newValue + ", old value = " + oldValue);
		return newValue;
	}

	@Override
	public int readInt(Object obj, String name, int oldValue)
	{
		System.out.println("readInt");
		return 0;
	}

	@Override
	public char readChar(Object obj, String name, char oldValue)
	{
		System.out.println("readChar");
		return 0;
	}

	@Override
	public byte readByte(Object obj, String name, byte oldValue)
	{
		System.out.println("readByte");
		return 0;
	}

	@Override
	public boolean readBoolean(Object obj, String name, boolean oldValue)
	{
		System.out.println("readBoolean");
		return false;
	}

	@Override
	public short readShort(Object obj, String name, short oldValue)
	{
		System.out.println("readShort");
		return 0;
	}

	@Override
	public float readFloat(Object obj, String name, float oldValue)
	{
		System.out.println("readFloat");
		return 0;
	}

	@Override
	public double readDouble(Object obj, String name, double oldValue)
	{
		System.out.println("readDouble");
		return 0;
	}

	@Override
	public long readLong(Object obj, String name, long oldValue)
	{
		System.out.println("readLong");
		return 0;
	}

	@Override
	public Object readObject(Object obj, String name, Object oldValue)
	{
		System.out.println("readObject called");
		return oldValue;
	}

}
