package info.archinnov.achilles.entity.type;

/**
 * MultiKey
 * 
 * Marker interface for multi key
 * 
 * Example
 * 
 * <pre>
 * {
 * 	{@code
 * 	public class CorrectMultiKey implements MultiKey
 * 	{
 * 		@Key(order = 1)
 * 		private String name;
 * 
 * 		@Key(order = 2)
 * 		private int rank;
 * 	}
 * }
 * </pre>
 * 
 * @author DuyHai DOAN
 * 
 */
public interface MultiKey
{

}
