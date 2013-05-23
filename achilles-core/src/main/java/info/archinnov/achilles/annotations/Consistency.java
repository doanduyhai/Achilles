package info.archinnov.achilles.annotations;

import info.archinnov.achilles.type.ConsistencyLevel;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Consistency
 * 
 * @author DuyHai DOAN
 * 
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(
{
		ElementType.TYPE,
		ElementType.FIELD
})
@Documented
/**
 * <p>
 * Define the consistency level for an Entity or External Wide Map
 * For ThriftEntityManager, the consistency level applies on all the fields on the entity, except join fields and external wide map field
 * </p>
 */
public @interface Consistency
{
	/**
	 * <p>
	 * Consistency level for read operations
	 * </p>
	 */
	ConsistencyLevel read() default ConsistencyLevel.ONE;

	/**
	 * <p>
	 * Consistency level for write operations
	 * </p>
	 */
	ConsistencyLevel write() default ConsistencyLevel.ONE;
}
