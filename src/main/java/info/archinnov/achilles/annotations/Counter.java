package info.archinnov.achilles.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Counter
 * 
 * @author DuyHai DOAN
 * 
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Documented
public @interface Counter
{
	/**
	 * <p>
	 * Define a field as counter type in Cassandra. This field will be persisted in a separated Counter Column Family. This column family is internal to Achilles
	 * </p>
	 */
}
