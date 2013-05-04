package info.archinnov.achilles.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * ColumnFamily
 * 
 * @author DuyHai DOAN
 * 
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
public @interface WideRow
{
	/**
	 * <p>
	 * Define a Wide row in Cassandra. A Wide row entity has an id (row key) annotated by @Id and a WideMap property annotated with @Column. The WideMap represents the Column Name/Column Value
	 * representation in Thrift
	 * </p>
	 */
}
