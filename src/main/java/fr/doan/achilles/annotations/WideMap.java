package fr.doan.achilles.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author DuyHai DOAN
 * 
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(
{
		ElementType.FIELD,
		ElementType.TYPE
})
@Documented
public @interface WideMap
{

	/**
	 * <p>
	 * Indicates the column family to store this wide map
	 * </p>
	 */
	String columnFamily() default "";

	/**
	 * <p>
	 * Indicates the join key if the wide map is stored in a separated column family
	 * </p>
	 */
	String[] key() default {};
}
