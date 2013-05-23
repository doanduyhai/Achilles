package info.archinnov.achilles.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Key
 * 
 * @author DuyHai DOAN
 * 
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Documented
public @interface Key
{

	/**
	 * <p>
	 * Indicates the key order. The order index start at 1
	 * </p>
	 */
	int order();
}
