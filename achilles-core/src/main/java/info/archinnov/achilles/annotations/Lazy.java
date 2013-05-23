package info.archinnov.achilles.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Lazy
 * 
 * @author DuyHai DOAN
 * 
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Documented
public @interface Lazy
{
	/**
	 * <p>
	 * Lazy property. The property will be loaded at the first access, cached and returned on subsequent invocation
	 * </p>
	 */
}
