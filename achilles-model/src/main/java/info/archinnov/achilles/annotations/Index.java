package info.archinnov.achilles.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Documented
public @interface Index {
	/**
	 * <p>
	 * Index property. This property is a secondary index. (ie. Ability to
	 * retrieve keys based on this field's value)
	 * </p>
	 */
	String name() default "";
}
