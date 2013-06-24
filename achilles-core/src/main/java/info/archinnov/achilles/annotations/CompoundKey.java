package info.archinnov.achilles.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * CompoundKey
 * 
 * @author DuyHai DOAN
 * 
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
public @interface CompoundKey
{
    /**
     * <p>
     * Define a Compound key in Cassandra <br/>
     * 
     * Each property of the class should have the info.archinnov.achilles.annotations.Order annotation
     * </p>
     */
}
