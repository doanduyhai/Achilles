package info.archinnov.achilles.helper;

import java.lang.reflect.Field;
import com.google.common.base.Function;

/**
 * LoggerHelper
 * 
 * @author DuyHai DOAN
 * 
 */
public class LoggerHelper {
    public static Function<Class<?>, String> fqcnToStringFn = new Function<Class<?>, String>() {
        @Override
        public String apply(Class<?> clazz) {
            return clazz.getCanonicalName();
        }
    };
    public static Function<Field, String> fieldToStringFn = new Function<Field, String>() {
        @Override
        public String apply(Field field) {
            return field.getName();
        }
    };
}
