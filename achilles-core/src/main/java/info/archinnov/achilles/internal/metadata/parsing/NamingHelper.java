package info.archinnov.achilles.internal.metadata.parsing;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import info.archinnov.achilles.type.NamingStrategy;

import java.util.UnknownFormatConversionException;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.apache.commons.lang3.StringUtils.splitByCharacterTypeCamelCase;

public class NamingHelper {

    public static final Function<String, String> TO_LOWER_CASE = new Function<String, String>() {
        @Override
        public String apply(String input) {
            return input.toLowerCase();
        }
    };

    public static String applyNamingStrategy(String name, NamingStrategy namingStrategy) {
        switch (namingStrategy) {
            case SNAKE_CASE: return toSnakeCase(name);
            case CASE_SENSITIVE: return toCaseSensitive(name);
            case LOWER_CASE: return name.toLowerCase();
            default: throw new UnknownFormatConversionException(format("Cannot convert name '%s' to unknow naming strategy '%s'", name, namingStrategy.name()));
        }
    }

    private static String toCaseSensitive(String name) {
        if (name.equals(name.toLowerCase())) return name;
        else return "\"" + name + "\"";
    }

    private static String toSnakeCase(String name) {
        final String[] tokens = splitByCharacterTypeCamelCase(name);
        final FluentIterable<String> lowerCaseTokens = FluentIterable.from(asList(tokens)).filter(Predicates.notNull()).transform(TO_LOWER_CASE);
        return Joiner.on('_').join(lowerCaseTokens).replaceAll("_+", "_");
    }
}
