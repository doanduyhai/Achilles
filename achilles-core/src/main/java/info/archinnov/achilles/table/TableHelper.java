package info.archinnov.achilles.table;

import info.archinnov.achilles.exception.AchillesInvalidColumnFamilyException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TableHelper
 * 
 * @author DuyHai DOAN
 * 
 */
public class TableHelper {
    protected static final String ACHILLES_DDL_SCRIPT = "ACHILLES_DDL_SCRIPT";

    protected static final Logger log = LoggerFactory.getLogger(ACHILLES_DDL_SCRIPT);
    public static final Pattern CF_PATTERN = Pattern.compile("[a-zA-Z0-9_]{1,48}");

    public static String normalizerAndValidateColumnFamilyName(String cfName) {
        log.trace("Normalizing column family '{}' name agains Cassandra restrictions", cfName);

        Matcher nameMatcher = CF_PATTERN.matcher(cfName);

        if (nameMatcher.matches()) {
            return cfName;
        } else if (cfName.contains(".")) {
            String className = cfName.replaceAll(".+\\.(.+)", "$1");
            return normalizerAndValidateColumnFamilyName(className);
        } else {
            throw new AchillesInvalidColumnFamilyException("The column family name '" + cfName
                    + "' is invalid. It should be respect the pattern [a-zA-Z0-9_] and be at most 48 characters long");
        }
    }
}
