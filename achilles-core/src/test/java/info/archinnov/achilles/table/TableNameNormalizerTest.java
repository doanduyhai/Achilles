package info.archinnov.achilles.table;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.exception.AchillesInvalidTableException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * AchillesTableHelperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class TableNameNormalizerTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void should_exception_when_even_class_name_exceeeds_48_characters() throws Exception
    {
        String canonicalName = "ItIsAVeryLoooooooooooooooooooooooooooooooooooooongClassNameExceeding48Characters";

        exception.expect(AchillesInvalidTableException.class);
        exception
                .expectMessage("The table name 'ItIsAVeryLoooooooooooooooooooooooooooooooooooooongClassNameExceeding48Characters' is invalid. It should be respect the pattern [a-zA-Z0-9_] and be at most 48 characters long");
        TableNameNormalizer.normalizerAndValidateColumnFamilyName(canonicalName);
    }

    @Test
    public void should_normalize_canonical_classname() throws Exception
    {
        String canonicalName = "org.achilles.entity.ClassName";

        String normalized = TableNameNormalizer
                .normalizerAndValidateColumnFamilyName(canonicalName);

        assertThat(normalized).isEqualTo("ClassName");
    }
}
