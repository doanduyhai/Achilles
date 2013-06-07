package info.archinnov.achilles.table;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.exception.AchillesInvalidColumnFamilyException;

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
public class TableHelperTest
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void should_exception_when_even_class_name_exceeeds_48_characters() throws Exception
	{
		String canonicalName = "ItIsAVeryLoooooooooooooooooooooooooooooooooooooongClassNameExceeding48Characters";

		exception.expect(AchillesInvalidColumnFamilyException.class);
		exception
				.expectMessage("The column family name 'ItIsAVeryLoooooooooooooooooooooooooooooooooooooongClassNameExceeding48Characters' is invalid. It should be respect the pattern [a-zA-Z0-9_] and be at most 48 characters long");
		TableHelper.normalizerAndValidateColumnFamilyName(canonicalName);
	}

	@Test
	public void should_normalize_canonical_classname() throws Exception
	{
		String canonicalName = "org.achilles.entity.ClassName";

		String normalized = TableHelper
				.normalizerAndValidateColumnFamilyName(canonicalName);

		assertThat(normalized).isEqualTo("ClassName");
	}
}
