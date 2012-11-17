package fr.doan.achilles.validation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import fr.doan.achilles.validation.Validator;

public class ValidatorTest
{

	@Test(expected = IllegalArgumentException.class)
	public void should_exception_when_blank() throws Exception
	{
		Validator.validateNotBlank("", "arg");
	}

	@Test(expected = IllegalArgumentException.class)
	public void should_exception_when_string_null() throws Exception
	{
		Validator.validateNotBlank(null, "arg");
	}

	@Test(expected = IllegalArgumentException.class)
	public void should_exception_when_null() throws Exception
	{
		Validator.validateNotNull(null, "arg");
	}

	@Test(expected = IllegalArgumentException.class)
	public void should_exception_when_empty_collection() throws Exception
	{
		Validator.validateNotEmpty(new ArrayList<String>(), "arg");
	}

	@Test(expected = IllegalArgumentException.class)
	public void should_exception_when_null_collection() throws Exception
	{
		Validator.validateNotEmpty((Collection<String>) null, "arg");
	}

	@Test(expected = IllegalArgumentException.class)
	public void should_exception_when_empty_map() throws Exception
	{
		Validator.validateNotEmpty(new HashMap<String, String>(), "arg");
	}

	@Test(expected = IllegalArgumentException.class)
	public void should_exception_when_null_map() throws Exception
	{
		Validator.validateNotEmpty((Map<String, String>) null, "arg");
	}

	@Test(expected = IllegalArgumentException.class)
	public void should_exception_when_no_default_constructor() throws Exception
	{
		Validator.validateNoargsConstructor(TestNoArgConstructor.class);
	}

	class TestNoArgConstructor
	{
		private TestNoArgConstructor() {}
	}
}
