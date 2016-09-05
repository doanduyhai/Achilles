CREATE FUNCTION IF NOT EXISTS convertStringToLong(longvalue text)
RETURNS NULL ON NULL INPUT
RETURNS bigint
LANGUAGE java
AS $$
  return Long.parseLong(longvalue);
$$;
