CREATE FUNCTION IF NOT EXISTS convertToLong(input text)
RETURNS NULL ON NULL INPUT
RETURNS bigint
LANGUAGE java
AS $$
  return Long.parseLong(input);
$$;



CREATE FUNCTION IF NOT EXISTS convertListToJson(strings list<text>)
RETURNS NULL ON NULL INPUT
RETURNS text
LANGUAGE java
AS $$
  final java.util.StringJoiner joiner = new java.util.StringJoiner(",", "[", "]");
   for(Object string: strings) {
       joiner.add("'" + (String)string + "'");
   }
  return joiner.toString();
$$;