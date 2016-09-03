CREATE FUNCTION IF NOT EXISTS convertToLong(longvalue text)
RETURNS NULL ON NULL INPUT
RETURNS bigint
LANGUAGE java
AS $$
  return Long.parseLong(longvalue);
$$;



CREATE FUNCTION IF NOT EXISTS convertConsistencyLevelList(strings list<text>)
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


CREATE FUNCTION IF NOT EXISTS convertListToJson(strings list<text>)
CALLED ON NULL INPUT
RETURNS text
LANGUAGE java
AS $$
    java.util.StringJoiner joiner = new java.util.StringJoiner(", ", "[", "]");
    for(String s: strings) {
        joiner.add(s);
    }
    return joiner.toString();
$$;

CREATE FUNCTION IF NOT EXISTS stringifyComplexNestingMap(map map<text, frozen<map<int, tuple<text, int, int>>>>)
CALLED ON NULL INPUT
RETURNS text
LANGUAGE java
AS $$
    java.util.Random r = new java.util.Random();
    return "whatever-" + r.nextInt();
$$;

