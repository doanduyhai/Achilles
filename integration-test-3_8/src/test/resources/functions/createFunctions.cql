CREATE FUNCTION IF NOT EXISTS textToLong(longvalue text)
RETURNS NULL ON NULL INPUT
RETURNS bigint
LANGUAGE java
AS $$
  return Long.parseLong(longvalue);
$$;


CREATE FUNCTION IF NOT EXISTS accumulate(map map<int, text>, clustering int, textVal text, doubleVal double, low_threshold double, high_threshold double)
RETURNS NULL ON NULL INPUT
RETURNS map<int, text>
LANGUAGE java
AS $$
  if(doubleval >= low_threshold && doubleval <= high_threshold) {
    map.put(clustering, textval);
  }
  return map;
$$;

CREATE AGGREGATE IF NOT EXISTS findByDoubleValue(int, text, double, double, double)
SFUNC accumulate
STYPE map<int, text>
INITCOND {};
