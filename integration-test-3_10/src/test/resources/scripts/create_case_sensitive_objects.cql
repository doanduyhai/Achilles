CREATE KEYSPACE "MyKeyspace" with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

CREATE TYPE "MyKeyspace"."CaseSensitiveUDT" (
  "Quantity" int,
  value text,
);

CREATE TABLE "MyKeyspace".entity_with_case_sensitive_udt (
    id text,
    details frozen<"CaseSensitiveUDT">,
    PRIMARY KEY (id)
);