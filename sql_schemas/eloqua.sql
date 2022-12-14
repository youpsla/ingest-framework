create schema eloqua_development;

create table eloqua_development.clients
(
    jab_id            integer   default "identity"(4116140, 0, '1,1'::text) encode az64,
    jab_created_at    timestamp default ('now'::text)::timestamp without time zone encode az64,
    name     varchar(255)
);


--  Replace <schema_name> with the newly created
 GRANT USAGE ON SCHEMA eloqua_development TO  GROUP dev_rw;
 GRANT SELECT ON ALL TABLES IN SCHEMA eloqua_development TO GROUP dev_rw;
 GRANT CREATE ON SCHEMA eloqua_development TO GROUP dev_rw;
 GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA eloqua_development TO GROUP dev_rw;
 ALTER DEFAULT PRIVILEGES IN SCHEMA eloqua_development GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO GROUP dev_rw;

