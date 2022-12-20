create schema eloqua_development;

create table eloqua_development.clients
(
    jab_id            integer   default "identity"(4116140, 0, '1,1'::text) encode az64,
    jab_created_at    timestamp default ('now'::text)::timestamp without time zone encode az64,
    name     varchar(255)
);


create table eloqua_development.accounts
(
    jab_id            integer   default "identity"(4116140, 0, '1,1'::text) encode az64,
    jab_created_at    timestamp default ('now'::text)::timestamp without time zone encode az64,
    client_name varchar(255) ENCODE lzo,
    id VARCHAR(255)   ENCODE lzo,
    name     varchar(255) ENCODE lzo,
    address1 varchar(1024) ENCODE lzo,
    city varchar(255) ENCODE lzo,
    country varchar(255) ENCODE lzo,
    created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
    updated_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
);


--  Replace <schema_name> with the newly created
 GRANT USAGE ON SCHEMA eloqua_development TO  GROUP dev_rw;
 GRANT SELECT ON ALL TABLES IN SCHEMA eloqua_development TO GROUP dev_rw;
 GRANT CREATE ON SCHEMA eloqua_development TO GROUP dev_rw;
 GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA eloqua_development TO GROUP dev_rw;
 ALTER DEFAULT PRIVILEGES IN SCHEMA eloqua_development GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO GROUP dev_rw;

