CREATE SCHEMA on24_development;

create table if not exists attendees
(
    client_id                  integer distkey,
    event_id                   integer,
    event_user_id              integer encode az64,
    source_event_id            integer encode az64,
    email                      varchar(255) encode lzo,
    is_blocked                 varchar(255) encode lzo,
    engagement_score           numeric(18) encode az64,
    live_minutes               integer encode az64,
    live_viewed                integer encode az64,
    archive_minutes            integer encode az64,
    archive_viewed             integer encode az64,
    asked_questions            integer encode az64,
    resources_downloaded       integer encode az64,
    answered_polls             integer encode az64,
    answered_surveys           integer encode az64,
    user_profile_url           varchar(1000) encode lzo,
    user_status                varchar(255) encode lzo,
    cumulative_live_minutes    integer encode az64,
    cumulative_archive_minutes integer encode az64,
    entry_date                 TIMESTAMP WITHOUT TIME ZONE encode az64,
    first_archive_activity     varchar(255) encode lzo,
    last_archive_activity      varchar(255) encode lzo
)
    diststyle key
    sortkey (client_id, event_id);

create table if not exists events
(
	jab_id INT IDENTITY(1,1),
	jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate,
    client_id         integer distkey,
    event_id          integer,
    goodafter         timestamp encode az64,
    description       varchar(1000) encode lzo,
    displaytimezonecd varchar(255) encode lzo,
    eventtype         varchar(255) encode lzo,
    createtimestamp   timestamp encode az64,
    localelanguagecd  varchar(255) encode lzo,
    lastmodified      timestamp encode az64,
    iseliteexpired    varchar(25) encode lzo,
    livestart         timestamp encode az64,
    liveend           timestamp encode az64,
    archivestart      varchar(255) encode lzo,
    archiveend        varchar(255) encode lzo,
    audienceurl       varchar(1000) encode lzo,
    eventprofile      varchar(255) encode lzo,
    streamtype        varchar(255) encode lzo,
    audiencekey       varchar(255) encode lzo,
    reporturl         varchar(1000) encode lzo,
    uploadurl         varchar(1000) encode lzo,
    pmurl             varchar(1000) encode lzo,
    previewurl        varchar(1000) encode lzo,
    contenttype       varchar(255) encode lzo,
    lastupdated       timestamp encode az64,
    category          varchar(255) encode lzo,
    isactive          boolean,
    regrequired       boolean
)
    diststyle key
    sortkey (client_id, event_id);

create table if not exists speakers
(
    client_id       integer distkey,
    event_id        integer,
    name            varchar(500) encode lzo,
    title           varchar(500) encode lzo,
    company         varchar(255) encode lzo,
    description     varchar(5000) encode lzo,
    createtimestamp TIMESTAMP WITHOUT TIME ZONE encode az64,
    entry_date      TIMESTAMP WITHOUT TIME ZONE encode az64,
)
    diststyle key
    sortkey (client_id, event_id);

create table if not exists videos
(
    client_id  integer distkey,
    event_id   integer,
    video_url  varchar(1000) encode lzo,
    entry_date TIMESTAMP WITHOUT TIME ZONE encode az64,
)
    diststyle key
    sortkey (client_id, event_id);

create table if not exists slides
(
    client_id  integer distkey,
    event_id   integer,
    slide_url  varchar(1000) encode lzo,
    entry_date TIMESTAMP WITHOUT TIME ZONE encode az64,
)
    diststyle key
    sortkey (client_id, event_id);

create table if not exists clients
(
    client_id              integer encode az64,
    name                   varchar(500) encode lzo,
    subscriber_division_id bigint encode az64,
    entry_date             TIMESTAMP WITHOUT TIME ZONE encode az64,
);


create table if not exists registrants
(
    client_id        integer distkey,
    event_id         integer,
    event_user_id    integer encode az64,
    first_name       varchar(255) encode lzo,
    last_name        varchar(255) encode lzo,
    email            varchar(255) encode lzo,
    company          varchar(255) encode lzo,
    job_function     varchar(255) encode lzo,
    marketing_email  varchar(255) encode lzo,
    event_email      varchar(255) encode lzo,
    user_profile_url varchar(1000) encode lzo,
    ip_address       varchar(255) encode lzo,
    os               varchar(255) encode lzo,
    browser          varchar(255) encode lzo,
    email_format     varchar(255) encode lzo,
    source_event_id  integer encode az64,
    partnerref       varchar(255) encode lzo,
    user_status      varchar(255) encode lzo,
    create_timestamp TIMESTAMP WITHOUT TIME ZONE encode az64,
    last_activity    TIMESTAMP WITHOUT TIME ZONE encode az64,
    entry_date       TIMESTAMP WITHOUT TIME ZONE encode az64,
    registrant_type  varchar(255) encode lzo
)
    diststyle key
    sortkey (client_id, event_id);


--  Replace <schema_name> with the newly created
 GRANT USAGE ON SCHEMA on24_development TO GROUP dev_ro;
 GRANT SELECT ON ALL TABLES IN SCHEMA on24_development TO GROUP dev_ro;
 ALTER DEFAULT PRIVILEGES IN SCHEMA on24_development GRANT SELECT ON TABLES TO GROUP dev_ro;

 GRANT CREATE ON SCHEMA on24_development TO GROUP dev_rw;
 GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA on24_development TO GROUP dev_rw;
 ALTER DEFAULT PRIVILEGES IN SCHEMA on24_development GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO GROUP dev_rw;

 GRANT CREATE ON DATABASE snowplow TO GROUP dev_rw;

