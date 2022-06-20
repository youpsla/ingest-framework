create schema new_linkedin;

alter schema new_linkedin owner to jabmo;

grant usage on schema new_linkedin to jabmoro;

create table account_pivot_campaign
(
    jab_id            integer   default "identity"(4116140, 0, '1,1'::text) encode az64,
    jab_created_at    timestamp default ('now'::text)::timestamp without time zone encode az64,
    advertiser_id     integer     not null encode az64,
    campaign_id       integer     not null encode az64,
    campaign_name     varchar(255),
    campaign_status   varchar(255),
    campaign_group_id integer     not null encode az64,
    impressions       integer     not null encode az64,
    clicks            integer     not null encode az64,
    shares            integer     not null encode az64,
    costinusd         varchar(50) not null,
    ad_format         varchar(255),
    start_date        timestamp encode az64,
    end_date          timestamp encode az64,
    entry_date        timestamp encode az64
);

alter table account_pivot_campaign
    owner to jabmo;

grant select on account_pivot_campaign to public;

grant select on account_pivot_campaign to jabmoro;

create table accounts
(
    jab_id             integer   default "identity"(4116152, 0, '1,1'::text) encode az64,
    jab_created_at     timestamp default ('now'::text)::timestamp without time zone encode az64,
    id                 integer      not null encode az64,
    name               varchar(255) not null,
    currency           varchar(50)  not null,
    account_type       varchar(50)  not null,
    created_date       timestamp    not null encode az64,
    last_modified_date timestamp    not null encode az64,
    status             varchar(50)  not null,
    ref                varchar(255),
    retrieve_history   boolean   default false
);

alter table accounts
    owner to jabmo;

grant select on accounts to public;

grant select on accounts to jabmoro;

create table campaign_groups
(
    jab_id             integer   default "identity"(4116164, 0, '1,1'::text) encode az64,
    jab_created_at     timestamp default ('now'::text)::timestamp without time zone encode az64,
    id                 integer      not null encode az64,
    name               varchar(255) not null,
    created_date       timestamp    not null encode az64,
    last_modified_date timestamp    not null encode az64,
    account_id         integer      not null encode az64,
    status             varchar(50)  not null
);

alter table campaign_groups
    owner to jabmo;

grant select on campaign_groups to public;

grant select on campaign_groups to jabmoro;

create table campaigns
(
    jab_id                     integer   default "identity"(4116176, 0, '1,1'::text) encode az64,
    jab_created_at             timestamp default ('now'::text)::timestamp without time zone encode az64,
    id                         integer      not null encode az64,
    name                       varchar(255) not null,
    campaign_group_id          integer      not null encode az64,
    account_id                 integer      not null encode az64,
    associated_entity          varchar(255) not null,
    created_date               timestamp    not null encode az64,
    last_modified_date         timestamp    not null encode az64,
    status                     varchar(50)  not null,
    cost_type                  varchar(15)  not null,
    unit_cost_currency_code    varchar(15)  not null,
    unit_cost_amount           varchar(15)  not null,
    daily_budget_currency_code varchar(15),
    daily_budget_amount        varchar(15),
    ad_format                  varchar(255)
);

alter table campaigns
    owner to jabmo;

grant select on campaigns to public;

grant select on campaigns to jabmoro;

create table creative_sponsored_update
(
    jab_id         integer   default "identity"(4116188, 0, '1,1'::text) encode az64,
    jab_created_at timestamp default ('now'::text)::timestamp without time zone encode az64,
    creative_id    integer encode az64,
    title          varchar(800),
    text           varchar(5000),
    creative_url   varchar(5000),
    description    varchar(5000),
    subject        varchar(800)
);

alter table creative_sponsored_update
    owner to jabmo;

grant select on creative_sponsored_update to public;

grant select on creative_sponsored_update to jabmoro;

create table creative_sponsored_video
(
    jab_id         integer   default "identity"(4116200, 0, '1,1'::text) encode az64,
    jab_created_at timestamp default ('now'::text)::timestamp without time zone encode az64,
    creative_id    integer not null encode az64,
    campaign_id    integer not null encode az64,
    creative_name  varchar(255),
    ugc_ref        varchar(255),
    status         varchar(255),
    duration_video integer encode az64,
    created_date   timestamp encode az64,
    width          varchar(50),
    height         varchar(50)
);

alter table creative_sponsored_video
    owner to jabmo;

grant select on creative_sponsored_video to public;

grant select on creative_sponsored_video to jabmoro;

create table creative_text_ads
(
    jab_id         integer   default "identity"(4116212, 0, '1,1'::text) encode az64,
    jab_created_at timestamp default ('now'::text)::timestamp without time zone encode az64,
    id             integer not null encode az64,
    title          varchar(255),
    text           varchar(255),
    vector_image   varchar(255),
    click_uri      varchar(255),
    type           varchar(255),
    status         varchar(255),
    created_date   timestamp encode az64,
    modified_date  timestamp encode az64,
    entry_date     timestamp encode az64
);

alter table creative_text_ads
    owner to jabmo;

grant select on creative_text_ads to public;

grant select on creative_text_ads to jabmoro;

create table creative_url
(
    jab_id         integer   default "identity"(4116224, 0, '1,1'::text) encode az64,
    jab_created_at timestamp default ('now'::text)::timestamp without time zone encode az64,
    creative_id    integer not null encode az64,
    linkedin_url   varchar(5000),
    preview_url    varchar(5000)
);

alter table creative_url
    owner to jabmo;

grant select on creative_url to public;

grant select on creative_url to jabmoro;

create table pivot_creative
(
    jab_id           integer   default "identity"(4116236, 0, '1,1'::text) encode az64,
    jab_created_at   timestamp default ('now'::text)::timestamp without time zone encode az64,
    creative_id      integer     not null encode az64,
    impressions      integer     not null encode az64,
    clicks           integer     not null encode az64,
    cost_in_usd      varchar(50) not null,
    facet            varchar(50) not null,
    facet_id         integer     not null encode az64,
    start_date       timestamp encode az64,
    end_date         timestamp encode az64,
    time_granularity varchar(255),
    account_id       integer encode az64,
    creative_type    varchar(255),
    status           varchar(50)
);

alter table pivot_creative
    owner to jabmo;

grant select on pivot_creative to public;

grant select on pivot_creative to jabmoro;

create table pivot_job_title
(
    jab_id           integer   default "identity"(4116248, 0, '1,1'::text) encode az64,
    jab_created_at   timestamp default ('now'::text)::timestamp without time zone encode az64,
    id               integer     not null encode az64,
    impressions      integer     not null encode az64,
    clicks           integer     not null encode az64,
    cost_in_usd      varchar(50) not null,
    facet            varchar(50) not null,
    facet_id         integer     not null encode az64,
    start_date       timestamp encode az64,
    end_date         timestamp encode az64,
    time_granularity varchar(255),
    job_title        varchar(255)
)
    sortkey (facet_id);

alter table pivot_job_title
    owner to jabmo;

grant select on pivot_job_title to public;

grant select on pivot_job_title to jabmoro;

create table pivot_member_company
(
    jab_id            integer   default "identity"(4116260, 0, '1,1'::text) encode az64,
    jab_created_at    timestamp default ('now'::text)::timestamp without time zone encode az64,
    organization_id   integer     not null encode az64,
    impressions       integer     not null encode az64,
    clicks            integer     not null encode az64,
    cost_in_usd       varchar(50) not null,
    start_date        timestamp   not null encode az64,
    end_date          timestamp   not null encode az64,
    facet             varchar(50) not null,
    facet_id          integer     not null encode az64,
    organization_name varchar(255),
    time_granularity  varchar(255)
);

alter table pivot_member_company
    owner to jabmo;

grant select on pivot_member_company to public;

grant select on pivot_member_company to jabmoro;

create table pivot_member_county
(
    jab_id           integer   default "identity"(4116272, 0, '1,1'::text) encode az64,
    jab_created_at   timestamp default ('now'::text)::timestamp without time zone encode az64,
    geo_id           integer     not null encode az64,
    impressions      integer     not null encode az64,
    clicks           integer     not null encode az64,
    cost_in_usd      varchar(50) not null,
    facet            varchar(50) not null,
    facet_id         integer     not null encode az64,
    country          varchar(255),
    time_granularity varchar(255),
    start_date       timestamp encode az64,
    end_date         timestamp encode az64,
    region           varchar(255),
    city             varchar(255)
);

alter table pivot_member_county
    owner to jabmo;

grant select on pivot_member_county to public;

grant select on pivot_member_county to jabmoro;

create table pivot_member_country
(
    jab_id           integer   default "identity"(4116284, 0, '1,1'::text) encode az64,
    jab_created_at   timestamp default ('now'::text)::timestamp without time zone encode az64,
    geo_id           integer     not null encode az64,
    impressions      integer     not null encode az64,
    clicks           integer     not null encode az64,
    cost_in_usd      varchar(50) not null,
    facet            varchar(50) not null,
    facet_id         integer     not null encode az64,
    country          varchar(255),
    time_granularity varchar(255),
    start_date       timestamp encode az64,
    end_date         timestamp encode az64
);

alter table pivot_member_country
    owner to jabmo;

grant select on pivot_member_country to public;

grant select on pivot_member_country to jabmoro;

create table pivot_job_title_full
(
    jab_id           integer   default "identity"(4116296, 0, '1,1'::text) encode az64,
    jab_created_at   timestamp default ('now'::text)::timestamp without time zone encode az64,
    id               integer     not null encode az64,
    impressions      integer     not null encode az64,
    clicks           integer     not null encode az64,
    cost_in_usd      varchar(50) not null,
    facet            varchar(50) not null,
    facet_id         integer     not null encode az64,
    start_date       timestamp encode az64,
    end_date         timestamp encode az64,
    time_granularity varchar(255),
    job_title        varchar(255)
)
    sortkey (facet_id);

alter table pivot_job_title_full
    owner to jabmo;

grant select on pivot_job_title_full to public;

grant select on pivot_job_title_full to jabmoro;

create table pivot_member_company_full
(
    jab_id            integer   default "identity"(4116308, 0, '1,1'::text) encode az64,
    jab_created_at    timestamp default ('now'::text)::timestamp without time zone encode az64,
    organization_id   integer     not null encode az64,
    impressions       integer     not null encode az64,
    clicks            integer     not null encode az64,
    cost_in_usd       varchar(50) not null,
    start_date        timestamp   not null encode az64,
    end_date          timestamp   not null encode az64,
    facet             varchar(50) not null,
    facet_id          integer     not null encode az64,
    organization_name varchar(255),
    time_granularity  varchar(255)
);

alter table pivot_member_company_full
    owner to jabmo;

grant select on pivot_member_company_full to public;

grant select on pivot_member_company_full to jabmoro;

create table pivot_member_country_full
(
    jab_id           integer   default "identity"(4116320, 0, '1,1'::text) encode az64,
    jab_created_at   timestamp default ('now'::text)::timestamp without time zone encode az64,
    geo_id           integer     not null encode az64,
    impressions      integer     not null encode az64,
    clicks           integer     not null encode az64,
    cost_in_usd      varchar(50) not null,
    facet            varchar(50) not null,
    facet_id         integer     not null encode az64,
    country          varchar(255),
    time_granularity varchar(255),
    start_date       timestamp encode az64,
    end_date         timestamp encode az64,
    region           varchar(255),
    city             varchar(255)
);

alter table pivot_member_country_full
    owner to jabmo;

grant select on pivot_member_country_full to public;

grant select on pivot_member_country_full to jabmoro;

create table social_metrics
(
    jab_id           integer   default "identity"(4116332, 0, '1,1'::text) encode az64,
    jab_created_at   timestamp default ('now'::text)::timestamp without time zone encode az64,
    creative_id      integer encode az64,
    campaign_id      integer encode az64,
    advertiser_id    integer encode az64,
    costinusd        numeric(18) encode az64,
    start_date       timestamp encode az64,
    end_date         timestamp encode az64,
    impressions      integer encode az64,
    shares           integer encode az64,
    clicks           integer encode az64,
    likes            integer encode az64,
    status           varchar(255),
    creative_type    varchar(255),
    time_granularity varchar(50)
);

alter table social_metrics
    owner to jabmo;

grant select on social_metrics to public;

grant select on social_metrics to jabmoro;

create table staging
(
    jab_id             integer encode az64,
    jab_created_at     timestamp encode az64,
    id                 integer      not null encode az64,
    name               varchar(255) not null,
    created_date       timestamp    not null encode az64,
    last_modified_date timestamp    not null encode az64,
    account_id         integer      not null encode az64,
    status             varchar(50)  not null
);

alter table staging
    owner to jabmo;

grant select on staging to public;

create view v_pivot_member_company_clicks
            (organization_id, impressions, clicks, cost_in_usd, start_date, end_date, facet, facet_id, jab_created_at,
             organization_name, time_granularity, gen_num, id)
as
SELECT t.organization_id,
       t.impressions,
       t.clicks,
       t.cost_in_usd,
       t.start_date,
       t.end_date,
       t.facet,
       t.facet_id,
       t.jab_created_at,
       t.organization_name,
       t.time_granularity,
       g.gen_num,
       md5((((((t.clicks::character varying::text || g.gen_num::character varying::text) ||
               t.impressions::character varying::text) || t.organization_id::character varying::text) ||
             t.facet_id::character varying::text) || t.start_date::character varying::text) ||
           t.organization_id::character varying::text) AS id
FROM new_linkedin.pivot_member_company t
         JOIN (SELECT 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num AS gen_num
               FROM (((((((((SELECT 1 AS num
                             UNION
                             SELECT 2)
                            UNION
                            SELECT 3)
                           UNION
                           SELECT 4)
                          UNION
                          SELECT 5)
                         UNION
                         SELECT 6)
                        UNION
                        SELECT 7)
                       UNION
                       SELECT 8)
                      UNION
                      SELECT 9)
                     UNION
                     SELECT 0) t1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t2 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t3 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t4 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t5 ON 1 = 1
               WHERE (10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) > 0
               ORDER BY 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) g ON 1 = 1
WHERE g.gen_num <= t.clicks;

alter table v_pivot_member_company_clicks
    owner to jabmo;

grant select on v_pivot_member_company_clicks to public;

grant select on v_pivot_member_company_clicks to jabmoro;

create view v_pivot_member_country_clicks
            (geo_id, impressions, clicks, cost_in_usd, facet, facet_id, country, time_granularity, start_date, end_date,
             jab_created_at, gen_num, id)
as
SELECT t.geo_id,
       t.impressions,
       t.clicks,
       t.cost_in_usd,
       t.facet,
       t.facet_id,
       t.country,
       t.time_granularity,
       t.start_date,
       t.end_date,
       t.jab_created_at,
       g.gen_num,
       md5((((((t.clicks::character varying::text || g.gen_num::character varying::text) ||
               t.impressions::character varying::text) || t.geo_id::character varying::text) ||
             t.facet_id::character varying::text) || t.start_date::character varying::text) ||
           t.geo_id::character varying::text) AS id
FROM new_linkedin.pivot_member_country t
         JOIN (SELECT 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num AS gen_num
               FROM (((((((((SELECT 1 AS num
                             UNION
                             SELECT 2)
                            UNION
                            SELECT 3)
                           UNION
                           SELECT 4)
                          UNION
                          SELECT 5)
                         UNION
                         SELECT 6)
                        UNION
                        SELECT 7)
                       UNION
                       SELECT 8)
                      UNION
                      SELECT 9)
                     UNION
                     SELECT 0) t1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t2 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t3 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t4 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t5 ON 1 = 1
               WHERE (10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) > 0
               ORDER BY 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) g ON 1 = 1
WHERE g.gen_num <= t.clicks;

alter table v_pivot_member_country_clicks
    owner to jabmo;

grant select on v_pivot_member_country_clicks to public;

grant select on v_pivot_member_country_clicks to jabmoro;

create view v_social_metrics_clicks
            (creative_id, campaign_id, advertiser_id, costinusd, start_date, end_date, impressions, shares, clicks,
             likes, status, creative_type, time_granularity, gen_num, id)
as
SELECT t.creative_id,
       t.campaign_id,
       t.advertiser_id,
       t.costinusd,
       t.start_date,
       t.end_date,
       t.impressions,
       t.shares,
       t.clicks,
       t.likes,
       t.status,
       t.creative_type,
       t.time_granularity,
       g.gen_num,
       md5((((t.clicks::character varying::text || g.gen_num::character varying::text) ||
             t.impressions::character varying::text) || t.start_date::character varying::text) ||
           t.creative_id::character varying::text) AS id
FROM new_linkedin.social_metrics t
         JOIN (SELECT 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num AS gen_num
               FROM (((((((((SELECT 1 AS num
                             UNION
                             SELECT 2)
                            UNION
                            SELECT 3)
                           UNION
                           SELECT 4)
                          UNION
                          SELECT 5)
                         UNION
                         SELECT 6)
                        UNION
                        SELECT 7)
                       UNION
                       SELECT 8)
                      UNION
                      SELECT 9)
                     UNION
                     SELECT 0) t1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t2 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t3 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t4 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t5 ON 1 = 1
               WHERE (10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) > 0
               ORDER BY 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) g ON 1 = 1
WHERE g.gen_num <= t.clicks;

alter table v_social_metrics_clicks
    owner to jabmo;

grant select on v_social_metrics_clicks to public;

grant select on v_social_metrics_clicks to jabmoro;

create view v_social_metrics_impressions
            (creative_id, campaign_id, advertiser_id, costinusd, start_date, end_date, impressions, shares, clicks,
             likes, status, creative_type, time_granularity, gen_num, id)
as
SELECT t.creative_id,
       t.campaign_id,
       t.advertiser_id,
       t.costinusd,
       t.start_date,
       t.end_date,
       t.impressions,
       t.shares,
       t.clicks,
       t.likes,
       t.status,
       t.creative_type,
       t.time_granularity,
       g.gen_num,
       md5((((t.clicks::character varying::text || g.gen_num::character varying::text) ||
             t.impressions::character varying::text) || t.start_date::character varying::text) ||
           t.creative_id::character varying::text) AS id
FROM new_linkedin.social_metrics t
         JOIN (SELECT 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num AS gen_num
               FROM (((((((((SELECT 1 AS num
                             UNION
                             SELECT 2)
                            UNION
                            SELECT 3)
                           UNION
                           SELECT 4)
                          UNION
                          SELECT 5)
                         UNION
                         SELECT 6)
                        UNION
                        SELECT 7)
                       UNION
                       SELECT 8)
                      UNION
                      SELECT 9)
                     UNION
                     SELECT 0) t1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t2 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t3 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t4 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t5 ON 1 = 1
               WHERE (10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) > 0
               ORDER BY 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) g ON 1 = 1
WHERE g.gen_num <= t.impressions;

alter table v_social_metrics_impressions
    owner to jabmo;

grant select on v_social_metrics_impressions to public;

grant select on v_social_metrics_impressions to jabmoro;

create view v_social_metrics_likes
            (creative_id, campaign_id, advertiser_id, costinusd, start_date, end_date, impressions, shares, clicks,
             likes, status, creative_type, time_granularity, gen_num, id)
as
SELECT t.creative_id,
       t.campaign_id,
       t.advertiser_id,
       t.costinusd,
       t.start_date,
       t.end_date,
       t.impressions,
       t.shares,
       t.clicks,
       t.likes,
       t.status,
       t.creative_type,
       t.time_granularity,
       g.gen_num,
       md5((((t.clicks::character varying::text || g.gen_num::character varying::text) ||
             t.impressions::character varying::text) || t.start_date::character varying::text) ||
           t.creative_id::character varying::text) AS id
FROM new_linkedin.social_metrics t
         JOIN (SELECT 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num AS gen_num
               FROM (((((((((SELECT 1 AS num
                             UNION
                             SELECT 2)
                            UNION
                            SELECT 3)
                           UNION
                           SELECT 4)
                          UNION
                          SELECT 5)
                         UNION
                         SELECT 6)
                        UNION
                        SELECT 7)
                       UNION
                       SELECT 8)
                      UNION
                      SELECT 9)
                     UNION
                     SELECT 0) t1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t2 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t3 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t4 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t5 ON 1 = 1
               WHERE (10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) > 0
               ORDER BY 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) g ON 1 = 1
WHERE g.gen_num <= t.likes;

alter table v_social_metrics_likes
    owner to jabmo;

grant select on v_social_metrics_likes to public;

grant select on v_social_metrics_likes to jabmoro;

create view v_social_metrics_shares
            (creative_id, campaign_id, advertiser_id, costinusd, start_date, end_date, impressions, shares, clicks,
             likes, status, creative_type, time_granularity, gen_num, id)
as
SELECT t.creative_id,
       t.campaign_id,
       t.advertiser_id,
       t.costinusd,
       t.start_date,
       t.end_date,
       t.impressions,
       t.shares,
       t.clicks,
       t.likes,
       t.status,
       t.creative_type,
       t.time_granularity,
       g.gen_num,
       md5((((t.clicks::character varying::text || g.gen_num::character varying::text) ||
             t.impressions::character varying::text) || t.start_date::character varying::text) ||
           t.creative_id::character varying::text) AS id
FROM new_linkedin.social_metrics t
         JOIN (SELECT 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num AS gen_num
               FROM (((((((((SELECT 1 AS num
                             UNION
                             SELECT 2)
                            UNION
                            SELECT 3)
                           UNION
                           SELECT 4)
                          UNION
                          SELECT 5)
                         UNION
                         SELECT 6)
                        UNION
                        SELECT 7)
                       UNION
                       SELECT 8)
                      UNION
                      SELECT 9)
                     UNION
                     SELECT 0) t1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t2 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t3 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t4 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t5 ON 1 = 1
               WHERE (10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) > 0
               ORDER BY 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) g ON 1 = 1
WHERE g.gen_num <= t.shares;

alter table v_social_metrics_shares
    owner to jabmo;

grant select on v_social_metrics_shares to public;

grant select on v_social_metrics_shares to jabmoro;

create view v_pivot_member_company_impressions
            (organization_id, impressions, clicks, cost_in_usd, start_date, end_date, facet, facet_id, jab_created_at,
             organization_name, time_granularity, gen_num, id)
as
SELECT t.organization_id,
       t.impressions,
       t.clicks,
       t.cost_in_usd,
       t.start_date,
       t.end_date,
       t.facet,
       t.facet_id,
       t.jab_created_at,
       t.organization_name,
       t.time_granularity,
       g.gen_num,
       md5((((((t.clicks::character varying::text || g.gen_num::character varying::text) ||
               t.impressions::character varying::text) || t.organization_id::character varying::text) ||
             t.facet_id::character varying::text) || t.start_date::character varying::text) ||
           t.organization_id::character varying::text) AS id
FROM new_linkedin.pivot_member_company t
         JOIN (SELECT 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num AS gen_num
               FROM (((((((((SELECT 1 AS num
                             UNION
                             SELECT 2)
                            UNION
                            SELECT 3)
                           UNION
                           SELECT 4)
                          UNION
                          SELECT 5)
                         UNION
                         SELECT 6)
                        UNION
                        SELECT 7)
                       UNION
                       SELECT 8)
                      UNION
                      SELECT 9)
                     UNION
                     SELECT 0) t1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t2 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t3 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t4 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t5 ON 1 = 1
               WHERE (10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) > 0
               ORDER BY 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) g ON 1 = 1
WHERE g.gen_num <= t.impressions;

alter table v_pivot_member_company_impressions
    owner to jabmo;

grant select on v_pivot_member_company_impressions to public;

grant select on v_pivot_member_company_impressions to jabmoro;

create view v_pivot_member_country_impressions
            (geo_id, impressions, clicks, cost_in_usd, facet, facet_id, country, time_granularity, start_date, end_date,
             jab_created_at, gen_num, id)
as
SELECT t.geo_id,
       t.impressions,
       t.clicks,
       t.cost_in_usd,
       t.facet,
       t.facet_id,
       t.country,
       t.time_granularity,
       t.start_date,
       t.end_date,
       t.jab_created_at,
       g.gen_num,
       md5((((((t.clicks::character varying::text || g.gen_num::character varying::text) ||
               t.impressions::character varying::text) || t.geo_id::character varying::text) ||
             t.facet_id::character varying::text) || t.start_date::character varying::text) ||
           t.geo_id::character varying::text) AS id
FROM new_linkedin.pivot_member_country t
         JOIN (SELECT 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num AS gen_num
               FROM (((((((((SELECT 1 AS num
                             UNION
                             SELECT 2)
                            UNION
                            SELECT 3)
                           UNION
                           SELECT 4)
                          UNION
                          SELECT 5)
                         UNION
                         SELECT 6)
                        UNION
                        SELECT 7)
                       UNION
                       SELECT 8)
                      UNION
                      SELECT 9)
                     UNION
                     SELECT 0) t1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t2 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t3 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t4 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t5 ON 1 = 1
               WHERE (10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) > 0
               ORDER BY 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) g ON 1 = 1
WHERE g.gen_num <= t.impressions;

alter table v_pivot_member_country_impressions
    owner to jabmo;

grant select on v_pivot_member_country_impressions to public;

grant select on v_pivot_member_country_impressions to jabmoro;

create view v_pivot_job_title_clicks
            (impressions, clicks, cost_in_usd, start_date, end_date, facet, facet_id, jab_created_at, organization_name,
             time_granularity, gen_num, id)
as
SELECT t.impressions,
       t.clicks,
       t.cost_in_usd,
       t.start_date,
       t.end_date,
       t.facet,
       t.facet_id,
       t.jab_created_at,
       t.job_title                                                       AS organization_name,
       t.time_granularity,
       g.gen_num,
       md5(((((t.clicks::character varying::text || g.gen_num::character varying::text) ||
              t.impressions::character varying::text) || t.facet_id::character varying::text) ||
            t.start_date::character varying::text) || t.job_title::text) AS id
FROM new_linkedin.pivot_job_title t
         JOIN (SELECT 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num AS gen_num
               FROM (((((((((SELECT 1 AS num
                             UNION
                             SELECT 2)
                            UNION
                            SELECT 3)
                           UNION
                           SELECT 4)
                          UNION
                          SELECT 5)
                         UNION
                         SELECT 6)
                        UNION
                        SELECT 7)
                       UNION
                       SELECT 8)
                      UNION
                      SELECT 9)
                     UNION
                     SELECT 0) t1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t2 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t3 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t4 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t5 ON 1 = 1
               WHERE (10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) > 0
               ORDER BY 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) g ON 1 = 1
WHERE g.gen_num <= t.clicks;

alter table v_pivot_job_title_clicks
    owner to jabmo;

grant select on v_pivot_job_title_clicks to public;

grant select on v_pivot_job_title_clicks to jabmoro;

create view v_pivot_job_title_impressions
            (impressions, clicks, cost_in_usd, start_date, end_date, facet, facet_id, jab_created_at, organization_name,
             time_granularity, gen_num, id)
as
SELECT t.impressions,
       t.clicks,
       t.cost_in_usd,
       t.start_date,
       t.end_date,
       t.facet,
       t.facet_id,
       t.jab_created_at,
       t.job_title                                                       AS organization_name,
       t.time_granularity,
       g.gen_num,
       md5(((((t.clicks::character varying::text || g.gen_num::character varying::text) ||
              t.impressions::character varying::text) || t.facet_id::character varying::text) ||
            t.start_date::character varying::text) || t.job_title::text) AS id
FROM new_linkedin.pivot_job_title t
         JOIN (SELECT 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num AS gen_num
               FROM (((((((((SELECT 1 AS num
                             UNION
                             SELECT 2)
                            UNION
                            SELECT 3)
                           UNION
                           SELECT 4)
                          UNION
                          SELECT 5)
                         UNION
                         SELECT 6)
                        UNION
                        SELECT 7)
                       UNION
                       SELECT 8)
                      UNION
                      SELECT 9)
                     UNION
                     SELECT 0) t1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t2 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t3 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t4 ON 1 = 1
                        JOIN (((((((((SELECT 1 AS num
                                      UNION
                                      SELECT 2)
                                     UNION
                                     SELECT 3)
                                    UNION
                                    SELECT 4)
                                   UNION
                                   SELECT 5)
                                  UNION
                                  SELECT 6)
                                 UNION
                                 SELECT 7)
                                UNION
                                SELECT 8)
                               UNION
                               SELECT 9)
                              UNION
                              SELECT 0) t5 ON 1 = 1
               WHERE (10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) > 0
               ORDER BY 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) g ON 1 = 1
WHERE g.gen_num <= t.impressions;

alter table v_pivot_job_title_impressions
    owner to jabmo;

grant select on v_pivot_job_title_impressions to public;

grant select on v_pivot_job_title_impressions to jabmoro;

