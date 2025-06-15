Creating database tables...

2025-06-10 14:28:21,753 INFO sqlalchemy.engine.Engine select pg_catalog.version()
2025-06-10 14:28:21,753 INFO sqlalchemy.engine.Engine [raw sql] {}
2025-06-10 14:28:21,758 INFO sqlalchemy.engine.Engine select current_schema()
2025-06-10 14:28:21,758 INFO sqlalchemy.engine.Engine [raw sql] {}
2025-06-10 14:28:21,758 INFO sqlalchemy.engine.Engine show standard_conforming_strings
2025-06-10 14:28:21,758 INFO sqlalchemy.engine.Engine [raw sql] {}
2025-06-10 14:28:21,764 INFO sqlalchemy.engine.Engine BEGIN (implicit)
2025-06-10 14:28:21,766 INFO sqlalchemy.engine.Engine SELECT pg_catalog.pg_class.relname

FROM pg_catalog.pg_class JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace
WHERE pg_catalog.pg_class.relname = %(table_name)s AND pg_catalog.pg_class.relkind = ANY (ARRAY[%(param_1)s, %(param_2)s, %(param_3)s, %(param_4)s, %(param_5)s]) AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) AND pg_catalog.pg_namespace.nspname != %(nspname_1)s
2025-06-10 14:28:21,766 INFO sqlalchemy.engine.Engine [generated in 0.00044s] {'table_name': 'stores', 'param_1': 'r', 'param_2': 'p', 'param_3': 'f', 'param_4': 'v', 'param_5': 'm', 'nspname_1': 'pg_catalog'}
2025-06-10 14:28:21,772 INFO sqlalchemy.engine.Engine SELECT pg_catalog.pg_class.relname 
FROM pg_catalog.pg_class JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace
WHERE pg_catalog.pg_class.relname = %(table_name)s AND pg_catalog.pg_class.relkind = ANY (ARRAY[%(param_1)s, %(param_2)s, %(param_3)s, %(param_4)s, %(param_5)s]) AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) AND pg_catalog.pg_namespace.nspname != %(nspname_1)s
2025-06-10 14:28:21,772 INFO sqlalchemy.engine.Engine [cached since 0.008057s ago] {'table_name': 'store_status', 'param_1': 'r', 'param_2': 'p', 'param_3': 'f', 'param_4': 'v', 'param_5': 'm', 'nspname_1': 'pg_catalog'}
2025-06-10 14:28:21,777 INFO sqlalchemy.engine.Engine SELECT pg_catalog.pg_class.relname
FROM pg_catalog.pg_class JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace
WHERE pg_catalog.pg_class.relname = %(table_name)s AND pg_catalog.pg_class.relkind = ANY (ARRAY[%(param_1)s, %(param_2)s, %(param_3)s, %(param_4)s, %(param_5)s]) AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) AND pg_catalog.pg_namespace.nspname != %(nspname_1)s
2025-06-10 14:28:21,777 INFO sqlalchemy.engine.Engine [cached since 0.01027s ago] {'table_name': 'menu_hours', 'param_1': 'r', 'param_2': 'p', 'param_3': 'f', 'param_4': 'v', 'param_5': 'm', 'nspname_1': 'pg_catalog'}
2025-06-10 14:28:21,777 INFO sqlalchemy.engine.Engine SELECT pg_catalog.pg_class.relname
FROM pg_catalog.pg_class JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace
WHERE pg_catalog.pg_class.relname = %(table_name)s AND pg_catalog.pg_class.relkind = ANY (ARRAY[%(param_1)s, %(param_2)s, %(param_3)s, %(param_4)s, %(param_5)s]) AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) AND pg_catalog.pg_namespace.nspname != %(nspname_1)s
2025-06-10 14:28:21,777 INFO sqlalchemy.engine.Engine [cached since 0.01212s ago] {'table_name': 'timezones', 'param_1': 'r', 'param_2': 'p', 'param_3': 'f', 'param_4': 'v', 'param_5': 'm', 'nspname_1': 'pg_catalog'}
2025-06-10 14:28:21,781 INFO sqlalchemy.engine.Engine SELECT pg_catalog.pg_class.relname 
FROM pg_catalog.pg_class JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace
WHERE pg_catalog.pg_class.relname = %(table_name)s AND pg_catalog.pg_class.relkind = ANY (ARRAY[%(param_1)s, %(param_2)s, %(param_3)s, %(param_4)s, %(param_5)s]) AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) AND pg_catalog.pg_namespace.nspname != %(nspname_1)s
2025-06-10 14:28:21,781 INFO sqlalchemy.engine.Engine [cached since 0.01482s ago] {'table_name': 'reports', 'param_1': 'r', 'param_2': 'p', 'param_3': 'f', 'param_4': 'v', 'param_5': 'm', 'nspname_1': 'pg_catalog'}
2025-06-10 14:28:21,783 INFO sqlalchemy.engine.Engine

CREATE TABLE stores (
        store_id VARCHAR NOT NULL,
        PRIMARY KEY (store_id)
)


2025-06-10 14:28:21,784 INFO sqlalchemy.engine.Engine [no key 0.00094s] {}
2025-06-10 14:28:21,809 INFO sqlalchemy.engine.Engine CREATE UNIQUE INDEX ix_stores_store_id ON stores (store_id)
2025-06-10 14:28:21,809 INFO sqlalchemy.engine.Engine [no key 0.00031s] {}
2025-06-10 14:28:21,815 INFO sqlalchemy.engine.Engine

CREATE TABLE store_status (
        id SERIAL NOT NULL,
        store_id VARCHAR,
        status VARCHAR,
        timestamp_utc TIMESTAMP WITH TIME ZONE,
        PRIMARY KEY (id),
        CONSTRAINT uq_store_status UNIQUE (store_id, timestamp_utc)
)


2025-06-10 14:28:21,815 INFO sqlalchemy.engine.Engine [no key 0.00036s] {}
2025-06-10 14:28:21,833 INFO sqlalchemy.engine.Engine CREATE INDEX ix_store_status_id ON store_status (id)
2025-06-10 14:28:21,833 INFO sqlalchemy.engine.Engine [no key 0.00024s] {}
2025-06-10 14:28:21,838 INFO sqlalchemy.engine.Engine CREATE INDEX ix_store_status_store_id ON store_status (store_id)
2025-06-10 14:28:21,838 INFO sqlalchemy.engine.Engine [no key 0.00026s] {}
2025-06-10 14:28:21,838 INFO sqlalchemy.engine.Engine CREATE INDEX ix_store_status_status ON store_status (status)
2025-06-10 14:28:21,838 INFO sqlalchemy.engine.Engine [no key 0.00020s] {}
2025-06-10 14:28:21,842 INFO sqlalchemy.engine.Engine CREATE INDEX ix_store_status_timestamp_utc ON store_status (timestamp_utc)
2025-06-10 14:28:21,842 INFO sqlalchemy.engine.Engine [no key 0.00023s] {}
2025-06-10 14:28:21,847 INFO sqlalchemy.engine.Engine

CREATE TABLE menu_hours (
        id SERIAL NOT NULL,
        store_id VARCHAR,
        day_of_week SMALLINT,
        start_time_local VARCHAR,
        end_time_local VARCHAR,
        PRIMARY KEY (id),
        CONSTRAINT uq_menu_hours UNIQUE (store_id, day_of_week)
)


2025-06-10 14:28:21,847 INFO sqlalchemy.engine.Engine [no key 0.00029s] {}
2025-06-10 14:28:21,854 INFO sqlalchemy.engine.Engine CREATE INDEX ix_menu_hours_id ON menu_hours (id)
2025-06-10 14:28:21,854 INFO sqlalchemy.engine.Engine [no key 0.00022s] {}
2025-06-10 14:28:21,858 INFO sqlalchemy.engine.Engine CREATE INDEX ix_menu_hours_store_id ON menu_hours (store_id)
2025-06-10 14:28:21,858 INFO sqlalchemy.engine.Engine [no key 0.00022s] {}
2025-06-10 14:28:21,863 INFO sqlalchemy.engine.Engine 

CREATE TABLE timezones (
        id SERIAL NOT NULL,
        store_id VARCHAR,
        timezone_str VARCHAR,
        PRIMARY KEY (id)
)


2025-06-10 14:28:21,863 INFO sqlalchemy.engine.Engine [no key 0.00026s] {}
2025-06-10 14:28:21,870 INFO sqlalchemy.engine.Engine CREATE UNIQUE INDEX ix_timezones_store_id ON timezones (store_id)
2025-06-10 14:28:21,870 INFO sqlalchemy.engine.Engine [no key 0.00020s] {}
2025-06-10 14:28:21,872 INFO sqlalchemy.engine.Engine CREATE INDEX ix_timezones_id ON timezones (id)
2025-06-10 14:28:21,872 INFO sqlalchemy.engine.Engine [no key 0.00019s] {}
2025-06-10 14:28:21,872 INFO sqlalchemy.engine.Engine CREATE INDEX ix_timezones_timezone_str ON timezones (timezone_str)        
2025-06-10 14:28:21,872 INFO sqlalchemy.engine.Engine [no key 0.00023s] {}
2025-06-10 14:28:21,879 INFO sqlalchemy.engine.Engine 

CREATE TABLE reports (
        report_id VARCHAR NOT NULL,
        status VARCHAR,
        created_by VARCHAR,
        created_at TIMESTAMP WITH TIME ZONE,
        completed_at TIMESTAMP WITH TIME ZONE,
        report_file_path VARCHAR,
        error_message TEXT,
        PRIMARY KEY (report_id),
        CONSTRAINT uq_report_id UNIQUE (report_id)
)


2025-06-10 14:28:21,879 INFO sqlalchemy.engine.Engine [no key 0.00029s] {}
2025-06-10 14:28:21,879 INFO sqlalchemy.engine.Engine CREATE INDEX ix_reports_report_id ON reports (report_id)
2025-06-10 14:28:21,886 INFO sqlalchemy.engine.Engine [no key 0.00020s] {}
2025-06-10 14:28:21,888 INFO sqlalchemy.engine.Engine COMMIT
Database tables created successfully!