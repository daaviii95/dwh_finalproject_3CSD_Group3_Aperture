DO $$
DECLARE 
    r RECORD;
BEGIN
    -- Drop all staging, raw, tmp tables
    FOR r IN 
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'public'
        AND (
            tablename LIKE 'stg_%'
            OR tablename LIKE 'raw_%'
            OR tablename LIKE 'tmp_%'
        )
    LOOP
        RAISE NOTICE 'Dropping table: %', r.tablename;
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;

    -- Drop ingestion log
    IF EXISTS (
        SELECT 1 
        FROM pg_tables 
        WHERE schemaname = 'public'
        AND tablename = 'ingestion_log'
    ) THEN
        RAISE NOTICE 'Dropping ingestion_log';
        EXECUTE 'DROP TABLE IF EXISTS ingestion_log CASCADE';
    END IF;
END $$;
