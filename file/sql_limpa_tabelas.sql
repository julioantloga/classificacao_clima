DO $$
DECLARE
    r RECORD;
BEGIN
    -- Desabilita constraints de foreign key temporariamente
    EXECUTE 'SET session_replication_role = replica';

    -- Trunca todas as tabelas do schema 'public'
    FOR r IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
    LOOP
        EXECUTE 'TRUNCATE TABLE public.' || quote_ident(r.tablename) || ' RESTART IDENTITY CASCADE';
    END LOOP;

    -- Reativa as constraints
    EXECUTE 'SET session_replication_role = origin';
END $$;
