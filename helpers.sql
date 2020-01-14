DROP FUNCTION IF EXISTS execute_test_function(text) CASCADE;
CREATE FUNCTION execute_test_function(function_name text) RETURNS setof text AS $$
declare
    exception text;
    dummy_exception text := 'DUMMY EXCEPTION';
BEGIN
    BEGIN
        -- pg_tap wants to plan how many tests it's going to run, we tell it to ignore that impulse
        RETURN NEXT no_plan();

        -- Run the tests
        RETURN QUERY EXECUTE 'select ' || function_name || '()';

        -- Finish the tests and clean up.
        RETURN QUERY select * from finish();

        -- Rollback so we don't produce any side effects
        RAISE EXCEPTION '%', dummy_exception;
    EXCEPTION
    WHEN raise_exception THEN
        GET STACKED DIAGNOSTICS exception = MESSAGE_TEXT;
        IF exception != dummy_exception THEN
            RAISE;
        END IF;
    END;
END
$$ LANGUAGE plpgsql;
