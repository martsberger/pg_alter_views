\ir helpers.sql

DROP FUNCTION IF EXISTS test_alter_views() CASCADE;
CREATE FUNCTION test_alter_views() returns setof text AS $TAV$
DECLARE
  test_name text;
  expected_result integer[];
  result integer[];
  exception text;
BEGIN
  test_name := 'Test that alter_views rebuilds a dependent view mat_view -> mat_view';

  CREATE MATERIALIZED VIEW base_view AS
  SELECT UNNEST(ARRAY[1, 2, 3]) as col1,
         UNNEST(ARRAY[4, 5, 6]) as col2;

  CREATE MATERIALIZED VIEW top_view AS
  SELECT col2 from base_view where col1 > 1;

  PERFORM alter_views(array['public'],
                     array['base_view'],
                     array['SELECT UNNEST(ARRAY[2, 1, 3]) as col1, '
                           'UNNEST(ARRAY[4, 5, 6]) as col2']
  );

  result := (SELECT array_agg(col2) FROM top_view);
  expected_result := ARRAY[4, 6];
  return NEXT IS(expected_result, result, test_name);

  DROP MATERIALIZED VIEW base_view CASCADE;

  test_name := 'Test alter_views rebuilds with a view in the stack mat_view->view->mat_view';

  CREATE MATERIALIZED VIEW base_view AS
  SELECT UNNEST(ARRAY[1, 2, 3]) as col1,
         UNNEST(ARRAY[4, 5, 6]) as col2,
         UNNEST(ARRAY[7, 8, 9]) as col3;

  CREATE VIEW middle_view AS
  SELECT col2, col3 from base_view where col1 > 1;

  CREATE MATERIALIZED VIEW top_view AS
  SELECT col3 from middle_view where col2 > 4;

  PERFORM alter_views(array['public'],
                     array['base_view'],
                     array['SELECT UNNEST(ARRAY[2, 1, 3]) as col1, '
                           'UNNEST(ARRAY[4, 5, 6]) as col2, '
                           'UNNEST(ARRAY[7, 8, 9]) as col3']
  );

  result := (SELECT array_agg(col3) FROM top_view);
  expected_result := ARRAY[9];
  return NEXT IS(expected_result, result, test_name);

  DROP MATERIALIZED VIEW base_view CASCADE;

  test_name := 'Test that the top view is recreated with its index view->mat_view';

  CREATE VIEW base_view AS
  SELECT UNNEST(ARRAY[1, 2, 3]) as col1,
         UNNEST(ARRAY[4, 5, 6]) as col2;

  CREATE MATERIALIZED VIEW top_view AS
  SELECT col2 from base_view where col1 > 1;
  CREATE INDEX top_view_col2_indx on top_view (col2);

  PERFORM alter_views(array['public'],
                     array['base_view'],
                     array['SELECT UNNEST(ARRAY[2, 1, 3]) as col1, '
                           'UNNEST(ARRAY[4, 5, 6]) as col2']
  );

  result := (SELECT array_agg(col2) FROM top_view);
  expected_result := ARRAY[4, 6];
  return NEXT IS(expected_result, result, test_name);

  result := array[(select count(*) from pg_indexes where tablename='top_view')];
  expected_result := ARRAY[1];
  return NEXT IS(expected_result, result, test_name);

  DROP VIEW base_view CASCADE;

END
$TAV$ LANGUAGE plpgsql;

SELECT execute_test_function('test_alter_views');
