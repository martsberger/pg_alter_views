-- Function to safely alter the definition of a view or multiple views. Since all dependent views have to be dropped,
-- their definitions are stored and they are re-created after the target view(s).
-- Note that the ddls are definitions, not create statements

CREATE OR REPLACE FUNCTION alter_views(view_schemas text[], view_names text[], view_ddls text[]) returns void as
$$
DECLARE
  random_schema text := 's_' || md5(random()::text);
BEGIN
  EXECUTE 'CREATE SCHEMA ' || random_schema || ';';
  PERFORM set_config('tmp.search_path', current_setting('search_path'), false);
  PERFORM set_config('search_path', random_schema || ',' || current_setting('search_path'), false);
  PERFORM create_dependency_table();
  PERFORM store_dependencies(view_schemas, view_names);
  PERFORM add_ddl_to_dependency_table();
  PERFORM store_changing_ddl(view_schemas, view_names, view_ddls);
  PERFORM build_temporary_stack();
  PERFORM alter_temporary_stack_schemas();
  PERFORM set_config('search_path', current_setting('tmp.search_path'), false);
  EXECUTE 'DROP SCHEMA ' || random_schema || ' CASCADE;';
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION create_dependency_table() returns void AS
$$
BEGIN
  CREATE TABLE dependent_views (
    schema_name varchar(120),
    view_name varchar(120),
    view_type varchar(120),
    ddl text DEFAULT null,
    depth integer
  );
END;
$$
LANGUAGE plpgsql;

-- Creates all the necessary views that need to be recreated in a separate schema
CREATE OR REPLACE FUNCTION build_temporary_stack() returns void as
$$
DECLARE
  v_curr record;
BEGIN
FOR v_curr IN
(
  SELECT
    ddl,
    view_name,
    view_type
  FROM dependent_views
  ORDER BY depth ASC
) loop
  EXECUTE create_view(v_curr.view_name, v_curr.ddl, v_curr.view_type);
  END loop;

FOR v_curr IN
(
  SELECT
    pg_indexes.indexdef
  FROM
    dependent_views
  JOIN
    pg_indexes ON dependent_views.schema_name=pg_indexes.schemaname AND dependent_views.view_name=pg_indexes.tablename
) loop
EXECUTE strip_schema_from_index_def(v_curr.indexdef);
END loop;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION strip_schema_from_index_def(index_def text) returns text AS
$$
BEGIN
  RETURN regexp_replace(index_def, 'ON [a-zA-Z0-9_$]+\.', 'ON ');
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION create_view(view_name text, ddl text, view_type varchar) returns text as
$$
DECLARE
  str text;
  v_curr record;
BEGIN
  IF view_type = 'v' then
    str := 'CREATE VIEW ' || view_name || ' AS ' || ddl || ';';
  ELSIF view_type = 'm' then
    str := 'CREATE MATERIALIZED VIEW '  || view_name || ' AS ' || ddl || ';';
  END IF;
  RETURN str;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION alter_temporary_stack_schemas() returns void as
$$
DECLARE
  v_curr record;
  view_text text;
BEGIN
FOR v_curr IN
(
  SELECT
    schema_name,
    view_name,
    view_type
  FROM
    dependent_views
) loop
  IF v_curr.view_type = 'v' then
    view_text := 'VIEW';
  ELSIF v_curr.view_type = 'm' then
    view_text := 'MATERIALIZED VIEW';
  END IF;
  EXECUTE 'DROP ' || view_text || ' IF EXISTS ' || v_curr.schema_name || '.' || v_curr.view_name || ' CASCADE;';
  EXECUTE 'ALTER ' || view_text || ' ' || v_curr.view_name || ' SET SCHEMA ' || v_curr.schema_name || ';';
  END loop;
END;
$$
LANGUAGE plpgsql;

-- Compute dependencies from mat views we are updating

CREATE OR REPLACE FUNCTION store_dependencies(mat_view_schemas text[], mat_view_names text[]) returns void as
$$
BEGIN
INSERT INTO dependent_views(schema_name, view_name, view_type, depth)
SELECT obj_schema, obj_name, obj_type, MAX(depth) as depth
  FROM
  (
    WITH RECURSIVE recursive_dependents(obj_schema, obj_name, obj_type, depth) AS
    (
      SELECT
        base.base_schema::varchar COLLATE "C",
        base.base_view::varchar COLLATE "C",
        sub.relkind::varchar COLLATE "C",
        0
      FROM (
        (select
        unnest(mat_view_schemas) base_schema,
        unnest(mat_view_names) base_view) base
        JOIN
        (
        select relname, n.nspname, relkind
        from pg_class c
        join pg_namespace n on c.relnamespace=n.oid
        ) sub ON (sub.nspname=base.base_schema AND sub.relname=base.base_view)
      )

        UNION

      SELECT
        dep_schema::varchar,
        dep_name::varchar,
        dep_type::varchar,
        recursive_dependents.depth + 1
      FROM
        (
          SELECT
            ref_nsp.nspname ref_schema,
            ref_cl.relname ref_name,
            rwr_cl.relkind dep_type,
            rwr_nsp.nspname dep_schema,
            rwr_cl.relname dep_name
          FROM
            pg_depend dep
          JOIN pg_class ref_cl on dep.refobjid = ref_cl.oid
          JOIN pg_namespace ref_nsp on ref_cl.relnamespace = ref_nsp.oid
          JOIN pg_rewrite rwr on dep.objid = rwr.oid
          JOIN pg_class rwr_cl on rwr.ev_class = rwr_cl.oid
          JOIN pg_namespace rwr_nsp on rwr_cl.relnamespace = rwr_nsp.oid
          WHERE dep.deptype = 'n'
                AND dep.classid = 'pg_rewrite'::regclass
        ) deps
        JOIN recursive_dependents on deps.ref_schema = recursive_dependents.obj_schema AND
                                     deps.ref_name = recursive_dependents.obj_name
        WHERE (deps.ref_schema != deps.dep_schema OR deps.ref_name != deps.dep_name) AND
              (deps.dep_type = 'v' OR deps.dep_type = 'm')

    )
    SELECT obj_schema, obj_name, obj_type, depth
    FROM recursive_dependents
    WHERE depth >= 0
    ) t
    GROUP BY obj_schema, obj_name, obj_type
    ORDER BY MAX(depth) DESC
;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION add_ddl_to_dependency_table() returns void AS
$$
BEGIN
  UPDATE
    dependent_views
  SET
    ddl = pg_matviews.definition
  FROM
    pg_matviews
  WHERE
    dependent_views.schema_name=pg_matviews.schemaname AND dependent_views.view_name=pg_matviews.matviewname;

  UPDATE
    dependent_views
  SET
    ddl = views.definition
  FROM
    pg_views views
  WHERE
    dependent_views.schema_name=views.schemaname AND dependent_views.view_name=views.viewname;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION store_changing_ddl(view_schemas text[], view_names text[], view_ddls text[]) returns void as
$$
BEGIN
  UPDATE
    dependent_views
  SET
    ddl = definitions.ddl
  FROM
    (
      SELECT
        unnest(view_schemas) view_schema,
        unnest(view_names) view_name,
        unnest(view_ddls) ddl
    ) definitions
  WHERE
    dependent_views.schema_name = definitions.view_schema AND
    dependent_views.view_name = definitions.view_name;
END
$$
LANGUAGE plpgsql
