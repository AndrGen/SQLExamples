CREATE OR REPLACE FUNCTION grant_generator(schema_name character varying, 
                                           role_name_r character varying,
                                           role_name_rw character varying)
RETURNS text AS
$BODY$
declare
     rw record; msg text;
begin
     msg=''; --init msg variable
     msg = msg || 'SCHEMA: ';

         msg = msg || 'grant usage on schema '|| schema_name ||' to ' || role_name_r;
         execute 'grant usage on schema '|| schema_name ||' to ' || role_name_r;

         msg = msg || 'grant usage on schema '|| schema_name ||' to ' || role_name_rw;
         execute 'grant usage on schema '|| schema_name ||' to ' || role_name_rw;

     msg = msg || 'TABLES: ';
     for rw in select table_name from information_schema.tables where table_schema=schema_name loop
         raise notice 'table name: %',rw.table_name;
         msg=msg || rw.table_name || ', ';
         execute 'grant select on ' || schema_name || '."' || rw.table_name || '" to ' || role_name_r;
        execute 'grant select, update, delete, insert on ' || schema_name || '."' || rw.table_name || '" to ' || role_name_rw;
     end loop;

    msg=msg ||' | SEQUENCES: ';
     for rw in SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema=schema_name loop
         raise notice 'sequence name: %',rw.sequence_name;
         msg=msg || rw.sequence_name || ', ';
         execute 'grant select on ' || schema_name || '."' || rw.sequence_name || '" to ' || role_name_r;
        execute 'grant ALL  on ' || schema_name || '."' || rw.sequence_name || '" to ' || role_name_rw;
     end loop; 
    
    msg=msg ||' | FUNCTIONS: ';
     for rw in
    SELECT format('%I.%I(%s)', ns.nspname, p.proname, oidvectortypes(p.proargtypes)) as func_name 
        FROM pg_proc p INNER JOIN pg_namespace ns ON (p.pronamespace = ns.oid)
        WHERE ns.nspname = schema_name 
    loop
         raise notice 'function name: %',rw.func_name;
         msg=msg || rw.func_name || ', ';
         execute 'grant EXECUTE  on function ' || rw.func_name || ' to ' || role_name_r;
        execute 'grant ALL  on function ' || rw.func_name || ' to ' || role_name_rw;
     end loop; 


     return msg;
end;
$BODY$
LANGUAGE 'plpgsql' VOLATILE;


--example usage:
--select grant_generator('test', 'TEST_R', 'TEST_RW')