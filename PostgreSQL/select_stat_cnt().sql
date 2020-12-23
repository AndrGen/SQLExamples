
CREATE OR REPLACE FUNCTION stat_cnt(
    dj_id bigint)
  RETURNS TABLE(success_cnt int, loaded_cnt int) AS
$BODY$ 
DECLARE
   _success_cnt int;
   _loaded_cnt int;
   
    BEGIN

      perform dblink_connect_u('hostaddr=localhost port=5432 dbname=test user=test password=test');	   


     CREATE TEMP TABLE temp_stat_cnt 
     (
	success_cnt integer,
	loaded_cnt integer
     );

    INSERT INTO temp_stat_cnt(success_cnt, loaded_cnt)
    (
	    SELECT t.success_cnt, t.loaded_cnt 
	    FROM  dblink('select success_cnt, numbers_cnt from calls_stat_view where id = ' || dj_id ) 
	    as t(success_cnt int, loaded_cnt int)
    );
	 
	         

     RETURN QUERY select * from temp_stat_cnt;


      drop table temp_stat_cnt;
      perform dblink_disconnect();
    END;
$BODY$
  LANGUAGE plpgsql VOLATILE;



