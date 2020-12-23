CREATE OR REPLACE FUNCTION copy_data() RETURNS TEXT AS $body$
DECLARE
    err text;
BEGIN
	BEGIN 
      insert into user(user_id, value)
      select user_id, value from user_info;
    EXCEPTION
  		WHEN others THEN
            GET STACKED DIAGNOSTICS err = PG_EXCEPTION_CONTEXT;
            RETURN 'user error: ' || SQLSTATE || ', ' || SQLERRM || ' &context: ' || err;
    END;
    
	RETURN 'user OK';

END;      
$body$
LANGUAGE PLPGSQL;