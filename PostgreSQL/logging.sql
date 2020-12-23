CREATE TABLE IF NOT EXISTS log (
	message text  NULL,
	comment text null
);



CREATE OR REPLACE FUNCTION insert_log(message_log text,	comment_log text) RETURNS VOID AS $body$
declare 
    db_name text := 'DB_NAME';
    user    text := 'DB_USER';
    pass    text := 'DB_PASS';	
    host    text := 'DB_HOST';
begin
    
    perform dblink_connect('log','hostaddr=' || host || ' port=5432 dbname=' || db_name || ' user= ' || user || ' password=' || pass);
   
    perform dblink('log',  'insert into log(message, comment) VALUES(''' || to_json(replace(message_log, '''', '')) || ''', ''' || to_json(replace(comment_log,'''', '')) || ''')');
   
    perform dblink_disconnect('log');
    
end;
$body$
LANGUAGE PLPGSQL;