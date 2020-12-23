
CREATE OR REPLACE FUNCTION drop_table_partitions (
  p_table_name text,
  p_date       date DEFAULT current_date
) RETURNS TABLE(result text)
AS
$body$
DECLARE
  v_date           date := p_date;
  v_storage_period bigint := 40;
  v_last_part      text;
  v_part_name      text;   

  c_ record;
BEGIN

  v_date      := v_date - v_storage_period * interval '1 month';
  v_last_part := p_table_name || '#' || to_char(v_date, 'yyyy_mm');

  FOR c_ IN (
    SELECT inhrelid::regclass::text part
      FROM pg_inherits
     WHERE inhparent = p_table_name::regclass
     ORDER BY inhrelid::regclass::text)
  LOOP
    v_part_name = replace(c_.part, '"','');
    EXIT WHEN v_part_name >= v_last_part;
    EXECUTE format('DROP TABLE IF EXISTS %I', v_part_name);
    return QUERY select v_part_name;
  END LOOP;

END;
$body$
LANGUAGE PLPGSQL
SECURITY DEFINER;


CREATE OR REPLACE FUNCTION create_table_partitions_forward (
  p_table_name     text,
  p_forward_period bigint,
  p_is_month_period boolean DEFAULT true,
  p_tablespace text DEFAULT 'pg_default',
  p_date           date DEFAULT current_date
) RETURNS VOID
AS
$body$
DECLARE
  v_date       date := p_date;
  v_table_name text;
  v_date_start text;
  v_date_end   text;
  table_space text := 'pg_default';
BEGIN

  FOR i IN 1 .. p_forward_period
  LOOP
    IF p_is_month_period THEN
		v_table_name := p_table_name || '#' || to_char(v_date, 'yyyy_mm');
		v_date_start := to_char(v_date, 'yyyy-mm-01');
		v_date       := v_date + interval '1 month';
		v_date_end   := to_char(v_date, 'yyyy-mm-01');
	ELSE
		v_table_name := p_table_name || '#' || to_char(v_date, 'yyyy');
		v_date_start := to_char(v_date, 'yyyy-01-01');
		v_date       := v_date + interval '1 year';
		v_date_end   := to_char(v_date, 'yyyy-01-01');
	END IF;
	

    EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (''%s'') TO (''%s'') TABLESPACE %s', v_table_name, p_table_name, v_date_start, v_date_end,
                   CASE table_space WHEN '&&TS_TABLE_PARTITIONS' THEN p_tablespace ELSE table_space END);
  END LOOP;

END;
$body$
LANGUAGE PLPGSQL
SECURITY DEFINER;


CREATE OR REPLACE FUNCTION create_table_partitions_backward (
  p_table_name     text,
  p_backward_period bigint,
  p_is_month_period boolean DEFAULT true,
  p_tablespace text DEFAULT 'pg_default',
  p_date           date DEFAULT current_date
) RETURNS VOID
AS
$body$
DECLARE
  v_date       date := p_date;
  v_table_name text;
  v_date_start text;
  v_date_end   text;
  table_space text := 'pg_default';
BEGIN

  FOR i IN 1 .. p_backward_period
  LOOP
    IF p_is_month_period THEN
		v_table_name := p_table_name || '#' || to_char(v_date, 'yyyy_mm');
		v_date_start := to_char(v_date, 'yyyy-mm-01');
        v_date       := v_date + interval '1 month';
		v_date_end   := to_char(v_date, 'yyyy-mm-01');
        v_date       := v_date - interval '2 month';
	ELSE
		v_table_name := p_table_name || '#' || to_char(v_date, 'yyyy');
		v_date_start := to_char(v_date, 'yyyy-01-01');
        v_date       := v_date + interval '1 year';
       	v_date_end   := to_char(v_date, 'yyyy-01-01');
        v_date       := v_date - interval '2 year';
	END IF;
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (''%s'') TO (''%s'') TABLESPACE %s' , v_table_name, p_table_name, v_date_start, v_date_end, 
                      CASE table_space WHEN '&&TS_TABLE_PARTITIONS' THEN p_tablespace ELSE table_space END);
    
  END LOOP;

END;
$body$
LANGUAGE PLPGSQL
SECURITY DEFINER;



CREATE OR REPLACE FUNCTION process_partitions (
  p_tablespace text DEFAULT 'pg_default'
) RETURNS VOID
AS
$body$
DECLARE
  v_forward_period_month bigint := 12; -- create partition for 12 months
  v_forward_period_year bigint := 2; -- create partition for 2 years
  
  v_backward_period_month bigint := 40; 
  v_backward_period_year bigint := 4; 
BEGIN


  -- Create partitions forward
  PERFORM create_table_partitions_forward('stat_operation', v_forward_period_month, TRUE, p_tablespace);
  PERFORM create_table_partitions_forward('stat_session', v_forward_period_month, TRUE, p_tablespace);
  PERFORM create_table_partitions_forward('stat_surfing', v_forward_period_month, TRUE, p_tablespace);
  
  PERFORM create_table_partitions_forward('logon_transition', v_forward_period_year, false, p_tablespace);
  PERFORM create_table_partitions_forward('operation', v_forward_period_year, false, p_tablespace);
  PERFORM create_table_partitions_forward('user_session', v_forward_period_year, false, p_tablespace);
 
  
    -- Create partitions backward
  PERFORM create_table_partitions_backward('stat_operation', v_backward_period_month, TRUE, p_tablespace);
  PERFORM create_table_partitions_backward('stat_session', v_backward_period_month, TRUE, p_tablespace);
  PERFORM create_table_partitions_backward('stat_surfing', v_backward_period_month, TRUE, p_tablespace);
  
  PERFORM create_table_partitions_backward('logon_transition', v_backward_period_year, false, p_tablespace);
  PERFORM create_table_partitions_backward('operation', v_backward_period_year, false, p_tablespace);
  PERFORM create_table_partitions_backward('user_session', v_backward_period_year, false, p_tablespace);


END;
$body$
LANGUAGE PLPGSQL
SECURITY DEFINER;
