CREATE OR REPLACE FUNCTION calculate_date(period interval, is_forward boolean) RETURNS TIMESTAMP AS $body$
BEGIN

	RETURN CASE WHEN is_forward THEN NOW() + period ELSE NOW() - period END;

END;

$body$
LANGUAGE PLPGSQL
IMMUTABLE
;


