CREATE OR REPLACE FUNCTION public.translate_ans(IN _ans TEXT)
  RETURNS TEXT AS
  $BODY$     
    BEGIN 

    RETURN (
	     coalesce(
			(
			    select string_agg(rus_text, ', ') 
			    from question_ans_locale 
			    where tag in (
						select regexp_split_to_table(_ans, ';')  
				         )
			), _ans
	             )
	   ); 
    
    END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;


