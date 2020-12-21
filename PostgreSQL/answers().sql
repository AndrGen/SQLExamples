DROP FUNCTION public.a_tbl(bigint);

CREATE OR REPLACE FUNCTION public.a_tbl(dj_id bigint)
  RETURNS TABLE(dj_record_ext_id text, q1 text, q2 text, q3 text, q4 text, q5 text, q6 text, q7 text, q8 text, q9 text, q10 text, q11 text, q12 text, q13 text, q14 text, q15 text, q16 text, q17 text, q18 text, q19 text, q20 text , modifydate timestamp without time zone) AS
$$     
DECLARE
  _str text;
    BEGIN 

     CREATE TEMP TABLE temp_a_tbl AS 
                            (
				with i as (
                            select 
					        dre.id::text as rowid,
					        q.descr as descr, 
					        q.name as attribute, 
					        qa.answer as value,
							GREATEST(qa.createdate,q.modifydate) as modifydateMain,
					        case when regexp_replace(q.name, '\D', '', 'g') is not null 
					             then cast(regexp_replace(q.name, '\D', '', 'g') as int)
					             else -1
					        end as qnumber,
                                                q.sort_order	 
						from question q 
						inner join app_question_staff aqs on aqs.question_id = q.id 
						inner join app a on a.id = aqs.app_id and not a.archive 
						inner join dj dj on a.id = dj.app_id and not dj.archive and dj.id = $1
						inner join dj_record dr on dr.dj_id = dj.id 
						inner join dj_record_ext dre on dre.dj_record_id = dr.id and not dre.archive 
						left join question_answer qa on dre.id = qa.dj_record_ext_id and q.id = qa.question_id
						where not q.archive
						order by sort_order, qnumber
				) 
				select rowid, attribute, value, descr, qnumber, sort_order , modifydateMain 
				from i );

--select string_agg(attribute,' text, ') from temp_a_tbl INTO _str;


CREATE TEMP TABLE temp_a_tbl_cstab AS 
(
    with i as 
	(	
		SELECT *
		FROM crosstab(
		  'select  rowid, attribute, value 
		   from temp_a_tbl
		   where attribute  in (' || (select '''' ||  string_agg(attribute,''', ''') || ''''  from temp_a_tbl ) || ')
		   order by 1, sort_order 
		   ')
		  as (dj_record_ext_id text, q1 text, q2 text, q3 text, q4 text, q5 text, q6 text, q7 text, q8 text, q9 text, q10 text, q11 text, q12 text, q13 text, q14 text, q15 text, q16 text, q17 text, q18 text, q19 text, q20 text)
	) 
    select * from i
);


RETURN QUERY
          select tac.*,ta.modifydateMain from temp_a_tbl_cstab tac
          left join  temp_a_tbl ta on tac.dj_record_ext_id=ta.rowid;
          
 



drop table temp_a_tbl_cstab;
drop table temp_a_tbl;
     
    END;
$$LANGUAGE plpgsql;

  
ALTER FUNCTION public.a_tbl(bigint)
  OWNER TO postgres;
GRANT EXECUTE ON FUNCTION public.a_tbl(bigint) TO public;
GRANT EXECUTE ON FUNCTION public.a_tbl(bigint) TO postgres;

