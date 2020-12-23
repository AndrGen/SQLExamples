CREATE OR REPLACE FUNCTION public.q_answers_stat(
    IN _dj_id bigint,
    IN _q_number_show integer,
    IN _condition text,
    IN _q_numbers_filter text,
    IN _q_answers_filter text)
  RETURNS TABLE(id bigint, dj_id bigint, descr text, q_number integer, answers_count bigint, answer_name text) AS
$BODY$ 
DECLARE
   _select_str text := '
			select 
				min(q.id) as id,
				dj.id as dj_id,
				q.descr,  
				max(q.sort_order) as q_number,
				count(qa.answer) as answers_count,
				(select coalesce(rus_text, qa.answer) from question_answer_locale where tag = qa.answer) as answer_name
			from question q 
			inner join app_question_staff aqs on aqs.question_id = q.id 
			inner join app a on a.id = aqs.app_id and not a.archive 
			inner join dialjob dj on a.id = dj.app_id and not dj.archive 
			inner join dialjob_record dr on dr.dj_id = dj.id 
			inner join dialjob_record_ext dre on dre.dialjob_record_id = dr.id and not dre.archive and dre.id in (
			';
 			 
   _where_str text := '
			 )
			left join question_answer qa on dre.id = qa.dialjob_record_ext_id and q.id = qa.question_id  
			where not q.archive
                       ';
  _select_sub_query_str text := '
				(select dre.id
				from question_answer qa
				inner join question q on qa.question_id = q.id and not q.archive
				inner join dialjob_record_ext dre on dre.id = qa.dialjob_record_ext_id and not dre.archive 
				inner join dialjob_record dr on dre.dialjob_record_id = dr.id
                                ';  
  _group_order_sub_query_str text := '
				group by dre.id
				order by dre.id)
				';   

  _groupby_str text := '
			group by q.descr, dj.id, qa.answer
			order by q_number, id;
			'; 
   
  _total_query text;	

  _cnt_filter integer := 0;	

  _i int := 0;	

  _sub_query text;	                
BEGIN

	RAISE NOTICE 'input parameters: % | % | % | % | %', _dj_id ,_q_number_show, _condition, _q_numbers_filter, _q_answers_filter;

	if _dj_id is null or _q_number_show is null or _condition is null or trim(_condition) = '' or _q_numbers_filter is null or trim(_q_numbers_filter) = '' or _q_answers_filter is null or trim(_q_answers_filter) = '' then 
				RAISE Exception 'One of input vars not found'; 
	end if;

        select count(*) from regexp_split_to_table(_q_numbers_filter, ';') into _cnt_filter; 

        LOOP	
		--RAISE NOTICE 'regexp_split_to_table %', (select * from regexp_split_to_table(_q_filter, ';') limit 1 offset _i);
                SELECT concat(
				_select_sub_query_str, 
				' where dj_id = ',
				_dj_id, 
				' and sort_order = ',
				(select * from regexp_split_to_table(_q_numbers_filter, ';') limit 1 offset _i),
				' and answer = ''',
                                (select coalesce(tag, '') from question_answer_locale where rus_text = 
										(select * from regexp_split_to_table(_q_answers_filter, ';') limit 1 offset _i)),
				'''',
				_group_order_sub_query_str,
				_condition,
				_sub_query
			     ) into _sub_query;

		_i := _i + 1; 
		EXIT WHEN (_i = _cnt_filter);
        
        END LOOP;

        _sub_query := left(_sub_query, length(_sub_query) - length(_condition));

	--RAISE NOTICE 'Sub query: %', _sub_query;

        SELECT concat(
			     _select_str,
			     _sub_query,
			     _where_str,
			     ' and dj_id = ',
			     _dj_id,
			     ' and sort_order = ',
			     _q_number_show,
			     _groupby_str
                     ) into _total_query;

        RAISE NOTICE 'Total query: %', _total_query;

        RETURN QUERY
        EXECUTE (_total_query); 
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100
  ROWS 1000;


-- select * from q_answers_stat(326,3,'intersect all','1;2','Первый вариант;Нет');
