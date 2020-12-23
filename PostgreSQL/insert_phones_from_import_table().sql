DROP FUNCTION public.phones_from_temp_table(bigint, text, integer);

CREATE OR REPLACE FUNCTION public.phones_from_temp_table(dj_id bigint, descr text, countload integer)
  RETURNS SETOF text AS
$BODY$ 
    DECLARE
       _cnt_imported bigint := 0;  
       _cnt_inserted_dj_record bigint := 0; 
       _last_insert_id bigint :=0;
       _dj_load_id bigint :=0;
    BEGIN

      if (
           select true
           from project p
           inner join dj dj on dj.project_id = p.id
           where p.shortname not ilike '%test%' and dj.id = $1
         )    
      then  
         delete from import_phones
	 where phone in( 
		select ip.phone 
		from import_phones ip
		inner join dj_record dr on dr.phone = ip.phone
		where dr.dj_id = $1
		);
	 return next cast('Duplicates deleted'  as text);	 
      else 
         return next cast('Test project'  as text);	  	
      end if;
     
    
      SELECT count(*) from import_phones INTO _cnt_imported;
      IF _cnt_imported = 0 THEN RAISE EXCEPTION 'Imported records not found';
      END IF;   
      return next cast('Count records for import ' || _cnt_imported as text);   


      EXECUTE 'CREATE TEMP TABLE temp_dj_record_phone AS 
                            (
				with i as (INSERT INTO dj_record_' || EXTRACT(month FROM now()) || '_' ||  EXTRACT(year FROM now()) || '(dj_id, phone, in_queue)
				                       (select ' || dj_id || ', phone, ''t'' from import_phones) returning id, phone) 
				select id,  phone
				from i 
                           )'; 
     
      SELECT count(*) from temp_dj_record_phone INTO _cnt_inserted_dj_record;
      IF _cnt_inserted_dj_record = 0 THEN RAISE EXCEPTION 'In dj_record nothing insert';
      END IF;   
      return next cast('Inserted into dj_record  ' || _cnt_inserted_dj_record as text);  


      INSERT INTO  dj_load(dj_id, overview, countload) VALUES(dj_id, descr, countload) RETURNING id INTO  _dj_load_id;
      IF _dj_load_id = 0 THEN RAISE EXCEPTION 'dj load not inserted';
      END IF;   
      return next cast('_dj_load_id = ' || _dj_load_id as text) ;
      
      
      EXECUTE 'INSERT INTO dj_record_ext_' || EXTRACT(month FROM now()) || '_' ||  EXTRACT(year FROM now()) ||
		    '(dj_record_id, cause_codes_id, callafterdate, load_id, timezone_id, dop1, dop2, dop3, dop4, dop5)
		     ( SELECT 
			id, 
			(select id from cause_codes where code_name  = ''QUEUE''),
			now(),
			' || _dj_load_id  || ',
			(select coalesce( (select id from time_zone where utc_offset = (select coalesce(timezone::text , ''3''::text)) OR code = (select coalesce(timezone::text , ''MSK''::text))), (select id from time_zone where code = ''MSK''))), 
			dop1, 
			dop2,
			dop3, 
			dop4, 
			dop5
		     from temp_dj_record_phone tdrp 
		     inner join import_phones tip on tip.phone = tdrp.phone
		  );' ;   
		  
      update dj_record_ext dre
      set cause_codes_id = (select id from cause_codes where code_name  = 'BLACK_LIST') 
      FROM dj_record AS dr
      where
           dr.id = dre.dj_record_id and
           phone in( 
		select ip.phone 
		from import_phones ip
		inner join black_list bl on bl.phone = ip.phone
		);
      return next cast('Black list scanned'  as text); 	

      update dj_record_ext dre
      set cause_codes_id = (select id from cause_codes where code_name  = 'BLACK_LIST') 
      FROM dj_record AS dr
      where 
             dr.id = dre.dj_record_id and
             phone in( 
		select ip.phone 
		from import_phones ip
		where (select regexp_matches(ip.phone,  phone_template) from black_list_template ) is not null
		);
      return next cast('Black list with templates scanned'  as text); 	



      drop table temp_dj_record_phone;
      
     return next 'Import ended';
    END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100
  ROWS 1000;

