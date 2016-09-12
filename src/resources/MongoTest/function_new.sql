--函数createTable(startYear integer, yearNum integer)创建从startYear开始连续yearNum年的子表

CREATE OR REPLACE FUNCTION mongo1_insert(recordNum integer, startTime varchar, days integer) RETURNS void AS
$BODY$
    declare
	tmp timestamp;
	oneDay bigint;
	sTime bigint;
	eTime bigint;
	sDay varchar;
	eDay varchar;
    begin
	tmp := to_timestamp(startTime, 'YYYY-MM-DD');
	raise info 'tmp id %', tmp;
	oneDay := 3600 * 24;
	sTime := extract(epoch FROM date_trunc('second', to_timestamp(startTime, 'YYYY-MM-DD')));
	eTime := sTime + oneDay;
	FOR j IN 1..days LOOP
		sDay := to_char(to_timestamp(sTime), 'YYYY-MM-DD');
		eDay := to_char(to_timestamp(eTime), 'YYYY-MM-DD');
		raise info 'start day is %', sDay;
		raise info 'end day is %', eDay;
		FOR i IN 1..recordNum LOOP	
			insert into mongo1 values(floor(random()*5000000), floor(random()*10000), floor(random()*((eTime-1)-(sTime+1))+(sTime+1)), random()<0.5, floor(random()*3600));
		END LOOP;
		sTime := sTime + oneDay;
		eTime := eTime + oneDay;		
	END LOOP;        
    end;
$BODY$
  LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION createTable_month(year integer, month integer, days integer) RETURNS void AS
$BODY$
    declare
	sql varchar;
	startDay varchar;
	startTime bigint;
	endTime bigint;
	oneDay bigint;
	tableSuffix varchar;
	num int;
    begin
	oneDay := 3600 * 24;
	startDay := year || '-' || month || '-01';
	raise info 'start day is %', startDay;
	startTime := extract(epoch FROM date_trunc('second', to_timestamp(startDay, 'YYYY-MM-DD')));
	endTime := startTime + oneDay;

	FOR i IN 1..days LOOP
		IF i < 10 THEN
			if month < 10 then
				tableSuffix := year || '0' || month || '0' || i;
			else
				tableSuffix := '' || year || month || '0' || i;
			end if;
		ELSE
			if month < 10 then
				tableSuffix := year || '0' || month || i;
			else
				tableSuffix := '' || year || month || i;
			end if;
		END IF;
		sql := 'create table mongo1_' || tableSuffix || '( check (finishtime >= ' || startTime || ' and finishtime < ' ||  endTime || ' ) ) inherits (mongo1)' ;
		EXECUTE sql;	
		startTime := startTime + oneDay;
		endTime := endTime + oneDay;
	END LOOP;
    end;
$BODY$
  LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION createTable_year(year integer) RETURNS void AS
$BODY$	
    begin
	perform createTable_month(year,1,31);
	perform createTable_month(year,2,29);
	perform createTable_month(year,3,31);
	perform createTable_month(year,4,30);
	perform createTable_month(year,5,31);
	perform createTable_month(year,6,30);
	perform createTable_month(year,7,31);
	perform createTable_month(year,8,31);
	perform createTable_month(year,9,30);
	perform createTable_month(year,10,31);
	perform createTable_month(year,11,30);
	perform createTable_month(year,12,31);
    end;
$BODY$
  LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION createTable(startYear integer, yearNum integer) RETURNS void AS
$BODY$	
    declare
	endYear integer;
    begin
	endYear := startYear + yearNum - 1;
	FOR i IN startYear..endYear LOOP
		perform createTable_year(i);
	END LOOP;
    end;
$BODY$
  LANGUAGE plpgsql;

drop trigger insert_mongo1_trigger on mongo1;

CREATE OR REPLACE FUNCTION mongo1_insert_trigger() RETURNS TRIGGER AS 
$BODY$ 
    declare
	day varchar;
	userid bigint;
	problemid bigint;
	finishtime bigint;
	result boolean;
	duration int;
	sql varchar;
    begin
	userid := NEW.userid;
	problemid := NEW.problemid;
	finishtime := NEW.finishtime;
	result := NEW.result;
	duration := NEW.duration;
	day := to_char(to_timestamp(NEW.finishtime), 'YYYY-MM-DD');
	day := substring(day from 1 for 4) || substring(day from 6 for 2) || substring(day from 9 for 2);

	sql := 'insert into mongo1_' || day || ' values (' || userid || ',' || problemid || ',' || finishtime || ',' || result || ',' || duration || ')';
	EXECUTE sql;

	RETURN NULL;
    end;
$BODY$
language plpgsql;



CREATE TRIGGER insert_mongo1_trigger
    Before INSERT ON mongo1
    FOR EACH ROW EXECUTE PROCEDURE mongo1_insert_trigger();



CREATE OR REPLACE FUNCTION mongo1_query(startDay varchar, endDay varchar, min_avg_duration integer, min_pid_count integer) RETURNS TABLE(problem_count integer,problem_id bigint,average_duration integer) AS
$BODY$
    declare
	startTime bigint;
	endTime bigint;
    begin
	startTime := extract(epoch FROM date_trunc('second', to_timestamp(startDay, 'YYYY-MM-DD')));
	endTime := extract(epoch FROM date_trunc('second', to_timestamp(endDay, 'YYYY-MM-DD')));
	RETURN  QUERY 
		select * from 
			(select count(problemid)::integer, problemid::bigint, avg(duration)::integer 
				from mongo1 where finishtime between startTime and endTime 
				group by problemid having avg(duration) > min_avg_duration) as pids 
		where pids.count > min_pid_count;
    end;
$BODY$
  LANGUAGE plpgsql;
