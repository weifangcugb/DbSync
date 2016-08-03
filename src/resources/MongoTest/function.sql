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
	--sTime := sTime - (sTime % (3600*24));
	eTime := sTime + oneDay;
	FOR j IN 1..days LOOP
		raise info 'start time is %', sTime;
		raise info 'end time is %', eTime;
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
	--IF year % 4 = 0 THEN
		perform createTable_month(year,2,29);
	--ELSE
	--	select createTable_month(year,2,28);
	--END IF;
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

DROP TABLE IF EXISTS mongo1 CASCADE;

create table mongo1(userid bigint, problemid bigint, finishtime bigint, result boolean, duration int);

select createTable_year(2016);

CREATE OR REPLACE FUNCTION mongo1_insert_trigger() RETURNS TRIGGER AS 
$BODY$ 
    declare
	day varchar;
	year int;
    begin 
	year := 2016;
	day := to_char(to_timestamp(NEW.finishtime), 'YYYY-MM-DD');
	day := year || substring(day from 6 for 2) || substring(day from 9 for 2);

	IF day = '20160101' THEN INSERT INTO mongo1_20160101 values (NEW.*);
	ELSEIF day = '20160102' THEN INSERT INTO mongo1_20160102 values (NEW.*);
	ELSEIF day = '20160103' THEN INSERT INTO mongo1_20160103 values (NEW.*);
	ELSEIF day = '20160104' THEN INSERT INTO mongo1_20160104 values (NEW.*);
	ELSEIF day = '20160105' THEN INSERT INTO mongo1_20160105 values (NEW.*);
	ELSEIF day = '20160106' THEN INSERT INTO mongo1_20160106 values (NEW.*);
	ELSEIF day = '20160107' THEN INSERT INTO mongo1_20160107 values (NEW.*);
	ELSEIF day = '20160108' THEN INSERT INTO mongo1_20160108 values (NEW.*);
	ELSEIF day = '20160109' THEN INSERT INTO mongo1_20160109 values (NEW.*);
	ELSEIF day = '20160110' THEN INSERT INTO mongo1_20160110 values (NEW.*);
	ELSEIF day = '20160111' THEN INSERT INTO mongo1_20160111 values (NEW.*);
	ELSEIF day = '20160112' THEN INSERT INTO mongo1_20160112 values (NEW.*);
	ELSEIF day = '20160113' THEN INSERT INTO mongo1_20160113 values (NEW.*);
	ELSEIF day = '20160114' THEN INSERT INTO mongo1_20160114 values (NEW.*);
	ELSEIF day = '20160115' THEN INSERT INTO mongo1_20160115 values (NEW.*);
	ELSEIF day = '20160116' THEN INSERT INTO mongo1_20160116 values (NEW.*);
	ELSEIF day = '20160117' THEN INSERT INTO mongo1_20160117 values (NEW.*);
	ELSEIF day = '20160118' THEN INSERT INTO mongo1_20160118 values (NEW.*);
	ELSEIF day = '20160119' THEN INSERT INTO mongo1_20160119 values (NEW.*);
	ELSEIF day = '20160120' THEN INSERT INTO mongo1_20160120 values (NEW.*);
	ELSEIF day = '20160121' THEN INSERT INTO mongo1_20160121 values (NEW.*);
	ELSEIF day = '20160122' THEN INSERT INTO mongo1_20160122 values (NEW.*);
	ELSEIF day = '20160123' THEN INSERT INTO mongo1_20160123 values (NEW.*);
	ELSEIF day = '20160124' THEN INSERT INTO mongo1_20160124 values (NEW.*);
	ELSEIF day = '20160125' THEN INSERT INTO mongo1_20160125 values (NEW.*);
	ELSEIF day = '20160126' THEN INSERT INTO mongo1_20160126 values (NEW.*);
	ELSEIF day = '20160127' THEN INSERT INTO mongo1_20160127 values (NEW.*);
	ELSEIF day = '20160128' THEN INSERT INTO mongo1_20160128 values (NEW.*);
	ELSEIF day = '20160129' THEN INSERT INTO mongo1_20160129 values (NEW.*);
	ELSEIF day = '20160130' THEN INSERT INTO mongo1_20160130 values (NEW.*);
	ELSEIF day = '20160131' THEN INSERT INTO mongo1_20160131 values (NEW.*);
	ELSEIF day = '20160201' THEN INSERT INTO mongo1_20160201 values (NEW.*);
	ELSEIF day = '20160202' THEN INSERT INTO mongo1_20160202 values (NEW.*);
	ELSEIF day = '20160203' THEN INSERT INTO mongo1_20160203 values (NEW.*);
	ELSEIF day = '20160204' THEN INSERT INTO mongo1_20160204 values (NEW.*);
	ELSEIF day = '20160205' THEN INSERT INTO mongo1_20160205 values (NEW.*);
	ELSEIF day = '20160206' THEN INSERT INTO mongo1_20160206 values (NEW.*);
	ELSEIF day = '20160207' THEN INSERT INTO mongo1_20160207 values (NEW.*);
	ELSEIF day = '20160208' THEN INSERT INTO mongo1_20160208 values (NEW.*);
	ELSEIF day = '20160209' THEN INSERT INTO mongo1_20160209 values (NEW.*);
	ELSEIF day = '20160210' THEN INSERT INTO mongo1_20160210 values (NEW.*);
	ELSEIF day = '20160211' THEN INSERT INTO mongo1_20160211 values (NEW.*);
	ELSEIF day = '20160212' THEN INSERT INTO mongo1_20160212 values (NEW.*);
	ELSEIF day = '20160213' THEN INSERT INTO mongo1_20160213 values (NEW.*);
	ELSEIF day = '20160214' THEN INSERT INTO mongo1_20160214 values (NEW.*);
	ELSEIF day = '20160215' THEN INSERT INTO mongo1_20160215 values (NEW.*);
	ELSEIF day = '20160216' THEN INSERT INTO mongo1_20160216 values (NEW.*);
	ELSEIF day = '20160217' THEN INSERT INTO mongo1_20160217 values (NEW.*);
	ELSEIF day = '20160218' THEN INSERT INTO mongo1_20160218 values (NEW.*);
	ELSEIF day = '20160219' THEN INSERT INTO mongo1_20160219 values (NEW.*);
	ELSEIF day = '20160220' THEN INSERT INTO mongo1_20160220 values (NEW.*);
	ELSEIF day = '20160221' THEN INSERT INTO mongo1_20160221 values (NEW.*);
	ELSEIF day = '20160222' THEN INSERT INTO mongo1_20160222 values (NEW.*);
	ELSEIF day = '20160223' THEN INSERT INTO mongo1_20160223 values (NEW.*);
	ELSEIF day = '20160224' THEN INSERT INTO mongo1_20160224 values (NEW.*);
	ELSEIF day = '20160225' THEN INSERT INTO mongo1_20160225 values (NEW.*);
	ELSEIF day = '20160226' THEN INSERT INTO mongo1_20160226 values (NEW.*);
	ELSEIF day = '20160227' THEN INSERT INTO mongo1_20160227 values (NEW.*);
	ELSEIF day = '20160228' THEN INSERT INTO mongo1_20160228 values (NEW.*);
	ELSEIF day = '20160229' THEN INSERT INTO mongo1_20160229 values (NEW.*);
	ELSEIF day = '20160301' THEN INSERT INTO mongo1_20160301 values (NEW.*);
	ELSEIF day = '20160302' THEN INSERT INTO mongo1_20160302 values (NEW.*);
	ELSEIF day = '20160303' THEN INSERT INTO mongo1_20160303 values (NEW.*);
	ELSEIF day = '20160304' THEN INSERT INTO mongo1_20160304 values (NEW.*);
	ELSEIF day = '20160305' THEN INSERT INTO mongo1_20160305 values (NEW.*);
	ELSEIF day = '20160306' THEN INSERT INTO mongo1_20160306 values (NEW.*);
	ELSEIF day = '20160307' THEN INSERT INTO mongo1_20160307 values (NEW.*);
	ELSEIF day = '20160308' THEN INSERT INTO mongo1_20160308 values (NEW.*);
	ELSEIF day = '20160309' THEN INSERT INTO mongo1_20160309 values (NEW.*);
	ELSEIF day = '20160310' THEN INSERT INTO mongo1_20160310 values (NEW.*);
	ELSEIF day = '20160311' THEN INSERT INTO mongo1_20160311 values (NEW.*);
	ELSEIF day = '20160312' THEN INSERT INTO mongo1_20160312 values (NEW.*);
	ELSEIF day = '20160313' THEN INSERT INTO mongo1_20160313 values (NEW.*);
	ELSEIF day = '20160314' THEN INSERT INTO mongo1_20160314 values (NEW.*);
	ELSEIF day = '20160315' THEN INSERT INTO mongo1_20160315 values (NEW.*);
	ELSEIF day = '20160316' THEN INSERT INTO mongo1_20160316 values (NEW.*);
	ELSEIF day = '20160317' THEN INSERT INTO mongo1_20160317 values (NEW.*);
	ELSEIF day = '20160318' THEN INSERT INTO mongo1_20160318 values (NEW.*);
	ELSEIF day = '20160319' THEN INSERT INTO mongo1_20160319 values (NEW.*);
	ELSEIF day = '20160320' THEN INSERT INTO mongo1_20160320 values (NEW.*);
	ELSEIF day = '20160321' THEN INSERT INTO mongo1_20160321 values (NEW.*);
	ELSEIF day = '20160322' THEN INSERT INTO mongo1_20160322 values (NEW.*);
	ELSEIF day = '20160323' THEN INSERT INTO mongo1_20160323 values (NEW.*);
	ELSEIF day = '20160324' THEN INSERT INTO mongo1_20160324 values (NEW.*);
	ELSEIF day = '20160325' THEN INSERT INTO mongo1_20160325 values (NEW.*);
	ELSEIF day = '20160326' THEN INSERT INTO mongo1_20160326 values (NEW.*);
	ELSEIF day = '20160327' THEN INSERT INTO mongo1_20160327 values (NEW.*);
	ELSEIF day = '20160328' THEN INSERT INTO mongo1_20160328 values (NEW.*);
	ELSEIF day = '20160329' THEN INSERT INTO mongo1_20160329 values (NEW.*);
	ELSEIF day = '20160330' THEN INSERT INTO mongo1_20160330 values (NEW.*);
	ELSEIF day = '20160331' THEN INSERT INTO mongo1_20160331 values (NEW.*);
	ELSEIF day = '20160401' THEN INSERT INTO mongo1_20160401 values (NEW.*);
	ELSEIF day = '20160402' THEN INSERT INTO mongo1_20160402 values (NEW.*);
	ELSEIF day = '20160403' THEN INSERT INTO mongo1_20160403 values (NEW.*);
	ELSEIF day = '20160404' THEN INSERT INTO mongo1_20160404 values (NEW.*);
	ELSEIF day = '20160405' THEN INSERT INTO mongo1_20160405 values (NEW.*);
	ELSEIF day = '20160406' THEN INSERT INTO mongo1_20160406 values (NEW.*);
	ELSEIF day = '20160407' THEN INSERT INTO mongo1_20160407 values (NEW.*);
	ELSEIF day = '20160408' THEN INSERT INTO mongo1_20160408 values (NEW.*);
	ELSEIF day = '20160409' THEN INSERT INTO mongo1_20160409 values (NEW.*);
	ELSEIF day = '20160410' THEN INSERT INTO mongo1_20160410 values (NEW.*);
	ELSEIF day = '20160411' THEN INSERT INTO mongo1_20160411 values (NEW.*);
	ELSEIF day = '20160412' THEN INSERT INTO mongo1_20160412 values (NEW.*);
	ELSEIF day = '20160413' THEN INSERT INTO mongo1_20160413 values (NEW.*);
	ELSEIF day = '20160414' THEN INSERT INTO mongo1_20160414 values (NEW.*);
	ELSEIF day = '20160415' THEN INSERT INTO mongo1_20160415 values (NEW.*);
	ELSEIF day = '20160416' THEN INSERT INTO mongo1_20160416 values (NEW.*);
	ELSEIF day = '20160417' THEN INSERT INTO mongo1_20160417 values (NEW.*);
	ELSEIF day = '20160418' THEN INSERT INTO mongo1_20160418 values (NEW.*);
	ELSEIF day = '20160419' THEN INSERT INTO mongo1_20160419 values (NEW.*);
	ELSEIF day = '20160420' THEN INSERT INTO mongo1_20160420 values (NEW.*);
	ELSEIF day = '20160421' THEN INSERT INTO mongo1_20160421 values (NEW.*);
	ELSEIF day = '20160422' THEN INSERT INTO mongo1_20160422 values (NEW.*);
	ELSEIF day = '20160423' THEN INSERT INTO mongo1_20160423 values (NEW.*);
	ELSEIF day = '20160424' THEN INSERT INTO mongo1_20160424 values (NEW.*);
	ELSEIF day = '20160425' THEN INSERT INTO mongo1_20160425 values (NEW.*);
	ELSEIF day = '20160426' THEN INSERT INTO mongo1_20160426 values (NEW.*);
	ELSEIF day = '20160427' THEN INSERT INTO mongo1_20160427 values (NEW.*);
	ELSEIF day = '20160428' THEN INSERT INTO mongo1_20160428 values (NEW.*);
	ELSEIF day = '20160429' THEN INSERT INTO mongo1_20160429 values (NEW.*);
	ELSEIF day = '20160430' THEN INSERT INTO mongo1_20160430 values (NEW.*);
	ELSEIF day = '20160501' THEN INSERT INTO mongo1_20160501 values (NEW.*);
	ELSEIF day = '20160502' THEN INSERT INTO mongo1_20160502 values (NEW.*);
	ELSEIF day = '20160503' THEN INSERT INTO mongo1_20160503 values (NEW.*);
	ELSEIF day = '20160504' THEN INSERT INTO mongo1_20160504 values (NEW.*);
	ELSEIF day = '20160505' THEN INSERT INTO mongo1_20160505 values (NEW.*);
	ELSEIF day = '20160506' THEN INSERT INTO mongo1_20160506 values (NEW.*);
	ELSEIF day = '20160507' THEN INSERT INTO mongo1_20160507 values (NEW.*);
	ELSEIF day = '20160508' THEN INSERT INTO mongo1_20160508 values (NEW.*);
	ELSEIF day = '20160509' THEN INSERT INTO mongo1_20160509 values (NEW.*);
	ELSEIF day = '20160510' THEN INSERT INTO mongo1_20160510 values (NEW.*);
	ELSEIF day = '20160511' THEN INSERT INTO mongo1_20160511 values (NEW.*);
	ELSEIF day = '20160512' THEN INSERT INTO mongo1_20160512 values (NEW.*);
	ELSEIF day = '20160513' THEN INSERT INTO mongo1_20160513 values (NEW.*);
	ELSEIF day = '20160514' THEN INSERT INTO mongo1_20160514 values (NEW.*);
	ELSEIF day = '20160515' THEN INSERT INTO mongo1_20160515 values (NEW.*);
	ELSEIF day = '20160516' THEN INSERT INTO mongo1_20160516 values (NEW.*);
	ELSEIF day = '20160517' THEN INSERT INTO mongo1_20160517 values (NEW.*);
	ELSEIF day = '20160518' THEN INSERT INTO mongo1_20160518 values (NEW.*);
	ELSEIF day = '20160519' THEN INSERT INTO mongo1_20160519 values (NEW.*);
	ELSEIF day = '20160520' THEN INSERT INTO mongo1_20160520 values (NEW.*);
	ELSEIF day = '20160521' THEN INSERT INTO mongo1_20160521 values (NEW.*);
	ELSEIF day = '20160522' THEN INSERT INTO mongo1_20160522 values (NEW.*);
	ELSEIF day = '20160523' THEN INSERT INTO mongo1_20160523 values (NEW.*);
	ELSEIF day = '20160524' THEN INSERT INTO mongo1_20160524 values (NEW.*);
	ELSEIF day = '20160525' THEN INSERT INTO mongo1_20160525 values (NEW.*);
	ELSEIF day = '20160526' THEN INSERT INTO mongo1_20160526 values (NEW.*);
	ELSEIF day = '20160527' THEN INSERT INTO mongo1_20160527 values (NEW.*);
	ELSEIF day = '20160528' THEN INSERT INTO mongo1_20160528 values (NEW.*);
	ELSEIF day = '20160529' THEN INSERT INTO mongo1_20160529 values (NEW.*);
	ELSEIF day = '20160530' THEN INSERT INTO mongo1_20160530 values (NEW.*);
	ELSEIF day = '20160531' THEN INSERT INTO mongo1_20160531 values (NEW.*);
	ELSEIF day = '20160601' THEN INSERT INTO mongo1_20160601 values (NEW.*);
	ELSEIF day = '20160602' THEN INSERT INTO mongo1_20160602 values (NEW.*);
	ELSEIF day = '20160603' THEN INSERT INTO mongo1_20160603 values (NEW.*);
	ELSEIF day = '20160604' THEN INSERT INTO mongo1_20160604 values (NEW.*);
	ELSEIF day = '20160605' THEN INSERT INTO mongo1_20160605 values (NEW.*);
	ELSEIF day = '20160606' THEN INSERT INTO mongo1_20160606 values (NEW.*);
	ELSEIF day = '20160607' THEN INSERT INTO mongo1_20160607 values (NEW.*);
	ELSEIF day = '20160608' THEN INSERT INTO mongo1_20160608 values (NEW.*);
	ELSEIF day = '20160609' THEN INSERT INTO mongo1_20160609 values (NEW.*);
	ELSEIF day = '20160610' THEN INSERT INTO mongo1_20160610 values (NEW.*);
	ELSEIF day = '20160611' THEN INSERT INTO mongo1_20160611 values (NEW.*);
	ELSEIF day = '20160612' THEN INSERT INTO mongo1_20160612 values (NEW.*);
	ELSEIF day = '20160613' THEN INSERT INTO mongo1_20160613 values (NEW.*);
	ELSEIF day = '20160614' THEN INSERT INTO mongo1_20160614 values (NEW.*);
	ELSEIF day = '20160615' THEN INSERT INTO mongo1_20160615 values (NEW.*);
	ELSEIF day = '20160616' THEN INSERT INTO mongo1_20160616 values (NEW.*);
	ELSEIF day = '20160617' THEN INSERT INTO mongo1_20160617 values (NEW.*);
	ELSEIF day = '20160618' THEN INSERT INTO mongo1_20160618 values (NEW.*);
	ELSEIF day = '20160619' THEN INSERT INTO mongo1_20160619 values (NEW.*);
	ELSEIF day = '20160620' THEN INSERT INTO mongo1_20160620 values (NEW.*);
	ELSEIF day = '20160621' THEN INSERT INTO mongo1_20160621 values (NEW.*);
	ELSEIF day = '20160622' THEN INSERT INTO mongo1_20160622 values (NEW.*);
	ELSEIF day = '20160623' THEN INSERT INTO mongo1_20160623 values (NEW.*);
	ELSEIF day = '20160624' THEN INSERT INTO mongo1_20160624 values (NEW.*);
	ELSEIF day = '20160625' THEN INSERT INTO mongo1_20160625 values (NEW.*);
	ELSEIF day = '20160626' THEN INSERT INTO mongo1_20160626 values (NEW.*);
	ELSEIF day = '20160627' THEN INSERT INTO mongo1_20160627 values (NEW.*);
	ELSEIF day = '20160628' THEN INSERT INTO mongo1_20160628 values (NEW.*);
	ELSEIF day = '20160629' THEN INSERT INTO mongo1_20160629 values (NEW.*);
	ELSEIF day = '20160630' THEN INSERT INTO mongo1_20160630 values (NEW.*);
	ELSEIF day = '20160701' THEN INSERT INTO mongo1_20160701 values (NEW.*);
	ELSEIF day = '20160702' THEN INSERT INTO mongo1_20160702 values (NEW.*);
	ELSEIF day = '20160703' THEN INSERT INTO mongo1_20160703 values (NEW.*);
	ELSEIF day = '20160704' THEN INSERT INTO mongo1_20160704 values (NEW.*);
	ELSEIF day = '20160705' THEN INSERT INTO mongo1_20160705 values (NEW.*);
	ELSEIF day = '20160706' THEN INSERT INTO mongo1_20160706 values (NEW.*);
	ELSEIF day = '20160707' THEN INSERT INTO mongo1_20160707 values (NEW.*);
	ELSEIF day = '20160708' THEN INSERT INTO mongo1_20160708 values (NEW.*);
	ELSEIF day = '20160709' THEN INSERT INTO mongo1_20160709 values (NEW.*);
	ELSEIF day = '20160710' THEN INSERT INTO mongo1_20160710 values (NEW.*);
	ELSEIF day = '20160711' THEN INSERT INTO mongo1_20160711 values (NEW.*);
	ELSEIF day = '20160712' THEN INSERT INTO mongo1_20160712 values (NEW.*);
	ELSEIF day = '20160713' THEN INSERT INTO mongo1_20160713 values (NEW.*);
	ELSEIF day = '20160714' THEN INSERT INTO mongo1_20160714 values (NEW.*);
	ELSEIF day = '20160715' THEN INSERT INTO mongo1_20160715 values (NEW.*);
	ELSEIF day = '20160716' THEN INSERT INTO mongo1_20160716 values (NEW.*);
	ELSEIF day = '20160717' THEN INSERT INTO mongo1_20160717 values (NEW.*);
	ELSEIF day = '20160718' THEN INSERT INTO mongo1_20160718 values (NEW.*);
	ELSEIF day = '20160719' THEN INSERT INTO mongo1_20160719 values (NEW.*);
	ELSEIF day = '20160720' THEN INSERT INTO mongo1_20160720 values (NEW.*);
	ELSEIF day = '20160721' THEN INSERT INTO mongo1_20160721 values (NEW.*);
	ELSEIF day = '20160722' THEN INSERT INTO mongo1_20160722 values (NEW.*);
	ELSEIF day = '20160723' THEN INSERT INTO mongo1_20160723 values (NEW.*);
	ELSEIF day = '20160724' THEN INSERT INTO mongo1_20160724 values (NEW.*);
	ELSEIF day = '20160725' THEN INSERT INTO mongo1_20160725 values (NEW.*);
	ELSEIF day = '20160726' THEN INSERT INTO mongo1_20160726 values (NEW.*);
	ELSEIF day = '20160727' THEN INSERT INTO mongo1_20160727 values (NEW.*);
	ELSEIF day = '20160728' THEN INSERT INTO mongo1_20160728 values (NEW.*);
	ELSEIF day = '20160729' THEN INSERT INTO mongo1_20160729 values (NEW.*);
	ELSEIF day = '20160730' THEN INSERT INTO mongo1_20160730 values (NEW.*);
	ELSEIF day = '20160731' THEN INSERT INTO mongo1_20160731 values (NEW.*);
	ELSEIF day = '20160801' THEN INSERT INTO mongo1_20160801 values (NEW.*);
	ELSEIF day = '20160802' THEN INSERT INTO mongo1_20160802 values (NEW.*);
	ELSEIF day = '20160803' THEN INSERT INTO mongo1_20160803 values (NEW.*);
	ELSEIF day = '20160804' THEN INSERT INTO mongo1_20160804 values (NEW.*);
	ELSEIF day = '20160805' THEN INSERT INTO mongo1_20160805 values (NEW.*);
	ELSEIF day = '20160806' THEN INSERT INTO mongo1_20160806 values (NEW.*);
	ELSEIF day = '20160807' THEN INSERT INTO mongo1_20160807 values (NEW.*);
	ELSEIF day = '20160808' THEN INSERT INTO mongo1_20160808 values (NEW.*);
	ELSEIF day = '20160809' THEN INSERT INTO mongo1_20160809 values (NEW.*);
	ELSEIF day = '20160810' THEN INSERT INTO mongo1_20160810 values (NEW.*);
	ELSEIF day = '20160811' THEN INSERT INTO mongo1_20160811 values (NEW.*);
	ELSEIF day = '20160812' THEN INSERT INTO mongo1_20160812 values (NEW.*);
	ELSEIF day = '20160813' THEN INSERT INTO mongo1_20160813 values (NEW.*);
	ELSEIF day = '20160814' THEN INSERT INTO mongo1_20160814 values (NEW.*);
	ELSEIF day = '20160815' THEN INSERT INTO mongo1_20160815 values (NEW.*);
	ELSEIF day = '20160816' THEN INSERT INTO mongo1_20160816 values (NEW.*);
	ELSEIF day = '20160817' THEN INSERT INTO mongo1_20160817 values (NEW.*);
	ELSEIF day = '20160818' THEN INSERT INTO mongo1_20160818 values (NEW.*);
	ELSEIF day = '20160819' THEN INSERT INTO mongo1_20160819 values (NEW.*);
	ELSEIF day = '20160820' THEN INSERT INTO mongo1_20160820 values (NEW.*);
	ELSEIF day = '20160821' THEN INSERT INTO mongo1_20160821 values (NEW.*);
	ELSEIF day = '20160822' THEN INSERT INTO mongo1_20160822 values (NEW.*);
	ELSEIF day = '20160823' THEN INSERT INTO mongo1_20160823 values (NEW.*);
	ELSEIF day = '20160824' THEN INSERT INTO mongo1_20160824 values (NEW.*);
	ELSEIF day = '20160825' THEN INSERT INTO mongo1_20160825 values (NEW.*);
	ELSEIF day = '20160826' THEN INSERT INTO mongo1_20160826 values (NEW.*);
	ELSEIF day = '20160827' THEN INSERT INTO mongo1_20160827 values (NEW.*);
	ELSEIF day = '20160828' THEN INSERT INTO mongo1_20160828 values (NEW.*);
	ELSEIF day = '20160829' THEN INSERT INTO mongo1_20160829 values (NEW.*);
	ELSEIF day = '20160830' THEN INSERT INTO mongo1_20160830 values (NEW.*);
	ELSEIF day = '20160831' THEN INSERT INTO mongo1_20160831 values (NEW.*);
	ELSEIF day = '20160901' THEN INSERT INTO mongo1_20160901 values (NEW.*);
	ELSEIF day = '20160902' THEN INSERT INTO mongo1_20160902 values (NEW.*);
	ELSEIF day = '20160903' THEN INSERT INTO mongo1_20160903 values (NEW.*);
	ELSEIF day = '20160904' THEN INSERT INTO mongo1_20160904 values (NEW.*);
	ELSEIF day = '20160905' THEN INSERT INTO mongo1_20160905 values (NEW.*);
	ELSEIF day = '20160906' THEN INSERT INTO mongo1_20160906 values (NEW.*);
	ELSEIF day = '20160907' THEN INSERT INTO mongo1_20160907 values (NEW.*);
	ELSEIF day = '20160908' THEN INSERT INTO mongo1_20160908 values (NEW.*);
	ELSEIF day = '20160909' THEN INSERT INTO mongo1_20160909 values (NEW.*);
	ELSEIF day = '20160910' THEN INSERT INTO mongo1_20160910 values (NEW.*);
	ELSEIF day = '20160911' THEN INSERT INTO mongo1_20160911 values (NEW.*);
	ELSEIF day = '20160912' THEN INSERT INTO mongo1_20160912 values (NEW.*);
	ELSEIF day = '20160913' THEN INSERT INTO mongo1_20160913 values (NEW.*);
	ELSEIF day = '20160914' THEN INSERT INTO mongo1_20160914 values (NEW.*);
	ELSEIF day = '20160915' THEN INSERT INTO mongo1_20160915 values (NEW.*);
	ELSEIF day = '20160916' THEN INSERT INTO mongo1_20160916 values (NEW.*);
	ELSEIF day = '20160917' THEN INSERT INTO mongo1_20160917 values (NEW.*);
	ELSEIF day = '20160918' THEN INSERT INTO mongo1_20160918 values (NEW.*);
	ELSEIF day = '20160919' THEN INSERT INTO mongo1_20160919 values (NEW.*);
	ELSEIF day = '20160920' THEN INSERT INTO mongo1_20160920 values (NEW.*);
	ELSEIF day = '20160921' THEN INSERT INTO mongo1_20160921 values (NEW.*);
	ELSEIF day = '20160922' THEN INSERT INTO mongo1_20160922 values (NEW.*);
	ELSEIF day = '20160923' THEN INSERT INTO mongo1_20160923 values (NEW.*);
	ELSEIF day = '20160924' THEN INSERT INTO mongo1_20160924 values (NEW.*);
	ELSEIF day = '20160925' THEN INSERT INTO mongo1_20160925 values (NEW.*);
	ELSEIF day = '20160926' THEN INSERT INTO mongo1_20160926 values (NEW.*);
	ELSEIF day = '20160927' THEN INSERT INTO mongo1_20160927 values (NEW.*);
	ELSEIF day = '20160928' THEN INSERT INTO mongo1_20160928 values (NEW.*);
	ELSEIF day = '20160929' THEN INSERT INTO mongo1_20160929 values (NEW.*);
	ELSEIF day = '20160930' THEN INSERT INTO mongo1_20160930 values (NEW.*);
	ELSEIF day = '20161001' THEN INSERT INTO mongo1_20161001 values (NEW.*);
	ELSEIF day = '20161002' THEN INSERT INTO mongo1_20161002 values (NEW.*);
	ELSEIF day = '20161003' THEN INSERT INTO mongo1_20161003 values (NEW.*);
	ELSEIF day = '20161004' THEN INSERT INTO mongo1_20161004 values (NEW.*);
	ELSEIF day = '20161005' THEN INSERT INTO mongo1_20161005 values (NEW.*);
	ELSEIF day = '20161006' THEN INSERT INTO mongo1_20161006 values (NEW.*);
	ELSEIF day = '20161007' THEN INSERT INTO mongo1_20161007 values (NEW.*);
	ELSEIF day = '20161008' THEN INSERT INTO mongo1_20161008 values (NEW.*);
	ELSEIF day = '20161009' THEN INSERT INTO mongo1_20161009 values (NEW.*);
	ELSEIF day = '20161010' THEN INSERT INTO mongo1_20161010 values (NEW.*);
	ELSEIF day = '20161011' THEN INSERT INTO mongo1_20161011 values (NEW.*);
	ELSEIF day = '20161012' THEN INSERT INTO mongo1_20161012 values (NEW.*);
	ELSEIF day = '20161013' THEN INSERT INTO mongo1_20161013 values (NEW.*);
	ELSEIF day = '20161014' THEN INSERT INTO mongo1_20161014 values (NEW.*);
	ELSEIF day = '20161015' THEN INSERT INTO mongo1_20161015 values (NEW.*);
	ELSEIF day = '20161016' THEN INSERT INTO mongo1_20161016 values (NEW.*);
	ELSEIF day = '20161017' THEN INSERT INTO mongo1_20161017 values (NEW.*);
	ELSEIF day = '20161018' THEN INSERT INTO mongo1_20161018 values (NEW.*);
	ELSEIF day = '20161019' THEN INSERT INTO mongo1_20161019 values (NEW.*);
	ELSEIF day = '20161020' THEN INSERT INTO mongo1_20161020 values (NEW.*);
	ELSEIF day = '20161021' THEN INSERT INTO mongo1_20161021 values (NEW.*);
	ELSEIF day = '20161022' THEN INSERT INTO mongo1_20161022 values (NEW.*);
	ELSEIF day = '20161023' THEN INSERT INTO mongo1_20161023 values (NEW.*);
	ELSEIF day = '20161024' THEN INSERT INTO mongo1_20161024 values (NEW.*);
	ELSEIF day = '20161025' THEN INSERT INTO mongo1_20161025 values (NEW.*);
	ELSEIF day = '20161026' THEN INSERT INTO mongo1_20161026 values (NEW.*);
	ELSEIF day = '20161027' THEN INSERT INTO mongo1_20161027 values (NEW.*);
	ELSEIF day = '20161028' THEN INSERT INTO mongo1_20161028 values (NEW.*);
	ELSEIF day = '20161029' THEN INSERT INTO mongo1_20161029 values (NEW.*);
	ELSEIF day = '20161030' THEN INSERT INTO mongo1_20161030 values (NEW.*);
	ELSEIF day = '20161031' THEN INSERT INTO mongo1_20161031 values (NEW.*);
	ELSEIF day = '20161101' THEN INSERT INTO mongo1_20161101 values (NEW.*);
	ELSEIF day = '20161102' THEN INSERT INTO mongo1_20161102 values (NEW.*);
	ELSEIF day = '20161103' THEN INSERT INTO mongo1_20161103 values (NEW.*);
	ELSEIF day = '20161104' THEN INSERT INTO mongo1_20161104 values (NEW.*);
	ELSEIF day = '20161105' THEN INSERT INTO mongo1_20161105 values (NEW.*);
	ELSEIF day = '20161106' THEN INSERT INTO mongo1_20161106 values (NEW.*);
	ELSEIF day = '20161107' THEN INSERT INTO mongo1_20161107 values (NEW.*);
	ELSEIF day = '20161108' THEN INSERT INTO mongo1_20161108 values (NEW.*);
	ELSEIF day = '20161109' THEN INSERT INTO mongo1_20161109 values (NEW.*);
	ELSEIF day = '20161110' THEN INSERT INTO mongo1_20161110 values (NEW.*);
	ELSEIF day = '20161111' THEN INSERT INTO mongo1_20161111 values (NEW.*);
	ELSEIF day = '20161112' THEN INSERT INTO mongo1_20161112 values (NEW.*);
	ELSEIF day = '20161113' THEN INSERT INTO mongo1_20161113 values (NEW.*);
	ELSEIF day = '20161114' THEN INSERT INTO mongo1_20161114 values (NEW.*);
	ELSEIF day = '20161115' THEN INSERT INTO mongo1_20161115 values (NEW.*);
	ELSEIF day = '20161116' THEN INSERT INTO mongo1_20161116 values (NEW.*);
	ELSEIF day = '20161117' THEN INSERT INTO mongo1_20161117 values (NEW.*);
	ELSEIF day = '20161118' THEN INSERT INTO mongo1_20161118 values (NEW.*);
	ELSEIF day = '20161119' THEN INSERT INTO mongo1_20161119 values (NEW.*);
	ELSEIF day = '20161120' THEN INSERT INTO mongo1_20161120 values (NEW.*);
	ELSEIF day = '20161121' THEN INSERT INTO mongo1_20161121 values (NEW.*);
	ELSEIF day = '20161122' THEN INSERT INTO mongo1_20161122 values (NEW.*);
	ELSEIF day = '20161123' THEN INSERT INTO mongo1_20161123 values (NEW.*);
	ELSEIF day = '20161124' THEN INSERT INTO mongo1_20161124 values (NEW.*);
	ELSEIF day = '20161125' THEN INSERT INTO mongo1_20161125 values (NEW.*);
	ELSEIF day = '20161126' THEN INSERT INTO mongo1_20161126 values (NEW.*);
	ELSEIF day = '20161127' THEN INSERT INTO mongo1_20161127 values (NEW.*);
	ELSEIF day = '20161128' THEN INSERT INTO mongo1_20161128 values (NEW.*);
	ELSEIF day = '20161129' THEN INSERT INTO mongo1_20161129 values (NEW.*);
	ELSEIF day = '20161130' THEN INSERT INTO mongo1_20161130 values (NEW.*);
	ELSEIF day = '20161201' THEN INSERT INTO mongo1_20161201 values (NEW.*);
	ELSEIF day = '20161202' THEN INSERT INTO mongo1_20161202 values (NEW.*);
	ELSEIF day = '20161203' THEN INSERT INTO mongo1_20161203 values (NEW.*);
	ELSEIF day = '20161204' THEN INSERT INTO mongo1_20161204 values (NEW.*);
	ELSEIF day = '20161205' THEN INSERT INTO mongo1_20161205 values (NEW.*);
	ELSEIF day = '20161206' THEN INSERT INTO mongo1_20161206 values (NEW.*);
	ELSEIF day = '20161207' THEN INSERT INTO mongo1_20161207 values (NEW.*);
	ELSEIF day = '20161208' THEN INSERT INTO mongo1_20161208 values (NEW.*);
	ELSEIF day = '20161209' THEN INSERT INTO mongo1_20161209 values (NEW.*);
	ELSEIF day = '20161210' THEN INSERT INTO mongo1_20161210 values (NEW.*);
	ELSEIF day = '20161211' THEN INSERT INTO mongo1_20161211 values (NEW.*);
	ELSEIF day = '20161212' THEN INSERT INTO mongo1_20161212 values (NEW.*);
	ELSEIF day = '20161213' THEN INSERT INTO mongo1_20161213 values (NEW.*);
	ELSEIF day = '20161214' THEN INSERT INTO mongo1_20161214 values (NEW.*);
	ELSEIF day = '20161215' THEN INSERT INTO mongo1_20161215 values (NEW.*);
	ELSEIF day = '20161216' THEN INSERT INTO mongo1_20161216 values (NEW.*);
	ELSEIF day = '20161217' THEN INSERT INTO mongo1_20161217 values (NEW.*);
	ELSEIF day = '20161218' THEN INSERT INTO mongo1_20161218 values (NEW.*);
	ELSEIF day = '20161219' THEN INSERT INTO mongo1_20161219 values (NEW.*);
	ELSEIF day = '20161220' THEN INSERT INTO mongo1_20161220 values (NEW.*);
	ELSEIF day = '20161221' THEN INSERT INTO mongo1_20161221 values (NEW.*);
	ELSEIF day = '20161222' THEN INSERT INTO mongo1_20161222 values (NEW.*);
	ELSEIF day = '20161223' THEN INSERT INTO mongo1_20161223 values (NEW.*);
	ELSEIF day = '20161224' THEN INSERT INTO mongo1_20161224 values (NEW.*);
	ELSEIF day = '20161225' THEN INSERT INTO mongo1_20161225 values (NEW.*);
	ELSEIF day = '20161226' THEN INSERT INTO mongo1_20161226 values (NEW.*);
	ELSEIF day = '20161227' THEN INSERT INTO mongo1_20161227 values (NEW.*);
	ELSEIF day = '20161228' THEN INSERT INTO mongo1_20161228 values (NEW.*);
	ELSEIF day = '20161229' THEN INSERT INTO mongo1_20161229 values (NEW.*);
	ELSEIF day = '20161230' THEN INSERT INTO mongo1_20161230 values (NEW.*);
	ELSE INSERT INTO mongo1_20161231 values (NEW.*);
	END IF;

	RETURN NULL;
    end;
$BODY$
language plpgsql;



CREATE TRIGGER insert_mongo1_trigger
    Before INSERT ON mongo1
    FOR EACH ROW EXECUTE PROCEDURE mongo1_insert_trigger();



CREATE OR REPLACE FUNCTION mongo1_query(startDay varchar, endDay varchar, min_avg_duration integer, min_pid_count integer) RETURNS TABLE(pid_count integer,pid integer,avg_duration integer) AS
$BODY$
    declare
	startTime bigint;
	endTime bigint;
    begin
	startTime := extract(epoch FROM date_trunc('second', to_timestamp(startDay, 'YYYY-MM-DD')));
	endTime := extract(epoch FROM date_trunc('second', to_timestamp(endDay, 'YYYY-MM-DD')));
	RETURN  QUERY 
		select * from 
			(select count(problemid) as pid_count, problemid pid, avg(duration) as avg_duration 
				from mongo1 where finishtime between startTime and endTime 
				group by problemid having avg(duration) > min_avg_duration) as pids 
		where pids.pid_count > min_pid_count;
    end;
$BODY$
  LANGUAGE plpgsql;
