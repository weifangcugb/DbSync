#!/bin/bash
#参数1是线程数，参数2是每天插入的记录数
THREAD_NUM=$1
RECORD_NUM=$2

month_days=(31 28 31 30 31 30 31 31 30 31 30 31)

for((i=1;i<=12;))
do
{
	for((j=1;j<=${THREAD_NUM};j++,i++))
	do
	{
	
		{
			/opt/PostgreSQL/9.6/bin/psql -U postgres -d test -h localhost -c "select mongo1_insert(${RECORD_NUM},'2016-${i}-01',${month_days[${i}]});"
			#/opt/PostgreSQL/9.6/bin/psql -U postgres -d test -h localhost -c "select ${i}, '2016-${i}-01', ${month_days[`expr ${i} - 1`]};"		
			sleep 2
		}&
	}
	done
	wait
}
done
