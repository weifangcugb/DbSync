#!/bin/bash
#参数1必须能被12整除,参数1是客户端数,参数2是每天插入的记录数
CLIENT_NUM=$1
RECORD_NUM=$2

month_days=(31 28 31 30 31 30 31 31 30 31 30 31)
month=12
pos=0
val=`expr $month / $CLIENT_NUM`
for((i=1;i<=${CLIENT_NUM};i++))
do
{
	total=0
	for((j=1;j<=${val};j++))
	do
	{
		total=`expr $total + ${month_days[$pos]}`
		pos=`expr $pos + 1`
	}
	done
	/opt/PostgreSQL/9.6/bin/psql -U postgres -d test -h localhost -c "select mongo1_insert(${RECORD_NUM},'2016-`expr $pos - $val + 1`-01',${total});"
	#/opt/PostgreSQL/9.6/bin/psql -U postgres -d test -h localhost -c "select ${i},'2016-`expr $pos - $val + 1`-01', ${total} ;"
}
done
