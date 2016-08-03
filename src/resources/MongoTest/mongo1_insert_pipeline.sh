#!/bin/bash
#参数1是客户端数，参数2是每天插入的记录数
CLIENT_NUM=$1
RECORD_NUM=$2

tmp="1\n"
month_days=(31 28 31 30 31 30 31 31 30 31 30 31)
for((i=1;i<$CLIENT_NUM;i++))
do
{
	tmp=${tmp}${tmp}
}
done
mkfifo fd2
exec 9<>fd2
echo -n -e "${tmp}" 1>&9
for((i=1;i<13;i++))
do
read -u 9
{
	pos=`expr $i - 1`
	/opt/PostgreSQL/9.6/bin/psql -U postgres -d test -h localhost -c "select mongo1_insert(${RECORD_NUM},'2016-${i}-01',${month_days[${pos}]});"
	#/opt/PostgreSQL/9.6/bin/psql -U postgres -d test -h localhost -c "select ${i}, '2016-${i}-01', ${month_days[${pos}]};"
	sleep 2
	echo -ne "1\n" 1>&9
} &
done
wait
rm -f fd2
