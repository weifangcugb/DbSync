#!/bin/bash
#参数1是客户端数，参数2是每天插入的记录数，参数3是年份，参数4是数据库名，参数5是数据库所属用户
CLIENT_NUM=$1
RECORD_NUM=$2
YEAR=$3
DATABASE=$4
USER=$5

tmp=`expr $YEAR % 4`
a=0
if [ ${tmp} == ${a} ]
then
	month_days=(31 29 31 30 31 30 31 31 30 31 30 31)
else
   	month_days=(31 28 31 30 31 30 31 31 30 31 30 31)
fi

for((i=1;i<=$CLIENT_NUM;i++))
do
{
	tmp=${tmp}"1\n"
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
	/opt/PostgreSQL/9.6/bin/psql -U ${USER} -d ${DATABASE} -h localhost -c "select mongo1_insert(${RECORD_NUM},'${YEAR}-${i}-01',${month_days[${pos}]});"
	#/opt/PostgreSQL/9.6/bin/psql -U ${USER} -d ${DATABASE} -h localhost -c "select ${RECORD_NUM},'${YEAR}-${i}-01',${month_days[${pos}]};"
	sleep 5
	echo -ne "1\n" 1>&9
} &
done
wait
YEAR=`expr $YEAR + 1`

rm -f fd2
