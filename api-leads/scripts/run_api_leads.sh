#!/bin/bash

flag_reprocess=0

for i in "$@"
do
	case $i in
	-d1=*|--date1=*)
	DATE1_IN="${i#*=}"
	shift # past argument=value
	;;
      	-d2=*|--date2=*)
	DATE2_IN="${i#*=}"
	shift # past argument=value
	;;
	esac
done

if [ ${#DATE1_IN} -gt 0 ]
then
	flag_reprocess=1
fi


d=$(date -d "`date`" +%d)
#d=02

if [ "$d" = "01" ]
then
	month=$(date -d "`date +%Y%m01` -1 month" +%Y-%m)
	start=$(date -d "`date +%Y%m01` -1 month" +%Y-%m-%d)
	end=$(date -d "`date +%Y%m01` -1 day" +%Y-%m-%d)
else
	month=$(date -d "`date +%Y%m01`" +%Y-%m)
	start=$(date -d "`date +%Y%m01`" +%Y-%m-%d)
	end=$(date -d "`date` -1 day" +%Y-%m-%d)
fi


if [ $flag_reprocess -eq 1 ]
then
	start=$DATE1_IN
	end=$DATE1_IN
	month=$( echo $DATE1_IN | cut -c1-7)
fi

echo $month
echo $start
echo $end

python3.5 ../src/main.py $month $start $end


#python2.6 /opt/dw_schibsted/yapo_bi/dw_blocketdb/etl_xiti/api/leads/xiti_leads.py 2018-08 2018-08-01 2018-08-29
#python2.6 /opt/dw_schibsted/yapo_bi/dw_blocketdb/etl_xiti/api/leads/xiti_leads.py 2018-07 2018-07-01 2018-07-31
#python2.6 /opt/dw_schibsted/yapo_bi/dw_blocketdb/etl_xiti/api/leads/xiti_leads.py 2018-06 2018-06-01 2018-06-30
