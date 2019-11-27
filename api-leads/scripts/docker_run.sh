d=$(date -d "`date`" +%d)

flag_reprocess=0

if [ ${#@} -eq 2 ]
then
	for i in "$@"
	do
		case $i in
	    -d1=*|--date1=*)
	    DATE1_IN="${i#*=}"
	    shift
	    ;;
      -d2=*|--date2=*)
	    DATE2_IN="${i#*=}"
	    shift
	    ;;
		esac
	done

  if [ ${#DATE1_IN} -gt 0 ] && [ ${#DATE2_IN} -gt 0 ]
  then
  	flag_reprocess=1
  fi
fi

start=$(date -d "`date` -1 day" +%Y-%m-%d)
end=$(date -d "`date` -1 day" +%Y-%m-%d)

if [ $flag_reprocess -eq 1 ]
then
  start=$DATE1_IN
  end=$DATE2_IN
fi

echo $start
echo $end

docker-compose up --build --no-start
