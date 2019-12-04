#!/bin/sh
RCol='\e[0m'
Red='\e[0;31m'
Yellow='\e[0;32m'

pep8 --version >/dev/null 2>&1 || { echo "please install pep8." >&2; exit 1; }

# check python version
python -c 'import sys; sys.exit(1 if sys.hexversion<0x03000000 else 0)'

if [ $? != 0 ]
then
	REINDENT='py2reindent.py'
else
	REINDENT='py3reindent.py'
fi

exec < /dev/tty

TABFILES=''

for filename in $(git diff --cached --name-only --diff-filter=ACMRTUXB);
do
	if [[ $filename == *.py ]]
	then

		pep8 -q --select=W191 $filename

		if [ $? != 0 ]
		then
			TABFILES="$TABFILES $filename"
		fi

	fi
done

if [[ $TABFILES != "" ]]
then
	echo -e "$Yellow tabs are detected in $TABFILES\n Please run$RCol $Red utils/$REINDENT -n FILENAME$Rcol $Yellow and readd these files$RCol"
	exit 1
fi

RETURNVALUE=0

for filename in $(git diff --cached --name-only --diff-filter=ACMRTUXB);
do
	if [[ $filename == *.py ]]
	then

		pep8 $filename
		RC=$?
		if [ $RETURNVALUE == 0 ] || [ $RC != 0 ]
		then
			RETURNVALUE=$RC
		fi
	
	fi
done

if [[ $RETURNVALUE > 0 ]]
then

	echo -e "$Red Do you want to continue with these errors?$RCol"
	read -p "(y/n) " yn
	case $yn in
		[Yy]* ) exit 0;;
		* ) exit 1;;
	esac
fi
