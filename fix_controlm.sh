#!/usr/bin/ksh
# Script Name : fix_controlm.sh
# Author      : Sajjan Janardhanan
# Created On  : 12/26/2013
# Description : This script can remove control-m characters from the following
#               - At the end of every record
#               - From the middle of a record, as long as the end of the record has no ^M
#               Param #1 =  absolute path of the main source file
#               Param #2 =  absolute path of the corrected source file, to be created by this script
#               Param #3 =  1|2|3 (OPTIONAL; default value is 1)
#                           1 : for ^M only at the end of the record/line
#                           2 : for ^M in the middle of a record, but not at the end
#                           3 : for ^M both at the end & middle of a record 
#
# Revision History
# ~~~~~~~~~~~~~~~~
# Rev-Dt        Rev-By                	Rev-Desc
# 12/26/2013    Sajjan Janardhanan    	Created new
# 09/25/2014	Sajjan Janardhanan		Added logic around param#3 value = 3

. /dw001/app/edwetl/scripts/common/etl_func_env.sh

# Local Variables
dir_bin=${ETL_SCRIPTS}/common
dir_log=${dir_bin}/log
now_dte=`date +%Y-%m-%d_%T`
script_nme=`basename $0` 
std_abort_msg_txt="> Fatal ERROR encountered! Pls contact EDW support.\n"
log_file_abs_path="${dir_log}/${script_nme}_${now_dte}.log"

{ # <== logging mechanism begin ==
    
    echo "[`date +%Y-%m-%d_%T`] INFO : Script Begin"
    total_parms_cnt=$#
    if [ $total_parms_cnt -eq 3 ]; then
        source_file_name=$1
        corrected_source_file_name=$2
        option=$3
    elif [ $total_parms_cnt -eq 2 ]; then
        source_file_name=$1
        corrected_source_file_name=$2
        option=1
    else
        echo "> Incorrect parameters were passed!"
        echo $std_abort_msg ;exit 1
    fi
    echo "[`date +%Y-%m-%d_%T`] INFO : Param #1 = "${source_file_name}
    echo "[`date +%Y-%m-%d_%T`] INFO : Param #2 = "${corrected_source_file_name}
    echo "[`date +%Y-%m-%d_%T`] INFO : Param #3 = "${option}

    if [ -f ${corrected_source_file_name} ]; then
		echo "[`date +%Y-%m-%d_%T`] Deleting existing file "$corrected_source_file_name
        rm -f ${corrected_source_file_name}
        if [ $? -ne 0 ]; then
            echo $std_abort_msg ;exit 1
        fi
    fi

    if [ ${option} -eq 1 ]; then
		echo "[`date +%Y-%m-%d_%T`] Removing ^M characters in the file"
        `cat ${source_file_name}|tr -d '\r'>${corrected_source_file_name}`
        if [ $? -ne 0 ]; then
            echo $std_abort_msg ;exit 1
        fi
    elif [ ${option} -eq 2 ]; then
        echo "[`date +%Y-%m-%d_%T`] Removing ^M characters in the file"
        while read source_file_record; do
            source_file_record_length=`echo $source_file_record|wc -c`
            source_file_record_clean=`echo $source_file_record|tr -d '\r'`
            source_file_record_clean_length=`echo $source_file_record_clean|wc -c`
            source_file_record_accumulator="${source_file_record_accumulator} ${source_file_record_clean}"
            if [ $source_file_record_length -eq $source_file_record_clean_length ]; then
                echo $source_file_record_accumulator >> ${corrected_source_file_name}
                source_file_record_accumulator=""
            fi
        done < ${source_file_name}
        incomplete_rec_check=`echo $source_file_record_accumulator | wc -c`
        if [ incomplete_rec_check -gt 1 ]; then
            echo $source_file_record_accumulator >> ${corrected_source_file_name}
            echo "[`date +%Y-%m-%d_%T`] WARNING : Incomplete record found in the end"
        fi
	elif [ ${option} -eq 3 ]; then
		echo "[`date +%Y-%m-%d_%T`] Removing ^M characters in the file"
		`cat ${source_file_name}|tr '\r' '\n'|sed '/^$/d' > ${corrected_source_file_name}`
		if [ $? -ne 0 ]; then
            echo $std_abort_msg ;exit 1
        fi
	else
		echo "> Invalid Option!"
		echo $std_abort_msg ;exit 1
    fi

    echo "[`date +%Y-%m-%d_%T`] INFO : Script Completed Successfully"
} > $log_file_abs_path 2>&1 # <== logging mechanism end ==
exit 0