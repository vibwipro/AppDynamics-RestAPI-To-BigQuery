#!/bin/ksh

#**************************************************************************
#Script Name    : SH_Gen_60Min_Data.sh
#Description    : This script will generate data for last 60 Min that will considered for loading to BQ-table
#Created by     : Vibhor Gupta
#Version        Author          Created Date    Comments
#1.0            Vibhor          2020-08-07      Initial version
#***************************************************************************

#****************************************************************


#------Get the list of variables-----------------------#

rundate=`date +%Y%m%d%H%M%S`
Current_path=/home/syslogadmin/aiops/appdynamics/db-outage
GS_path="gs://prod-de-pipeline/appdynamics"
#------Get data----------------------------------------#

#------mBuy-ITDBFEMJBCTAXINVOICE01--------------------#
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_1" mBuy-ITDBFEMJBCTAXINVOICE01 &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_2" "mBuy-ITDBFEMJBCTAXINVOICE01%7CITC1241: itbosski-lx1234.ikea.com: 1521" &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_3" "mBuy-ITDBFEMJBCTAXINVOICE01%7CITC1242: itbosski-lx2917.ikea.com: 1521" &

#------mBuy-ITDBFEMJBCSERVICE01----------------------#
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_4" mBuy-ITDBFEMJBCSERVICE01 &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_5" "mBuy-ITDBFEMJBCSERVICE01%7CITC1241: itbosski-lx1234.ikea.com: 1521" &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_6" "mBuy-ITDBFEMJBCSERVICE01%7CITC1242: itbosski-lx2917.ikea.com: 1521" &

#------mBuy-ITDBFEMJBCMFSEVENT01--------------------#
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_7" mBuy-ITDBFEMJBCMFSEVENT01 &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_8" "mBuy-ITDBFEMJBCMFSEVENT01%7CITC1241: itbosski-lx1234.ikea.com: 1521" &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_9" "mBuy-ITDBFEMJBCMFSEVENT01%7CITC1242: itbosski-lx2917.ikea.com: 1521" &

#------mBuy-ITDBFEMJBCMFSDSUP01--------------------#
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_10" mBuy-ITDBFEMJBCMFSDSUP01 &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_11" "mBuy-ITDBFEMJBCMFSDSUP01%7CITC1241: itbosski-lx1234.ikea.com: 1521" &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_12" "mBuy-ITDBFEMJBCMFSDSUP01%7CITC1242: itbosski-lx2917.ikea.com: 1521" &

#------mBuy-ITDBFEMJBCMFSCUST01--------------------#
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_13" mBuy-ITDBFEMJBCMFSCUST01 &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_14" "mBuy-ITDBFEMJBCMFSCUST01%7CITC1241: itbosski-lx1234.ikea.com: 1521" &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_15" "mBuy-ITDBFEMJBCMFSCUST01%7CITC1242: itbosski-lx2917.ikea.com: 1521" &

#------mBuy-ITDBFEMJBCMFSCOM01--------------------#
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_16" mBuy-ITDBFEMJBCMFSCOM01 &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_17" "mBuy-ITDBFEMJBCMFSCOM01%7CITC1241: itbosski-lx1234.ikea.com: 1521" &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_18" "mBuy-ITDBFEMJBCMFSCOM01%7CITC1242: itbosski-lx2917.ikea.com: 1521" &

#------mBuy-ITDBFEMJBCMFSPAYM01--------------------#
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_19" mBuy-ITDBFEMJBCMFSPAYM01 &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_20" "mBuy-ITDBFEMJBCMFSPAYM01%7CITC1241: itbosski-lx1234.ikea.com: 1521" &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_21" "mBuy-ITDBFEMJBCMFSPAYM01%7CITC1242: itbosski-lx2917.ikea.com: 1521" &

#------mBuy-ITDBFEMJBCMFSRANGE01--------------------#
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_22" mBuy-ITDBFEMJBCMFSRANGE01 &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_23" "mBuy-ITDBFEMJBCMFSRANGE01%7CITC1241: itbosski-lx1234.ikea.com: 1521" &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_24" "mBuy-ITDBFEMJBCMFSRANGE01%7CITC1242: itbosski-lx2917.ikea.com: 1521" &

#------mBuy-ITDBFEMJBCMFSRANGECACHEA01--------------------#
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_25" mBuy-ITDBFEMJBCMFSRANGECACHEA01 &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_26" "mBuy-ITDBFEMJBCMFSRANGECACHEA01%7CITC1241: itbosski-lx1234.ikea.com: 1521" &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_27" "mBuy-ITDBFEMJBCMFSRANGECACHEA01%7CITC1242: itbosski-lx2917.ikea.com: 1521" &

#------mBuy-ITDBFEMJBCMFSRANGECACHEB01--------------------#
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_28" mBuy-ITDBFEMJBCMFSRANGECACHEB01 &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_29" "mBuy-ITDBFEMJBCMFSRANGECACHEB01%7CITC1241: itbosski-lx1234.ikea.com: 1521" &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_30" "mBuy-ITDBFEMJBCMFSRANGECACHEB01%7CITC1242: itbosski-lx2917.ikea.com: 1521" &

#------------------------mBuy-ITDBFEMJBCMFSSALES01_BATCH----------------------------------------------------------------------------------
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_31" mBuy-ITDBFEMJBCMFSSALES01_BATCH &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_32" "mBuy-ITDBFEMJBCMFSSALES01_BATCH%7CITC1251: itbosski-lx1234.ikea.com: 1521" &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_33" "mBuy-ITDBFEMJBCMFSSALES01_BATCH%7CITC1252: itbosski-lx2917.ikea.com: 1521" &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_34" "mBuy-ITDBFEMJBCMFSSALES01_BATCH%7CITC1253: itbosski-lx2991.ikea.com: 1521" &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_35" "mBuy-ITDBFEMJBCMFSSALES01_BATCH%7CITC1254: itbosski-lx2992.ikea.com: 1521" &


#-------------------------mBuy-ITDBFEMJBCMFSSALES01------------------------------------------------------------------------------------------
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_36" mBuy-ITDBFEMJBCMFSSALES01 &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_37" "mBuy-ITDBFEMJBCMFSSALES01%7CITC1251: itbosski-lx1234.ikea.com: 1521" &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_38" "mBuy-ITDBFEMJBCMFSSALES01%7CITC1252: itbosski-lx2917.ikea.com: 1521" &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_39" "mBuy-ITDBFEMJBCMFSSALES01%7CITC1253: itbosski-lx2991.ikea.com: 1521" &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_40" "mBuy-ITDBFEMJBCMFSSALES01%7CITC1254: itbosski-lx2992.ikea.com: 1521" &


#--------------------------mBuy-ITDBSEBCIMFSSALES02-------------------------------------------------------------------------------------------
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_41" "mBuy- ITDBSEBCIMFSSALES02" &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_42" "mBuy- ITDBSEBCIMFSSALES02%7CITC1271: itbosski-lx2924.ikea.com: 1521" &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_43" "mBuy- ITDBSEBCIMFSSALES02%7CITC1272: itbosski-lx2925.ikea.com: 1521" &

#---------------------------mBuy-ITDBFEMJBCMFSSALESMAINT01-------------------------------------------------------------------------------------
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_44" mBuy-ITDBFEMJBCMFSSALESMAINT01 &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_45" "mBuy-ITDBFEMJBCMFSSALESMAINT01%7CITC1261: itbosski-lx1234.ikea.com: 1521" &
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_46" "mBuy-ITDBFEMJBCMFSSALESMAINT01%7CITC1262: itbosski-lx2917.ikea.com: 1521" &

#----------------------------mBuy-ITDBSRUEBCMFSSALES01-------------------------------------------------------------------------------------------
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_47" mBuy-ITDBSRUEBCMFSSALES01 &

#----------------------------ISOM-ITAPSOM--------------------------------------------------------------------------------------------------------
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_48" ISOM-ITAPSOM &

#----------------------------ISOM-ITCNSOM--------------------------------------------------------------------------------------------------------

python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_49" ISOM-ITCNSOM &

#----------------------------ISOM-ITEUSOM----------------------------------------------------------------------------------------------------------
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_50" ISOM-ITEUSOM12c &

#-----------------------------ISOM-ITNASOM---------------------------------------------------------------------------------------------------------
python $Current_path/Gen_60Min_rollup.py "AppDynamics_${rundate}_51" ISOM-ITNASOM &


#-------End -------------------------------------------#

while [ 1 -lt 200 ]
do
    CNT_AppD=$(ps -ef | grep -i 'Gen_60Min_rollup.py' | grep -v grep | wc -l)
    if [ "${CNT_AppD}" -eq 0 ]; then
        break
    fi
    sleep 5
done

#-------Club all records-------------------------------#

cat "AppDynamics_${rundate}_"* >> "AppDynamics_${rundate}"

rm "AppDynamics_${rundate}_"*

#-------Evaluating No of records received from AppDynamics rest API-------#
count=$(< "AppDynamics_${rundate}" wc -l)

if [ $count -lt 5814 ]; then
    python $Current_path/MS_TeamsNotification.py 'AppDynamics - We have not recived data for all 51 services & its instances'
fi

#-------Movng file to GCS------------------------------#

sudo gsutil cp "AppDynamics_${rundate}" $GS_path/"AppDynamics_${rundate}"

sudo python $Current_path/BQ_Load_60Min_rollup.py  --input $GS_path/"AppDynamics_${rundate}" --region europe-west1 --project proj --temp_location gs://prod-de-pipeline/temp --runner DataflowRunner --job_name "appdynamics-${rundate}"

sudo gsutil rm $GS_path/"AppDynamics_${rundate}"

if [ $? -eq 0 ]
then

    rm "AppDynamics_${rundate}"
fi

echo '*************************'
echo 'Finish'


