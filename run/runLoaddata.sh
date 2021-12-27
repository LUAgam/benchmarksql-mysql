#!/bin/bash

if [ $# -lt 1 ] ; then
    echo "usage: $(basename $0) PROPS [OPT VAL [...]]" >&2
    exit 2
fi

PROPS="$1"
shift
if [ ! -f "${PROPS}" ] ; then
    echo "${PROPS}: no such file or directory" >&2
    exit 1
fi

BEFORE_LOAD="tableCreates"
AFTER_LOAD="indexCreates  buildFinish"
dble_ip="10.186.17.107"
dble_username="root"
dble_password="111111"
benchmarksql_home="/home/benchmarksql-5.0"
database="benchmarksql-test"

rm -rf /home/benchmarksql-5.0/csv/bench*
> runTest-load-data.log
bash runDatabaseDestroy.sh "${PROPS}"

#step.1 init csv

echo "init csv start..."
start=$(date +%s)
for step in ${BEFORE_LOAD} ; do
    ./runSQL.sh "${PROPS}" $step
done
./runLoader.sh "${PROPS}" $*


end_1=$(date +%s)
take_1=$(( end_1 - start ))
echo "init csv end. cost ${take_1} seconds"


#step.4 init key
echo "init key start..."
for step in ${AFTER_LOAD} ; do
    ./runSQL.sh "${PROPS}" $step
done
echo "init key end."

#step.2 split csv
echo "split csv start..."
start=$(date +%s)

mysql -u$dble_username -p$dble_password -P9066 -h$dble_ip -e "split_loaddata $benchmarksql_home/csv/benchmarksql.cust-hist.csv $benchmarksql_home/csv/split/ -s$database -tbmsql_history" -vvv > logs/split.log &
mysql -u$dble_username -p$dble_password -P9066 -h$dble_ip -e "split_loaddata $benchmarksql_home/csv/benchmarksql.customer.csv $benchmarksql_home/csv/split/ -s$database -tbmsql_customer" -vvv >> logs/split.log &
mysql -u$dble_username -p$dble_password -P9066 -h$dble_ip -e "split_loaddata $benchmarksql_home/csv/benchmarksql.district.csv $benchmarksql_home/csv/split/ -s$database -tbmsql_district" -vvv >> logs/split.log &
mysql -u$dble_username -p$dble_password -P9066 -h$dble_ip -e "split_loaddata $benchmarksql_home/csv/benchmarksql.new-order.csv $benchmarksql_home/csv/split/ -s$database -tbmsql_new_order" -vvv >> logs/split.log &
mysql -u$dble_username -p$dble_password -P9066 -h$dble_ip -e "split_loaddata $benchmarksql_home/csv/benchmarksql.order.csv $benchmarksql_home/csv/split/ -s$database -tbmsql_oorder" -vvv >> logs/split.log &
mysql -u$dble_username -p$dble_password -P9066 -h$dble_ip -e "split_loaddata $benchmarksql_home/csv/benchmarksql.stock.csv $benchmarksql_home/csv/split/ -s$database -tbmsql_stock" -vvv >> logs/split.log &
mysql -u$dble_username -p$dble_password -P9066 -h$dble_ip -e "split_loaddata $benchmarksql_home/csv/benchmarksql.warehouse.csv $benchmarksql_home/csv/split/ -s$database -tbmsql_warehouse" -vvv >> logs/split.log &
mysql -u$dble_username -p$dble_password -P9066 -h$dble_ip -e "split_loaddata $benchmarksql_home/csv/benchmarksql.order-line.csv $benchmarksql_home/csv/split/ -s$database -tbmsql_order_line" -vvv >> logs/split.log &

wait


#step.3 loda csv
echo "loda csv start..."


#此处需要根据配置自行补充
#db_1
mysql -uaction -paction --local-infile=1 -P3306 -h10.186.17.101 -Ddh_dn_1 -e "LOAD DATA LOCAL INFILE '$benchmarksql_home/csv/benchmarksql.config.csv' INTO TABLE bmsql_config FIELDS TERMINATED BY ','" -vvv > logs/load-an1.log &
mysql -uaction -paction --local-infile=1 -P3306 -h10.186.17.101 -Ddh_dn_1 -e "LOAD DATA LOCAL INFILE '$benchmarksql_home/csv/benchmarksql.item.csv' INTO TABLE bmsql_item FIELDS TERMINATED BY ','" -vvv >> logs/load-an1.log &
mysql -uaction -paction --local-infile=1 -P3306 -h10.186.17.101 -Ddh_dn_1 -e "LOAD DATA LOCAL INFILE '$benchmarksql_home/csv/split/benchmarksql.cust-hist.csv-an1.csv' INTO TABLE bmsql_history FIELDS TERMINATED BY ','" -vvv >> logs/load-an1.log &
mysql -uaction -paction --local-infile=1 -P3306 -h10.186.17.101 -Ddh_dn_1 -e "LOAD DATA LOCAL INFILE '$benchmarksql_home/csv/split/benchmarksql.customer.csv-an1.csv' INTO TABLE bmsql_customer FIELDS TERMINATED BY ','" -vvv >> logs/load-an1.log &
mysql -uaction -paction --local-infile=1 -P3306 -h10.186.17.101 -Ddh_dn_1 -e "LOAD DATA LOCAL INFILE '$benchmarksql_home/csv/split/benchmarksql.district.csv-an1.csv' INTO TABLE bmsql_district FIELDS TERMINATED BY ','" -vvv >> logs/load-an1.log &
mysql -uaction -paction --local-infile=1 -P3306 -h10.186.17.101 -Ddh_dn_1 -e "LOAD DATA LOCAL INFILE '$benchmarksql_home/csv/split/benchmarksql.new-order.csv-an1.csv' INTO TABLE bmsql_new_order FIELDS TERMINATED BY ','" -vvv >> logs/load-an1.log &
mysql -uaction -paction --local-infile=1 -P3306 -h10.186.17.101 -Ddh_dn_1 -e "LOAD DATA LOCAL INFILE '$benchmarksql_home/csv/split/benchmarksql.order.csv-an1.csv' INTO TABLE bmsql_oorder FIELDS TERMINATED BY ','" -vvv >> logs/load-an1.log &
mysql -uaction -paction --local-infile=1 -P3306 -h10.186.17.101 -Ddh_dn_1 -e "LOAD DATA LOCAL INFILE '$benchmarksql_home/csv/split/benchmarksql.order-line.csv-an1.csv' INTO TABLE bmsql_order_line FIELDS TERMINATED BY ','" -vvv >> logs/load-an1.log &
mysql -uaction -paction --local-infile=1 -P3306 -h10.186.17.101 -Ddh_dn_1 -e "LOAD DATA LOCAL INFILE '$benchmarksql_home/csv/split/benchmarksql.stock.csv-an1.csv' INTO TABLE bmsql_stock FIELDS TERMINATED BY ','" -vvv >> logs/load-an1.log &
mysql -uaction -paction --local-infile=1 -P3306 -h10.186.17.101 -Ddh_dn_1 -e "LOAD DATA LOCAL INFILE '$benchmarksql_home/csv/split/benchmarksql.warehouse.csv-an1.csv' INTO TABLE bmsql_warehouse FIELDS TERMINATED BY ','" -vvv >> logs/load-an1.log &

#db_2
mysql -uaction -paction --local-infile=1 -P3306 -h10.186.17.102 -Ddh_dn_2 -e "LOAD DATA LOCAL INFILE '$benchmarksql_home/csv/benchmarksql.config.csv' INTO TABLE bmsql_config FIELDS TERMINATED BY ','" -vvv > logs/load-an2.log &
mysql -uaction -paction --local-infile=1 -P3306 -h10.186.17.102 -Ddh_dn_2 -e "LOAD DATA LOCAL INFILE '$benchmarksql_home/csv/benchmarksql.item.csv' INTO TABLE bmsql_item FIELDS TERMINATED BY ','" -vvv >> logs/load-an2.log &
mysql -uaction -paction --local-infile=1 -P3306 -h10.186.17.102 -Ddh_dn_2 -e "LOAD DATA LOCAL INFILE '$benchmarksql_home/csv/split/benchmarksql.cust-hist.csv-an2.csv' INTO TABLE bmsql_history FIELDS TERMINATED BY ','" -vvv >> logs/load-an2.log &
mysql -uaction -paction --local-infile=1 -P3306 -h10.186.17.102 -Ddh_dn_2 -e "LOAD DATA LOCAL INFILE '$benchmarksql_home/csv/split/benchmarksql.customer.csv-an2.csv' INTO TABLE bmsql_customer FIELDS TERMINATED BY ','" -vvv >> logs/load-an2.log &
mysql -uaction -paction --local-infile=1 -P3306 -h10.186.17.102 -Ddh_dn_2 -e "LOAD DATA LOCAL INFILE '$benchmarksql_home/csv/split/benchmarksql.district.csv-an2.csv' INTO TABLE bmsql_district FIELDS TERMINATED BY ','" -vvv >> logs/load-an2.log &
mysql -uaction -paction --local-infile=1 -P3306 -h10.186.17.102 -Ddh_dn_2 -e "LOAD DATA LOCAL INFILE '$benchmarksql_home/csv/split/benchmarksql.new-order.csv-an2.csv' INTO TABLE bmsql_new_order FIELDS TERMINATED BY ','" -vvv >> logs/load-an2.log &
mysql -uaction -paction --local-infile=1 -P3306 -h10.186.17.102 -Ddh_dn_2 -e "LOAD DATA LOCAL INFILE '$benchmarksql_home/csv/split/benchmarksql.order.csv-an2.csv' INTO TABLE bmsql_oorder FIELDS TERMINATED BY ','" -vvv >> logs/load-an2.log &
mysql -uaction -paction --local-infile=1 -P3306 -h10.186.17.102 -Ddh_dn_2 -e "LOAD DATA LOCAL INFILE '$benchmarksql_home/csv/split/benchmarksql.order-line.csv-an2.csv' INTO TABLE bmsql_order_line FIELDS TERMINATED BY ','" -vvv >> logs/load-an2.log &
mysql -uaction -paction --local-infile=1 -P3306 -h10.186.17.102 -Ddh_dn_2 -e "LOAD DATA LOCAL INFILE '$benchmarksql_home/csv/split/benchmarksql.stock.csv-an2.csv' INTO TABLE bmsql_stock FIELDS TERMINATED BY ','" -vvv >> logs/load-an2.log &
mysql -uaction -paction --local-infile=1 -P3306 -h10.186.17.102 -Ddh_dn_2 -e "LOAD DATA LOCAL INFILE '$benchmarksql_home/csv/split/benchmarksql.warehouse.csv-an2.csv' INTO TABLE bmsql_warehouse FIELDS TERMINATED BY ','" -vvv >> logs/load-an2.log &



wait


echo "load data success."
