#!/bin/bash

warehouses=$1

warehouse_str="bmsql_warehouse"
item_str="bmsql_item"
stock_str="bmsql_stock"
district_str="bmsql_district"
customer_str="bmsql_customer"
history_str="bmsql_history"
oorder_str="bmsql_oorder"
order_line_str="bmsql_order_line"
new_order_str="bmsql_new_order"
config_str="bmsql_config"
check_table="$warehouse_str $item_str $stock_str $district_str $customer_str $history_str $oorder_str $order_line_str $new_order_str $config_str"
item_count=100000
config_count=4
dble_ip="10.186.17.107"
dble_username="test"
dble_password="111111"
database="benchmarksql-test"

#http://10.186.18.11/confluence/display/~guoaomen/LOAD+DATA
echo "Check the number of data rows start..."
for table in ${check_table} ; do
        hive_table=$(mysql -u$dble_username -p$dble_password -P8066 -h$dble_ip -D$database -e "select count(1) from $table;")
        num=`echo $hive_table |awk '{print $2}'`
        if [ $table = $warehouse_str ]; then if [ $num != $warehouses ]; then echo "The number of rows in the $table is incorrect!" ; exit 1; fi
        elif  [ $table = $item_str ]; then if [ $num != $item_count ]; then echo "The number of rows in the $table is incorrect!" ; exit 1; fi
        elif  [ $table = $stock_str ]; then if [ $num != $((item_count*warehouses)) ]; then echo "The number of rows in the $table is incorrect!" ; exit 1; fi
        elif  [ $table = $district_str ]; then if [ $num != $((10*warehouses)) ]; then echo "The number of rows in the $table is incorrect!" ; exit 1; fi
        elif  [ $table = $customer_str ] || [ $table = $history_str ] || [ $table = $oorder_str ]; then if [ $num != $((30000*warehouses)) ]; then echo "The number of rows in the $table is incorrect!" ; exit 1; fi
        elif  [ $table = $new_order_str ]; then if [ $num != $((9000*warehouses)) ]; then echo "The number of rows in the $table is incorrect!" ; exit 1; fi
        elif  [ $table = $config_str ]; then if [ $num != $config_count ]; then echo "The number of rows in the $table is incorrect!" ; exit 1; fi
        fi
done
echo "Check the number of data rows end."


echo "Check data start..."
# Condition 1: W_YTD = sum(D_YTD)
condition_1="SELECT count(1) FROM (SELECT w.w_id, w.w_ytd, d.sum_d_ytd FROM bmsql_warehouse w, (SELECT d_w_id, SUM(d_ytd) sum_d_ytd FROM bmsql_district GROUP BY d_w_id) d WHERE w.w_id = d.d_w_id) as x WHERE w_ytd != sum_d_ytd"
# Condition 2: D_NEXT_O_ID - 1 = max(O_ID) = max(NO_O_ID)
condition_2="SELECT count(1) FROM (SELECT d.d_w_id, d.d_id, d.d_next_o_id, o.max_o_id, no.max_no_o_id FROM bmsql_district d, (SELECT o_w_id, o_d_id, MAX(o_id) max_o_id FROM bmsql_oorder GROUP BY o_w_id, o_d_id) o, (SELECT no_w_id, no_d_id, MAX(no_o_id) max_no_o_id FROM bmsql_new_order GROUP BY no_w_id, no_d_id) no WHERE d.d_w_id = o.o_w_id AND d.d_w_id = no.no_w_id AND d.d_id = o.o_d_id AND d.d_id = no.no_d_id) as x WHERE d_next_o_id - 1 != max_o_id OR d_next_o_id - 1 != max_no_o_id;"
# Condition 3: max(NO_O_ID) - min(NO_O_ID) + 1
condition_3="SELECT count(1) FROM (SELECT no_w_id, no_d_id, MAX(no_o_id) max_no_o_id, MIN(no_o_id) min_no_o_id, COUNT(*) count_no FROM bmsql_new_order GROUP BY no_w_id, no_d_Id) as x WHERE max_no_o_id - min_no_o_id + 1 != count_no;"
# Condition 4: sum(O_OL_CNT)
condition_4="SELECT count(1) FROM (SELECT o.o_w_id, o.o_d_id, o.sum_o_ol_cnt, ol.count_ol FROM (SELECT o_w_id, o_d_id, SUM(o_ol_cnt) sum_o_ol_cnt FROM bmsql_oorder GROUP BY o_w_id, o_d_id) o, (SELECT ol_w_id, ol_d_id, COUNT(*) count_ol FROM bmsql_order_line GROUP BY ol_w_id, ol_d_id) ol WHERE o.o_w_id = ol.ol_w_id AND o.o_d_id = ol.ol_d_id) as x WHERE sum_o_ol_cnt != count_ol;"
# Condition 5: For any row in the ORDER table, O_CARRIER_ID is set to a null
condition_5="SELECT count(1) FROM (SELECT o.o_w_id, o.o_d_id, o.o_id, o.o_carrier_id, no.count_no FROM bmsql_oorder o, (SELECT no_w_id, no_d_id, no_o_id, COUNT(*) count_no FROM bmsql_new_order GROUP BY no_w_id, no_d_id, no_o_id) no WHERE o.o_w_id = no.no_w_id AND o.o_d_id = no.no_d_id AND o.o_id = no.no_o_id) as x WHERE (o_carrier_id IS NULL AND count_no = 0) OR (o_carrier_id IS NOT NULL AND count_no != 0);"
# Condition 6: For any row in the ORDER table, O_OL_CNT must equal the number
condition_6="SELECT count(1) FROM (SELECT o.o_w_id, o.o_d_id, o.o_id, o.o_ol_cnt, ol.count_ol FROM bmsql_oorder o, (SELECT ol_w_id, ol_d_id, ol_o_id, COUNT(*) count_ol FROM bmsql_order_line GROUP BY ol_w_id, ol_d_id, ol_o_id) ol WHERE o.o_w_id = ol.ol_w_id AND o.o_d_id = ol.ol_d_id AND o.o_id = ol.ol_o_id) as x WHERE o_ol_cnt != count_ol;"
# Condition 7: W_YTD = sum(H_AMOUNT)
condition_7="SELECT count(1) FROM (SELECT w.w_id, w.w_ytd, h.sum_h_amount FROM bmsql_warehouse w, (SELECT h_w_id, SUM(h_amount) sum_h_amount FROM bmsql_history GROUP BY h_w_id) h WHERE w.w_id = h.h_w_id) as x WHERE w_ytd != sum_h_amount;"
# Condition 8: D_YTD = sum(H_AMOUNT)
condition_8="SELECT count(1) FROM (SELECT d.d_w_id, d.d_id, d.d_ytd, h.sum_h_amount FROM bmsql_district d, (SELECT h_w_id, h_d_id, SUM(h_amount) sum_h_amount FROM bmsql_history GROUP BY h_w_id, h_d_id) h WHERE d.d_w_id = h.h_w_id AND d.d_id = h.h_d_id) as x WHERE d_ytd != sum_h_amount;"
check_sql=("$condition_1"
                   "$condition_2"
                   "$condition_3"
                   "$condition_4"
                   "$condition_5"
                   "$condition_6"
                   "$condition_7"
                   "$condition_8")
IFS=""
for sql in ${check_sql[*]}
do
                hive_table=$(mysql -u$dble_username -p$dble_password -P8066 -h$dble_ip -D$database -e "$sql")
                num=`echo $hive_table |awk '{print $2}'`
                if [ -n "$num" ]; then echo $sql" result is incorrect!" ; exit 1; fi
done

echo "Check data end."
