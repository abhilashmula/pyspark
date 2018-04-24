import subprocess
import sys
#dir_name="/infa/staging/delta/R11194_pnr_rmk/"
#table_name = "delta_demo"
dir_name = sys.argv[1]
table_name = sys.argv[2]

cmd_get_file_name="hadoop fs -cat " + dir_name + "*.txt | awk 'NR==1{print $1}' "
header = subprocess.check_output(cmd_get_file_name,shell=True)
header_datatype = header.replace("|", " string,")
#table_props = ' tblproperties ("skip.header.line.count"="1")'
create_table = "create external table " + table_name + " (" + header_datatype + " string" + ") ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LOCATION '" + dir_name + "'"

print (create_table)
hive_command = 'hive -e ' + '"' + create_table + '"'
print(30 * '-')
print("Creating table " + table_name)
print(30 * '-')
print(hive_command)
subprocess.check_output(hive_command,shell=True)

