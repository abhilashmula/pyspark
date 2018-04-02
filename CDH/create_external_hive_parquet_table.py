import subprocess
dir_name="/infa/staging/sqoopcustomers/"
table_name = "sqoopcustomers"

cmd_get_file_name="hadoop fs -ls " + dir_name + "*.parquet | awk -F'/' '{print $NF}' | head -1"
file_name = subprocess.check_output(cmd_get_file_name,shell=True)
print (file_name)

file_path = dir_name + file_name
print (file_path)

create_table_command ="create external table " + str(table_name) + " like parquet '" + str(file_path) + "'" + " stored as parquet location '" + str(dir_name) + "'"
impala_command = 'impala-shell -q ' + '"' + create_table_command + '"'
print(30 * '-')
print("Creating table " + table_name)
print(30 * '-')
print(impala_command)
subprocess.check_output(impala_command,shell=True)