import argparse
import csv
import logging
from string import Template
import subprocess
import shlex

sql_statement_count_template = Template('select count(*), \'$table\' from &&__USER..$table \n union all \n')


def help_msg():
    """ help to describe the script"""
    help_str = """ TODO
                   """
    return help_str


def get_list_ora_error_to_exclude():
    list_ora_notincluded = ['ORA-00955', 'ORA-00001', 'ORA-01430', 'ORA-01442', 'ORA-02275', 'ORA-02260', 'ORA-02261',
                            'ORA-02264', 'ORA-02443', 'ORA-01451']
    return list_ora_notincluded


def count_reference_substr(data, substr):
    startIndex = 0
    count = 0
    for i in range(len(data)):
        k = data.find(substr, startIndex)
        if k != -1:
            startIndex = k + 1
            count += 1
            k = 0
    return count


def all_ora_to_excluded(output_sql, list_ora_excl):
    counter = []
    for error_ora_id in list_ora_excl:
        num_ora = count_reference_substr(output_sql, error_ora_id)
        counter.append(num_ora)
    return sum(counter)


def is_ok_sql_execution(output_sql):
    # ORA-00955 esiste già non dovrebbe bloccare l'esecuzione
    list_ora_exc = get_list_ora_error_to_exclude()
    status = False
    if (output_sql.find('Error starting') == -1) and (output_sql.rfind('Error starting') == -1):
        status = True
    else:
        count_errno_err = count_reference_substr(output_sql, 'Error starting')
        count_errno_ora = all_ora_to_excluded(output_sql, list_ora_exc)
        if count_errno_err == count_errno_ora:
            # LOG DA CANCELLARE print("      WARNING: " + " / ".join(list_ora_exc[0:]) + " - Probably the set has been executed before ")
            logger.warning(
                "      WARNING: " + " / ".join(list_ora_exc[0:]) + " - Probably the set has been executed before ")
            status = True
        else:
            status = False
    return status


# execute on SQLc the script with sql_conn as connection parameter
def run_sqlplus(sqlplus_script, sql_conn):
    """
    Run a sql command or group of commands against
    a database using sqlplus.
    """

    # command_line = 'sql ba212cnt/ba212cnt@//midevdb03:1521/wedge'
    args_sql = shlex.split(sql_conn)
    # print(args)
    p = subprocess.Popen(args=args_sql, stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    sql_to_execute = sqlplus_script.encode('utf-8', 'ignore')
    (stdout, stderr) = p.communicate(sqlplus_script.encode('utf-8'))
    stdout_lines = stdout.decode('utf-8', 'ignore').split("\n")
    # LOG DA CANCELLARE print(stdout_lines)
    logger.info(stdout_lines)
    return stdout_lines


def sql_string_connection(conf_db):
    # command_line = 'sql ba212cnt/ba212cnt@//midevdb03:1521/wedge'
    sql_conn = 'sql ' + conf_db['User'] + '/' + conf_db['Passwd'] + '@//' + conf_db['hostname'] + ':' + conf_db[
        'port'] + '/' + conf_db['service']
    return sql_conn


def load_db_conf(group_sel):
    list_of_db = []
    with open(config_db) as csv_file:
        fieldnames = ['Group', 'Version', 'Tablesp_data', 'Tablesp_indexType', 'Type', 'Active', 'User', 'Passwd',
                      'hostname', 'port', 'service']
        csv_reader = csv.DictReader(csv_file, delimiter=';')
        line_count = 0
        for row in csv_reader:
            # print(row)
            # print(row['Active'])
            if row['Active'] == 'True':
                # print(row['Group'])
                if row['Group'] == group_sel:
                    list_of_db.append(row)
                    line_count += 1
        # LOG DA CANCELLARE print(f'Read  {line_count} active db configurations.')
        logger.info(f'Read  {line_count} active db configurations.')
        for db_conf in list_of_db:
            # LOG DA CANCELLARE print(db_conf)
            logger.info(db_conf)
    return list_of_db


def sql_execute_script(conf_db, user_cnt, sql_to_execute):
    sql_script_final = []
    statement_sql = ""
    status_ok = True
    label_to_ignore = "ONLINESKIP"
    label_environment = 'DEV'
    final_commit = "\n\ncommit;\n"
    if conf_db['Type'] == 'CNT':
        for line in sql_to_execute:
            line = line.replace('&&__USER.', conf_db['User']).replace(
                '&&__CNTTSIDX.', conf_db['Tablesp_index']).replace('&&__ENV.', label_environment)
            line = line.replace('\\\__USER.', conf_db['User']).replace(
                '\\\__CNTTSIDX.', conf_db['Tablesp_index']).replace('\\\__ENV.', label_environment)
            line = line.replace('&&__USER', conf_db['User'])
            if label_to_ignore in line:
                # LOG DA CANCELLARE print ("SQL statement to ESCLUDE =  " + line)
                logger.warning("SQL statement to ESCLUDE =  " + line)
            else:
                sql_script_final.append(line)

        sql_script_final.append(final_commit)
        # LOG DA CANCELLARE [print(instr_sql) for instr_sql in sql_script_final]
        [logger.info(instr_sql) for instr_sql in sql_script_final]
        statement_sql = "".join(sql_script_final[0:])
    else:
        for line in sql_to_execute:
            line = line.replace('&&__USER.', conf_db['User']).replace(
                '&&__USRTSIDX.', conf_db['Tablesp_index']).replace('&&__USER.', user_cnt).replace('&&__ENV.',
                                                                                                     label_environment)
            line = line.replace('\\\__USER.', conf_db['User']).replace(
                '\\\__USRTSIDX.', conf_db['Tablesp_index']).replace('\\\__USER.', user_cnt).replace('\\\__ENV.',
                                                                                                       label_environment)
            line = line.replace('&&__USER', conf_db['User'])
            sql_script_final.append(line)
        sql_script_final.append(final_commit)
        # LOG DA CANCELLARE [print(instr_sql) for instr_sql in sql_script_final]
        [logger.info(instr_sql) for instr_sql in sql_script_final]
        statement_sql = "".join(sql_script_final[0:])
    sql_connection = sql_string_connection(conf_db)
    # LOG DA CANCELLARE print("ESECUZIONE SQL with connection = " + sql_connection)
    logger.info("ESECUZIONE SQL with connection = " + sql_connection)
    output = run_sqlplus(statement_sql, sql_connection)
    status_ok = check_status_sql_execution_sql(output, is_ok_sql_execution)
    # execute
    # check if there ia some ORA error
    # output = run_sqlplus(sql_script_final, conf_db)
    return status_ok


def check_status_sql_execution_sql(output_sql, algorithm_check):
    output_total = "".join(output_sql[0:])
    status_ok = algorithm_check(output_total)
    return status_ok


def read_script_sql(dir_branch, filename_script):
    script_file = dir_branch + '\\' + filename_script
    a_file = open(script_file, "r", encoding='utf-8', errors='ignore')
    list_of_lines = a_file.readlines()
    a_file.close()
    filter_list = []
    for line in list_of_lines:
        if line.find('ACCEPT') == -1:
            filter_list.append(line)
    return filter_list


def lettura_file_configurazione_tabelle(file_configurazione):
    sql_script = []

    with open(file_configurazione, mode='r') as csv_file_tables:
        csv_reader = csv.DictReader(csv_file_tables, delimiter=';')
        line_modified = 0
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                logger.info(f'Column names are {", ".join(row)}')
                line_count += 1
            else:
                table_name = row["TABLE_NAME"]
                sql_statement = sql_statement_count_template.substitute(table=table_name.strip())
                sql_script.append(sql_statement)
    return sql_script


if __name__ == '__main__':
    print('Selezione e creazione file di conteggio righe per differenza ')
    parser = argparse.ArgumentParser(description=help_msg())
    parser.add_argument('-d', '--directory_working',
                        default='C:\\Users\\u958garofalo\\Working\\Test_DBTOOL\\',
                        help='Directory dove risiedono SVN per il DB  ', required=False)
    parser.add_argument('-cd', '--directory_conf',
                        default='C:\\Users\\u958garofalo\\Working\\Test_DBTOOL\\file_csv\\',
                        help='Directory dove risiedono i file di configurazione  ', required=False)
    parser.add_argument('-Sa', '--schema_A',
                        default='',
                        help='schema di partenza',
                        required=True)
    parser.add_argument('-Sb', '--schema_B',
                        default='',
                        help='schema di arrivo',
                        required=True)
    parser.add_argument('-Sw', '--schema_working',
                        default='',
                        help='schema su cui lavorare',
                        required=True)
    parser.add_argument('-ro', '--repository_out_dir',
                        default='C:\\Users\\u958garofalo\\Working\\Test_DBTOOL\\output_script\\',
                        help='directory repository per l''output',
                        required=False)
    parser.add_argument('-rc', '--repository_dir',
                        default='C:\\Users\\u958garofalo\\Working',
                        help='directory repository',
                        required=False)
    # LOG features
    parser.add_argument('-l', '--log_dir',
                        default='C:\\Users\\u958garofalo\\Working\\Test_DBTOOL\\LOG_DBTOOL\\',
                        help='sub directory where store the log of patches',
                        required=False)

    args = parser.parse_args()
    repository_dir_conf = args.repository_dir
    directory_conf_csv = args.directory_conf
    repository_output_dir = args.repository_out_dir
    schema_working = args.schema_working
    version_db_A = args.schema_A
    version_db_B = args.schema_B
    # lista tabelle da verificare (estratte da TOAD)
    filename_conf_tabelle_csv = 'lista_tabelle_to_check_' + version_db_A + '_to_' + version_db_B + '.csv'
    lista_tabelle_file = directory_conf_csv + filename_conf_tabelle_csv
    log_file = 'log_file_DBdiff_' + version_db_A + '_to_' + version_db_B + '.log'
    log_file_complete = args.log_dir + log_file
    FORMAT = '%(asctime)s %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=FORMAT, encoding='utf-8',
                        handlers=[logging.FileHandler(log_file_complete), logging.StreamHandler()])
    # esempio dizionario con informazioni aggiuntive d = {'clientip': '192.168.0.1', 'user': 'fbloggs'}
    logger = logging.getLogger('exec_db_count_diff')
    sql_script_complete = lettura_file_configurazione_tabelle(lista_tabelle_file)
    sql_script_last_statement = sql_script_complete[-1]
    sql_script_last_statement = sql_script_last_statement.replace('union all', ';')
    print(sql_script_last_statement)
    sql_script_complete.pop()
    sql_script_complete.append(sql_script_last_statement)

    # creazione del file di script parametrico sullo schema
    sql_script_count_filename = 'select_count_tables_' + version_db_A + '_to_' + version_db_B + '.sql'
    sql_script_file = repository_output_dir + sql_script_count_filename
    with open(sql_script_file, mode='a') as mod_file_sql:
        mod_file_sql.writelines(sql_script_complete)
    # esecuzione sui due schemi log sul lo generale
    logger.info(f'\tESECUZIONE SCRIPT {sql_script_count_filename}')
    config_conn_db_file_name = 'connessioni_db_conf.csv'

    config_db = repository_dir_conf + '\\' + 'configurazione_tool_DB_patch\\' + config_conn_db_file_name
    # read the database connection configurazione files
    # the list of database where apply the patch

    # list_of_conf_db_A = load_db_conf(version_db_A)
    # list_of_conf_db_B = load_db_conf(version_db_B)

    version_list = [version_db_A, version_db_B]
    for version in version_list:
        list_of_conf_db = load_db_conf(version)
        conn_conf = [config for config in list_of_conf_db if (config['Type'] == schema_working)]
        schema_user_cnt = conn_conf[0]['User']
        for conn_conf_s in conn_conf:

            logger.info(" - Execute the script for " + schema_working + "  = " + sql_script_file +
                        " on schema = " + conn_conf_s['User'])
            status_ok = sql_execute_script(conn_conf_s, schema_user_cnt, sql_script_complete)

            if not status_ok:
                logger.warning(
                    "     Warning: error executing the script =" + sql_script_file + " on schema = " +
                    conn_conf_s['User'])
                break
            else:
                logger.info("     Script OK ")
