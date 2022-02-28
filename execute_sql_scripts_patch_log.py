import subprocess
import shlex
import argparse
import shutil
import os
from pathlib import Path
from datetime import date
import csv
import logging

# import logging.config

"""
Example of running a sqlplus script from Python.
Works with Python 3 and 2.
"""


def last2filepath_name(directory_patch):
    paths = sorted(Path(directory_patch).iterdir(), key=os.path.getmtime, reverse=True)
    exist_cntx = [True for file in paths[:3] if (os.path.basename(file).find('cntx') != -1) is True]
    if exist_cntx:
        print("EXIST the cntx file patch!")
        only2file = [os.path.basename(file) for file in paths[:3]]
    else:
        only2file = [os.path.basename(file) for file in paths[:2]]
    return only2file


# check to perform on CNT schema
# create the sql statement for each USR schemas to check
def sql_check_svn_version_script(version_release, svn_to_check, schema_cnt, other_schema):
    # sql_script = """
    #
    # select case when count(*) = 0 then 'OK' else 'KO' end check_version from (
    # select DB_SCHEMA_VERSION  from &&__USRUSER..T_FND_USR_VERSION where VERSION = &&VERS_REL and DB_SCHEMA_VERSION = &&SVN_CHECK
    # minus
    # select DB_SCHEMA_VERSION  from &&__CNTUSER..T_FND_CNT_VERSION where VERSION = &&VERS_REL and DB_SCHEMA_VERSION = &&SVN_CHECK ) T;
    #
    # """

    sql_script = """

                select case when F = 1 then 'OK' else 'KO' end check_version from (
                select count(*) as F from (select USR_V.DB_SCHEMA_VERSION from &&__USRUSER..T_FND_USR_VERSION USR_V join &&__CNTUSER..T_FND_CNT_VERSION CNT_V  on USR_V.DB_SCHEMA_VERSION = CNT_V.DB_SCHEMA_VERSION and USR_V.VERSION = CNT_V.VERSION
                where USR_V.VERSION = &&VERS_REL and CNT_V.DB_SCHEMA_VERSION = &&SVN_CHECK));

    """
    if test_configuration:
        # sql_script = """
        #
        #     select case when count(*) = 0 then 'OK' else 'KO' end check_version from (
        #     select DB_SCHEMA_VERSION  from &&__USRUSER..T_FND_USR_VERSION_RG where VERSION = &&VERS_REL and DB_SCHEMA_VERSION = &&SVN_CHECK
        #     minus
        #     select DB_SCHEMA_VERSION  from &&__CNTUSER..T_FND_CNT_VERSION_RG where VERSION = &&VERS_REL and DB_SCHEMA_VERSION = &&SVN_CHECK ) T;
        #
        #     """
        sql_script = """
        
            select case when F = 1 then 'OK' else 'KO' end check_version from (
            select count(*) as F from (select USR_V.DB_SCHEMA_VERSION from &&__USRUSER..T_FND_USR_VERSION_RG USR_V join &&__CNTUSER..T_FND_CNT_VERSION_RG CNT_V  on USR_V.DB_SCHEMA_VERSION = CNT_V.DB_SCHEMA_VERSION and USR_V.VERSION = CNT_V.VERSION
            where USR_V.VERSION = &&VERS_REL and CNT_V.DB_SCHEMA_VERSION = &&SVN_CHECK));
            
            """
    sql_script_check = sql_script.replace('&&__USRUSER.', other_schema).replace('&&__CNTUSER.', schema_cnt).replace(
        '&&VERS_REL', '\'' + version_release + '\'').replace('&&SVN_CHECK', '\'' + svn_to_check + '\'')
    return sql_script_check


def sql_string_connection(conf_db):
    # command_line = 'sql ba212cnt/ba212cnt@//midevdb03:1521/wedge'
    sql_conn = 'sql ' + conf_db['User'] + '/' + conf_db['Passwd'] + '@//' + conf_db['hostname'] + ':' + conf_db[
        'port'] + '/' + conf_db['service']
    return sql_conn


def is_ok_check_version(output_sql):
    status = False
    if output_sql.find('OK') == -1:
        status = False
        # print("NO")
    else:
        status = True
        # print("YES")
    return status


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


def sql_check_actual_svn_version(version_rel, version_to_check_int, svn_to_check_before, schema_cnt, conf_db):
    # PROVA cnt_conf = [config for config in conf_db if (config['Type'] == schema_cnt and config['Version'] == version_rel)]
    cnt_conf = [config for config in conf_db if (config['Type'] == schema_cnt)]
    # LOG DA CANCELLARE print(cnt_conf)
    status_ok = False
    logger.info(cnt_conf)
    # PROVA oth_schemas = [config for config in conf_db if (config['Type'] != schema_cnt and config['Version'] == version_rel)]
    oth_schemas = [config for config in conf_db if (config['Type'] != schema_cnt)]
    # version_soft = version_rel[1:]
    version_soft = version_to_check_int[1:]
    for oth_sch in oth_schemas:
        sql_statement = sql_check_svn_version_script(version_soft, svn_to_check_before, cnt_conf[0]['User'],
                                                     oth_sch['User'])
        sql_connection = sql_string_connection(cnt_conf[0])
        # print(sql_statement)
        output = run_sqlplus(sql_statement, sql_connection)
        # print(output)
        status_ok = check_status_sql_execution_sql(output, is_ok_check_version)
        # ATTENZIONE: se la versione NON ESISTE in entrambi da ok lo stesso
        if not status_ok:
            # LOG DA CANCELLARE print("WARNING the actual svn version on the DB for the Schema = " + oth_sch['User'] + " is not equal to CNT ")
            logger.warning("WARNING the actual svn version on the DB for the Schema = " + oth_sch[
                'User'] + " is not equal to CNT ")
            break
    return status_ok


def sql_execute_script(conf_db, user_cnt, sql_to_execute):
    sql_script_final = []
    statement_sql = ""
    status_ok = True
    label_to_ignore = "ONLINESKIP"
    label_environment = 'DEV'
    final_commit = "\n\ncommit;\n"
    if conf_db['Type'] == 'CNT':
        for line in sql_to_execute:
            line = line.replace('&&__CNTUSER.', conf_db['User']).replace(
                '&&__CNTTSIDX.', conf_db['Tablesp_index']).replace('&&__ENV.', label_environment)
            line = line.replace('\\\__CNTUSER.', conf_db['User']).replace(
                '\\\__CNTTSIDX.', conf_db['Tablesp_index']).replace('\\\__ENV.', label_environment)
            line = line.replace('&&__CNTUSER', conf_db['User'])
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
            line = line.replace('&&__USRUSER.', conf_db['User']).replace(
                '&&__USRTSIDX.', conf_db['Tablesp_index']).replace('&&__CNTUSER.', user_cnt).replace('&&__ENV.',
                                                                                                     label_environment)
            line = line.replace('\\\__USRUSER.', conf_db['User']).replace(
                '\\\__USRTSIDX.', conf_db['Tablesp_index']).replace('\\\__CNTUSER.', user_cnt).replace('\\\__ENV.',
                                                                                                       label_environment)
            line = line.replace('&&__USRUSER', conf_db['User'])
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


def define_algoritm(conf_db, version_release, version_to_check, svn_to_check_before, svn_to_check_after, schema_cnt,
                    dir_update,
                    script_executing
                    ):
    # 1 phase check the actual version
    # delete the initial v
    count_step = 1
    # LOG DA CANCELLARE print(str(count_step) + " - State of check Actual SVN = " + svn_to_check_before)
    logger.info(str(count_step) + " - State of check Actual SVN = " + svn_to_check_before)
    status_ok = sql_check_actual_svn_version(version_release, version_to_check, svn_to_check_before, schema_cnt,
                                             conf_db)
    # LOG DA CANCELLARE print("     State of check Actual SVN = " + str(status_ok))
    logger.info("     State of check Actual SVN = " + str(status_ok))
    count_step += 1
    if status_ok:
        schema_user_cnt = ''
        for script_to_execute in script_executing:
            sql_to_execute = read_script_sql(dir_update, script_to_execute)
            if script_to_execute.find('cnt') != -1:
                # script on CNT
                # PROVA conn_conf = [config for config in conf_db if
                #             (config['Type'] == 'CNT' and config['Version'] == version_release)]
                conn_conf = [config for config in conf_db if(config['Type'] == 'CNT')]
                schema_user_cnt = conn_conf[0]['User']
                # LOG DA CANCELLARE
                # print(str(
                #     count_step) + " - Execute the script for CNT  = " + script_to_execute + " on schema = " + schema_user_cnt)
                logger.info(str(
                    count_step) + " - Execute the script for CNT  = " + script_to_execute + " on schema = " + schema_user_cnt)
                status_ok = sql_execute_script(conn_conf[0], schema_user_cnt, sql_to_execute)
                count_step += 1
                if not status_ok:
                    # LOG DA CANCELLARE
                    # print("     Warning: error executing the script = " + script_to_execute + " on schema = " +
                    #       schema_user_cnt)
                    logger.warning("     Warning: error executing the script = " + script_to_execute + " on schema = " +
                                   schema_user_cnt)
                else:
                    # LOG DA CANCELLARE print("     Script OK ")
                    logger.info("     Script OK ")
            else:
                # execution of the CNTX scripts
                #
                if script_to_execute.find('cntx') != -1:
                    # script CNTX
                    # PROVA
                    # conn_conf_usr = [config for config in conf_db if
                    #                  (config['Type'] == 'USR' and config['Version'] == version_release)]
                    # conn_conf_head = [config for config in conf_db if
                    #                   (config['Type'] == 'CNT' and config['Version'] == version_release)]

                    conn_conf_usr = [config for config in conf_db if (config['Type'] == 'USR')]
                    conn_conf_head = [config for config in conf_db if (config['Type'] == 'CNT')]
                else:
                    # script USRs
                    # PROVA
                    # conn_conf = [config for config in conf_db if
                    #              (config['Type'] == 'USR' and config['Version'] == version_release)]

                    conn_conf = [config for config in conf_db if (config['Type'] == 'USR')]
                    for conn_conf_s in conn_conf:
                        # LOG DA CANCELLARE
                        # print(str(count_step) + " - Execute the script for USR  = " + script_to_execute +
                        #       " on schema = " + conn_conf_s['User'])
                        logger.info(str(count_step) + " - Execute the script for USR  = " + script_to_execute +
                                    " on schema = " + conn_conf_s['User'])
                        status_ok = sql_execute_script(conn_conf_s, schema_user_cnt, sql_to_execute)
                        count_step += 1
                        if not status_ok:
                            # LOG DA CANCELLARE
                            # print("     Warning: error executing the script =" + script_to_execute + " on schema = " +
                            #     conn_conf_s['User'])
                            logger.warning(
                                "     Warning: error executing the script =" + script_to_execute + " on schema = " +
                                conn_conf_s['User'])
                            break
                        else:
                            # LOG DA CANCELLARE print("     Script OK ")
                            logger.info("     Script OK ")
            # check if all is ok
            # LOG DA CANCELLARE print(str(count_step) + " - State of check Last SVN = " + svn_to_check_after)
            logger.info(str(count_step) + " - State of check Last SVN = " + svn_to_check_after)
            # status_ok = sql_check_actual_svn_version(version_release, svn_to_check_after, schema_cnt, conf_db)
            status_ok = sql_check_actual_svn_version(version_release, version_to_check, svn_to_check_after, schema_cnt,
                                                     conf_db)
            # LOG DA CANCELLARE print("     State of check Last SVN = " + str(status_ok))
            logger.info("     State of check Last SVN = " + str(status_ok))
    else:
        # LOG DA CANCELLARE print("WARNING the start SVN is not correct with the last in update patch")
        logger.warning("WARNING the start SVN is not correct with the last in update patch")
    return status_ok


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


def extract_prev_num_patch(last_patch_filename):
    string_list = last_patch_filename.split('to')
    pre_svn_i = string_list[0].split('updateDbFrom')
    return pre_svn_i[1]


def extract_last_num_patch(last_patch_filename):
    string_list = last_patch_filename.split('to')
    last_svn_i = string_list[1].split('_')
    return last_svn_i[0]


# to manage the selection of specific patch files
def searchSVN2filepath_name(directory_patch, svn_last_id):
    paths = sorted(Path(directory_patch).iterdir(), key=os.path.getmtime, reverse=True)
    lista_patch_filenames = [file for file in paths if (os.path.basename(file).find(svn_last_id + '_') != -1) is True]
    return lista_patch_filenames


# old code
# def get_pre_last_svn_script_file(dir_branch):
#     last_2_patch_filenames = last2filepath_name(dir_branch)
#     last_usr_filename = [f for f in last_2_patch_filenames if f.find("usr") != -1]
#     last_cnt_filename = [f for f in last_2_patch_filenames if f.find("cnt") != -1 if f.find("cntx") == -1]
#     # extract the to version from filename
#     last_svn = extract_last_num_patch(last_usr_filename[0])
#     pre_svn = extract_prev_num_patch(last_usr_filename[0])
#     return pre_svn, last_svn, last_2_patch_filenames

def get_pre_last_svn_script_file(dir_branch, svn_last_id):
    if svn_last_id != -1:
        # last_2_patch_filenames = searchSVN2filepath_name(dir_branch, svn_last_id)
        last_2_patch_filenames = [os.path.basename(f) for f in searchSVN2filepath_name(dir_branch, svn_last_id)]
        last_usr_filename = last_2_patch_filenames
    else:
        # probabilmente è da eliminare
        last_2_patch_filenames = last2filepath_name(dir_branch)
        last_usr_filename = [f for f in last_2_patch_filenames if f.find("usr") != -1]

    # (TODO) da eliminare last_cnt_filename = [f for f in last_2_patch_filenames if f.find("cnt") != -1 if f.find("cntx") == -1]
    # extract the to version from filename
    last_svn = extract_last_num_patch(last_usr_filename[0])
    pre_svn = extract_prev_num_patch(last_usr_filename[0])
    return pre_svn, last_svn, last_2_patch_filenames


def help_msg():
    """ help to describe the script"""
    help_str = """ TODO
                   """
    return help_str


def get_list_ora_error_to_exclude():
    list_ora_notincluded = ['ORA-00955', 'ORA-00001', 'ORA-01430', 'ORA-01442', 'ORA-02275', 'ORA-02260', 'ORA-02261',
                            'ORA-02264', 'ORA-02443']
    return list_ora_notincluded


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


def reorder_scripts_execution(list_script):
    new_list_ordered = []
    exist_cntx = False
    for script_name in list_script:
        if script_name.find('cnt') != -1:
            new_list_ordered.insert(0, script_name)
    for script_name in list_script:
        if script_name.find('cntx') != -1:
            new_list_ordered.insert(1, script_name)
            exist_cntx = True
    for script_name in list_script:
        if script_name.find('cnt') == -1:
            if script_name.find('cntx') == -1:
                if exist_cntx:
                    new_list_ordered.insert(2, script_name)
                else:
                    new_list_ordered.insert(1, script_name)

    return new_list_ordered


if __name__ == '__main__':
    print('Selezione e creazione file di patch')
    parser = argparse.ArgumentParser(description=help_msg())
    parser.add_argument('-d', '--directory_svn',
                        default='C:\\Users\\u958garofalo\\SVN',
                        help='Directory dove risiedono SVN per il DB  ', required=False)
    parser.add_argument('-v', '--version',
                        default='',
                        help='version branch',
                        required=True)
    parser.add_argument('-vu', '--version_to_update',
                        default='',
                        help='version to update - in general is the same as version branch',
                        required=False)
    parser.add_argument('-c', '--commit_id',
                        default='',
                        help='SVN number of commit',
                        required=True)
    parser.add_argument('-i', '--identificativo_bug',
                        default='',
                        help='identifier and description bugs',
                        required=False)
    parser.add_argument('-r', '--repository_dir',
                        default='C:\\Users\\u958garofalo\\Working',
                        help='directory repository',
                        required=False)
    parser.add_argument('-g', '--group_id',
                        default='1',
                        help='group id database to check',
                        required=False)
    # LOG features
    parser.add_argument('-l', '--log_dir',
                        default='C:\\Users\\u958garofalo\\Working\\LOG_DBTOOL',
                        help='sub directory where store the log of patches',
                        required=False)
    parser.add_argument('-t', '--test',
                        default=False,
                        help='sub directory where store the log of patches',
                        required=False)
    args = parser.parse_args()
    version_branch = args.version
    # version to which the patch is releated (it is possible apply on a specific version_branch
    # a patch on a previous version . e.g On 21.3 can apply all the missing patch of 12.2 or earlier version
    version_to_check = args.version_to_update
    if not version_to_check:
        version_to_check = version_branch
    commit_last_SVN = args.commit_id
    base_dir = args.directory_svn
    id_baco = args.identificativo_bug  # JASS or altro
    base_name_file = 'updateDbFrom'
    update_dir = '\\database\\branches\\' + version_branch + '\\sql\\upgrade\\esop\\update\\'
    config_conn_db_file_name = 'connessioni_db_conf.csv'
    repository_work_dir = args.repository_dir
    dir_branch_complete = base_dir + update_dir + version_branch
    config_db = repository_work_dir + '\\' + 'configurazione_tool_DB_patch\\' + config_conn_db_file_name
    group_selected = args.group_id

    # LOG da CANCELLARE print("Group selected = " + group_selected)

    # read script file cnt and usr
    pre_svn, last_svn, script_filenames = get_pre_last_svn_script_file(dir_branch_complete, commit_last_SVN)
    # LOG features
    # log directory + log file created using the version and Pre svn and Last svn
    log_file = 'log_file_for_v' + version_to_check + '_from_' + pre_svn + '_to_' + last_svn + '.log'
    log_file_complete = args.log_dir + '\\' + log_file
    FORMAT = '%(asctime)s %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=FORMAT, encoding='utf-8',
                        handlers=[logging.FileHandler(log_file_complete), logging.StreamHandler()])
    # esempio dizionario con informazioni aggiuntive d = {'clientip': '192.168.0.1', 'user': 'fbloggs'}
    logger = logging.getLogger('exec_db_patch')
    # print("Pre svn = " + pre_svn + " Last SVN = " + last_svn)
    test_configuration = args.test
    if test_configuration:
        logger.info("######################  START EXECUTION TEST #################################")
    else:
        logger.info("######################  START EXECUTION #################################")
    logger.info("Group selected = " + group_selected)
    # read the database connection configurazione files
    # the list of database where apply the patch
    list_of_conf_db = load_db_conf(group_selected)
    # read the database configuration files
    [logger.info(file) for file in script_filenames]
    script_filenames = reorder_scripts_execution(script_filenames)

    # LOG features
    define_algoritm(list_of_conf_db, version_branch, version_to_check, pre_svn, last_svn, 'CNT', dir_branch_complete,
                    script_filenames)
