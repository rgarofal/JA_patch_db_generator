import argparse
import csv
from string import Template

# Template common SQL statement
comment_table_template = Template('COMMENT ON TABLE "&&__BIZUSER."."$table" IS ''$comment_table'';\n')
comment_column_template = Template('COMMENT ON COLUMN "&&__BIZUSER."."$table"."$column_name" IS ''$comment_column'';\n')
create_psd_view_instance_template = Template('\tJOIN FX on ($source_col = FX_SOURCE_ID)\t')
# ATTENZIONE per l'ACCOUNT aggiungere la colonna alla BV =>  CTM_STRATEGY.ACCOUNT_ID                   AS BUYER_ORGANIZATION_ID
create_psd_view_account_template = Template(
    '\t(FX_ACCOUNT_ID IS NULL OR FX_ACCOUNT_ID = BUYER_ORGANIZATION_ID)\tAND (FX_ACCOUNT_ID_LIST IS NULL OR (INSTR(FX_ACCOUNT_ID_LIST,''|'' || BUYER_ORGANIZATION_ID || ''|'') > 0))''')
# ATTENZIONE per la DIVISION aggiungere la colonna DIVISION_OID
create_psd_view_division_template = Template(
    '\tAND (FX_DIVISION_OID IS NULL OR (INSTR(FX_DIVISION_OID,''|'' || PROJECT_DIVISION || ''|'') > 0))')

create_psd_view_template = Template(
    'CREATE OR REPLACE VIEW "&&__BIZUSER."."$psd_view_name" AS $select_body \t\nFROM\t $mv_view_name $cross_join $join_instance WHERE $cond_account $cond_division;\t')
create_mat_view_template = Template(
    'CREATE MATERIALIZED VIEW "&&__BIZUSER."."$mv_view_name" BUILD DEFERRED AS\tSELECT * FROM "&&__BIZUSER."."$bv_view_name"&&__OLTP_DBLINK.;\t')
alter_mat_view_template = Template('ALTER MATERIALIZED VIEW "&&__BIZUSER."."$mv_view_name" COMPILE;')
create_index_template = Template(
    'CREATE INDEX "&&__BIZUSER."."$name_of_index" ON "&&__BIZUSER."."$mv_view_name" ("$col_name");\t')
create_pk_psd_view_template = Template(
    'ALTER VIEW CAT_STRATEGY_ADDITIONAL_INFO ADD CONSTRAINT PK_$name_psd_view PRIMARY KEY ($list_column) RELY DISABLE NOVALIDATE;')

create_col_timestamp_tz = Template('FROM_TZ(CAST($col_name AS TIMESTAMP),(CF.SCHEMA_TZ)) AS $col_name')
create_cross_join_for_time_tz = Template('CROSS JOIN &&__BIZUSER..MV_CONFIG CF')


# IF columtype = TIMESTAMP WITH TIME ZONE THEN column -> FROM_TZ(<column_name> ,(CF.SCHEMA_TZ)) AS <columns_name> AND add CROSS JOIN &&__BIZUSER..MV_CONFIG CF
#
def print_working_env(line_col_view_working, working_listview_work, view_description, flag_instance, flag_account,
                      flag_division, file_bv, file_mv, file_psd, file_constraint):
    print('################### VIEW ##############################')
    print(
        f'\t\t{view_description} : {flag_instance} : {flag_account} : {flag_division} : {file_bv} : {file_mv} : {file_psd} : {file_constraint}')
    print(f'\t\t Linee estratte = {line_col_view_working} ')
    print(*working_listview_work, sep="\n")
    print('################### END VIEW ##############################')


def create_select_col(line_col_view_working, working_listview_work):
    select_body = ['\nSELECT /*### PSD ###*/ ']
    count = 1
    there_is_timestp_tz = False
    for row in working_listview_work:
        col_name_l = row["Column Name"]
        if row["Column Type"] == 'TIMESTAMP WITH TIME ZONE':
            col_name_l = create_col_timestamp_tz.substitute(col_name=col_name_l)
            there_is_timestp_tz = True
        if count == 1:
            select_body.append('\n' + col_name_l)
            count += 1
        else:
            select_body.append('\n,' + col_name_l)
    return select_body, there_is_timestp_tz


def create_sel_join_instance(flag_account, working_listview_work):
    select_join_instance = ''
    col_source_extracted = working_listview_work[0]
    if flag_account == 'X':
        join_cond = create_psd_view_instance_template.substitute(source_col=col_source_extracted['Column Name'])
        select_join_instance = '\n' + join_cond + '\n'
    return select_join_instance


def create_sel_account(flag_account):
    select_account = ''
    if flag_account == 'X':
        account_cond = create_psd_view_account_template.substitute()
        select_account = account_cond + '\n'
    return select_account


def create_sel_division(flag_division):
    select_division = ''
    if flag_division == 'X':
        division_cond = create_psd_view_division_template.substitute()
        select_division = division_cond + '\n'
    return select_division


def create_list_of_comment(view_on_working, view_description, working_listview_work):
    sql_list_comments = []

    view_description_l = '\'' + view_description + '\''
    table_comment = comment_table_template.substitute(table=view_on_working, comment_table=view_description_l)
    sql_list_comments.append(table_comment)

    for comments_col in working_listview_work:
        comments_col_l = '\'' + comments_col['Column Description'].replace("'", "''") + '\''
        sql_comment = comment_column_template.substitute(table=view_on_working,
                                                         column_name=comments_col['Column Name'],
                                                         comment_column=comments_col_l)
        sql_list_comments.append(sql_comment)
    return sql_list_comments


# with open('C:\\Users\\u958garofalo\\Working\\Test_DBTOOL\\file_csv\\prova_modifiche_risultato.sql',
#           mode='a') as mod_file:
def read_csv_file_catalog(file_name_complete_test_work, catalog_complete_file_name):
    w_file = open(file_name_complete_test_work, "w", encoding="utf-8")
    output_script = ["-- File autogenerated by script reading config file = " + catalog_complete_file_name + "\n\n"]

    with open(catalog_complete_file_name, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=';')
        line_modified = 0
        line_count = 0
        view_on_working = ''
        line_on_same_view = 1
        working_listview_work = []
        for row in csv_reader:

            if line_count == 0:
                # Print description of the structure of Catalog
                print(f'Column names are {", ".join(row)}')
                line_count += 1
            if line_count == 1:
                # The first line for each view of the catalog contains the description of the view and files, query BV, security filter
                line_col_view_working = 0
                view_on_working = row["View  Name"]
                print(f'\t WORKING ON VIEW = {row["View  Name"]}')
                view_description = row["View Description"]
                flag_instance = row["INSTANCE"]
                flag_account = row["ACCOUNT"]
                flag_division = row["DIVISION"]
                file_bv = row["FILE_BV"]
                file_mv = row["FILE_MV"]
                file_psd = row["FILE_PSD"]
                file_constraint = row["FILE_CONSTRAINT"]
                print(
                    f'\t{view_description} : {flag_instance} : {flag_account} : {flag_division} : {file_bv} : {file_mv} : {file_psd} : {file_constraint}')
                # working_listview_work.append(row)
                # elabora
            else:
                if view_on_working != row["View  Name"]:
                    # Here you should elaborate the view previous read
                    print(f'\tElabora la vista {view_on_working}')
                    print_working_env(line_col_view_working, working_listview_work, view_description, flag_instance,
                                      flag_account, flag_division, file_bv, file_mv, file_psd, file_constraint)
                    output_script.append(f'\t--Working on view {view_on_working}\n\n')
                    select_body_col, there_is_timestp_tz = create_select_col(line_col_view_working, working_listview_work)
                    if there_is_timestp_tz:
                        select_cross_join = create_cross_join_for_time_tz.substitute()
                        select_cross_join = '\n\t' + select_cross_join
                    else:
                        select_cross_join =''
                    select_body_col_str = ''.join([str(col) for col in select_body_col])
                    select_join_instance = create_sel_join_instance(flag_instance, working_listview_work)
                    select_cond_account = create_sel_account(flag_account)
                    select_cond_division = create_sel_division(flag_division)
                    create_sql_statement = create_psd_view_template.substitute(psd_view_name=view_on_working,
                                                                               select_body=select_body_col_str,
                                                                               mv_view_name='<TODO>',
                                                                               cross_join=select_cross_join,
                                                                               join_instance=select_join_instance,
                                                                               cond_account=select_cond_account,
                                                                               cond_division=select_cond_division)
                    # comments
                    sql_list_of_comment = create_list_of_comment(view_on_working, view_description,
                                                                 working_listview_work)

                    output_script.append(create_sql_statement)
                    output_script.append('\n\n')
                    for comment_str in sql_list_of_comment:
                        output_script.append(comment_str)
                    output_script.append('\n\n')
                    output_script.append(f'\t--End Working on view {view_on_working}\n\n')
                    # Next view
                    working_listview_work = []
                    line_col_view_working = 0
                    view_on_working = row["View  Name"]
                    print(f'\t WORKING ON VIEW = {row["View  Name"]}')
                    view_description = row["View Description"]
                    flag_instance = row["INSTANCE"]
                    flag_account = row["ACCOUNT"]
                    flag_division = row["DIVISION"]
                    file_bv = row["FILE_BV"]
                    file_mv = row["FILE_MV"]
                    file_psd = row["FILE_PSD"]
                    file_constraint = row["FILE_CONSTRAINT"]
                    print(
                        f'\t{view_description} : {flag_instance} : {flag_account} : {flag_division} : {file_bv} : {file_mv} : {file_psd} : {file_constraint}')
                    # elabora
                    # working_listview_work.append(row)

            if view_on_working == row["View  Name"]:
                # read the configuration files each columns to process
                line_col_view_working += 1
                working_listview_work.append(row)
            line_count += 1
        print(f'\tElabora la vista {view_on_working}')
        print_working_env(line_col_view_working, working_listview_work, view_description, flag_instance,
                          flag_account, flag_division, file_bv, file_mv, file_psd, file_constraint)
        # Here you should elaborate the view previous read
        print(f'\tElabora la vista {view_on_working}')
        print_working_env(line_col_view_working, working_listview_work, view_description, flag_instance,
                          flag_account, flag_division, file_bv, file_mv, file_psd, file_constraint)
        output_script.append(f'\t--Working on view {view_on_working}\n\n')
        select_body_col, there_is_timestp_tz = create_select_col(line_col_view_working, working_listview_work)
        if there_is_timestp_tz:
            select_cross_join = create_cross_join_for_time_tz.substitute()
            select_cross_join = '\n\t' + select_cross_join
        else:
            select_cross_join = ''
        select_body_col_str = ''.join([str(col) for col in select_body_col])
        select_join_instance = create_sel_join_instance(flag_instance, working_listview_work)
        select_cond_account = create_sel_account(flag_account)
        select_cond_division = create_sel_division(flag_division)
        create_sql_statement = create_psd_view_template.substitute(psd_view_name=view_on_working,
                                                                   select_body=select_body_col_str,
                                                                   mv_view_name='<TODO>',
                                                                   cross_join=select_cross_join,
                                                                   join_instance=select_join_instance,
                                                                   cond_account=select_cond_account,
                                                                   cond_division=select_cond_division)
        # comments
        sql_list_of_comment = create_list_of_comment(view_on_working, view_description,
                                                     working_listview_work)

        output_script.append(create_sql_statement)
        output_script.append('\n\n')
        for comment_str in sql_list_of_comment:
            output_script.append(comment_str)
        output_script.append('\n\n')
        output_script.append(f'\t--End Working on view {view_on_working}\n\n')
        w_file.writelines(output_script)
        w_file.close()


def help_msg():
    print()


if __name__ == '__main__':
    print('Partenza')

    parser = argparse.ArgumentParser(description=help_msg())
    parser.add_argument('-d', '--directory_svn',
                        default='C:\\Users\\u958garofalo\\SVN',
                        help='SVN Directory for DB  ',
                        required=False)
    parser.add_argument('-v', '--version',
                        default='v22.2.0',
                        help='version branch',
                        required=False)
    parser.add_argument('-r', '--repository_dir',
                        default='C:\\Users\\u958garofalo\\Working',
                        help='directory repository',
                        required=False)

    parser.add_argument('-dw', '--working_dir',
                        default='\\Test_DBTOOL\\output_script',
                        help='directory working repository',
                        required=False)

    parser.add_argument('-cp', '--catalog_path',
                        default='\\Test_DBTOOL\\file_csv',
                        help='Path where reside the catalog new PSD view',
                        required=False)
    parser.add_argument('-cg', '--catalog_file',
                        default='ProvaWorkingNewView.csv',
                        help='Excel file with catalog new PSD view',
                        required=False)
    # LOG features
    parser.add_argument('-l', '--log_dir',
                        default='C:\\Users\\u958garofalo\\Working\\LOG_PSD_DP',
                        help='sub directory where store the log of script',
                        required=False)
    args = parser.parse_args()

    version_branch = args.version
    base_dir = args.directory_svn
    working_dir = args.working_dir
    repository_dir = args.repository_dir

    log_file = 'log_file_for_PSD_GENERATION' + version_branch + '.log'
    log_file_complete = args.log_dir + '\\' + log_file

    update_dir = '\\database\\branches\\' + version_branch + '\\sql\\master\dmr\psd\\'
    repository_work_dir = args.repository_dir + working_dir
    # file name temporaneo di lavoro
    path_name_catalog = args.catalog_path
    file_name_catalog = args.catalog_file
    file_name_test_final = file_name_catalog[:-4] + '.sql'
    path_repository_csv = repository_dir + path_name_catalog
    file_name_complete_catalog = path_repository_csv + '\\' + file_name_catalog
    file_name_complete_test_work = repository_work_dir + '\\' + file_name_test_final

    dir_branch_complete = base_dir + '\\' + update_dir
    file_name_complete_target = dir_branch_complete + '\\'  # + file_name_target_final

    read_csv_file_catalog(file_name_complete_test_work, file_name_complete_catalog)
