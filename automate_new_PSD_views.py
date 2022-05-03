import argparse
import csv
from string import Template

# Template common SQL statement
comment_table_template = Template('COMMENT ON TABLE "&&__BIZUSER."."$table" IS ''$comment_table'';')
comment_column_template = Template('COMMENT ON COLUMN "&&__BIZUSER."."$table"."$column_name" IS ''$comment_table'';')
create_psd_view_instance_template = Template('\tJOIN FX on ($source_col = FX_SOURCE_ID)\t')
# ATTENZIONE per l'ACCOUNT aggiungere la colonna alla BV =>  CTM_STRATEGY.ACCOUNT_ID                   AS BUYER_ORGANIZATION_ID
create_psd_view_account_template = Template(
    '\t(FX_ACCOUNT_ID IS NULL OR FX_ACCOUNT_ID = BUYER_ORGANIZATION_ID)\tAND (FX_ACCOUNT_ID_LIST IS NULL OR (INSTR(FX_ACCOUNT_ID_LIST,''|'' || BUYER_ORGANIZATION_ID || ''|'') > 0))''')
# ATTENZIONE per la DIVISION aggiungere la colonna DIVISION_OID
create_psd_view_division_template = Template(
    '\tAND (FX_DIVISION_OID IS NULL OR (INSTR(FX_DIVISION_OID,''|'' || PROJECT_DIVISION || ''|'') > 0))')

create_psd_view_template = Template(
    'CREATE OR REPLACE VIEW "&&__BIZUSER."."$psd_view_name" AS $select_body \tFROM\t $mv_view_name $join_instance \tWHERE $cond_where\t $cond_account\t;\t')
create_mat_view_template = Template(
    'CREATE MATERIALIZED VIEW "&&__BIZUSER."."$mv_view_name" BUILD DEFERRED AS\tSELECT * FROM "&&__BIZUSER."."$bv_view_name"&&__OLTP_DBLINK.\t;\t')
alter_mat_view_template = Template('ALTER MATERIALIZED VIEW "&&__BIZUSER."."$mv_view_name" COMPILE;')
create_index_template = Template(
    'CREATE INDEX "&&__BIZUSER."."$name_of_index" ON "&&__BIZUSER."."$mv_view_name" ("$col_name");\t')
create_pk_psd_view_template = Template(
    'ALTER VIEW CAT_STRATEGY_ADDITIONAL_INFO ADD CONSTRAINT PK_$name_psd_view PRIMARY KEY ($list_column) RELY DISABLE NOVALIDATE;')


#
def print_working_env(line_col_view_working, working_listview_work, view_description, flag_instance, flag_account,
                      flag_division, file_bv, file_mv, file_psd, file_constraint):
    print('################### VIEW ##############################')
    print(
        f'\t\t{view_description} : {flag_instance} : {flag_account} : {flag_division} : {file_bv} : {file_mv} : {file_psd} : {file_constraint}')
    print(f'\t\t Linee estratte = {line_col_view_working} ')
    print(*working_listview_work, sep="\n")
    print('################### END VIEW ##############################')


# def  compile_file_sql(view_on_working, path_result, line_col_view_working, working_listview_work, view_description, flag_instance,
#                               flag_account, flag_division, file_bv, file_mv, file_psd, file_constraint):
#     print('Compile file_materialized view')
#     with open(path_result + file_mv, mode='a') as mod_file:
#         mv_view_name = f'MV_{view_on_working}'
#         list_sql_statement=[]
#         sql_statement = create_mat_view_template.substitute(mv_view_name=view_on_working)
#         list_sql_statement.append(sql_statement)
#         sql_statement = alter_mat_view_template.substitute(mv_view_name=view_on_working)
#         list_sql_statement.append(sql_statement)
#         name_table_ind = view_on_working if len(view_on_working) < 14 else view_on_working[1:15]
#         for row in line_col_view_working:
#             # 30 -16 = 14 caratteri
#             if row[]
#             column_name_indx = row['Column Name'][1:4]
#             name_index = f'IDX_{name_table_ind}_{column_name_indx}'
#             create_index_template
#             column_named[1:4]
#         mod_file.writelines(list_sql_statement)


# with open('C:\\Users\\u958garofalo\\Working\\Test_DBTOOL\\file_csv\\prova_modifiche_risultato.sql',
#           mode='a') as mod_file:
def read_csv_file_catalog(path_result, catalog_complete_file_name):
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
                #The first line for each view of the catalog contains the description of the view and files, query BV, security filter
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
                    print(f'\tElabora la vista {view_on_working}')
                    print_working_env(line_col_view_working, working_listview_work, view_description, flag_instance,
                                      flag_account, flag_division, file_bv, file_mv, file_psd, file_constraint)
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
                    working_listview_work.append(row)

            if view_on_working == row["View  Name"]:
                line_col_view_working += 1
                working_listview_work.append(row)
            line_count += 1
        print(f'\tElabora la vista {view_on_working}')
        print_working_env(line_col_view_working, working_listview_work, view_description, flag_instance,
                          flag_account, flag_division, file_bv, file_mv, file_psd, file_constraint)

        # compile_file_sql(view_on_working, path_result, line_col_view_working, working_listview_work, view_description, flag_instance,
        #                       flag_account, flag_division, file_bv, file_mv, file_psd, file_constraint)

        # print(
        #     f'\t{row["View  Name"]} on the column name  {row["Column Name"]} column type  {row["Column Type"]} colum descriiption  {row["Column Description"]} indici {row["Indici"]}')

        #     if row["Notes"] == 'Change column description':
        #         if line_count == 1:
        #             header_modification = f'\n\n#################  VISTA MODIFICATA {row["View"]} ###############################'
        #             first_view = row["View"]
        #             mod_file.writelines(header_modification)
        #         else:
        #             if first_view != row["View"]:
        #                 header_modification = f'\n\n#################  VISTA MODIFICATA {row["View"]} ###############################'
        #                 first_view = row["View"]
        #                 mod_file.writelines(header_modification)
        #
        #         #comment_str = "'" if not row["Column description"].startswith("'") else row["Column description"] + "'" if not row["Column description"].endswith("'") else row["Column description"]
        #         change_statement = f'\nCOMMENT ON COLUMN "&&__BIZUSER."."{row["View"]}"."{row["Column"]}" IS "{row["Column description"]}";'
        #         #print(change_statement)
        #         mod_file.writelines(change_statement)
        #         line_modified += 1
        #     line_count += 1
        # print(f'Processed {line_count} lines.')
        # print(f'Modified  {line_modified} lines.')


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
    path_repository_csv = repository_dir + path_name_catalog
    file_name_complete_catalog = path_repository_csv + '\\' + file_name_catalog
    file_name_complete_test_work = repository_work_dir + '\\'  # + file_name_test_final

    dir_branch_complete = base_dir + '\\' + update_dir
    file_name_complete_target = dir_branch_complete + '\\'  # + file_name_target_final

    read_csv_file_catalog(working_dir, file_name_complete_catalog)
