# Script to generate the file with all PSD view for dataPipeline teams
# Author: Roberto Garofalo
#
import argparse
import re
import shutil
import os
from pathlib import Path
from datetime import date
import logging
import shutil
from string import Template

header_file = '06-psd_view'
file_name_target = '07-mv_view-rfx_biz_WORK.sql'
file_name_target_final = '07-mv_view-dpl_biz.sql'
file_name_test_final = '07-mv_view-dpl_biz_TEST.sql'

# check_count_psd_view_template = Template(
#     'SELECT count(*) FROM $dpl_view UNION ALL SELECT count(*) FROM $psd_view ;\n')
# SQL to perform the test file to check the counting of original PSD view and Data Pipeline view
check_count_psd_view_template = Template(
    'select CASE WHEN (N_T.NUM_N < O_T.NUM or (N_T.NUM_N = 0 and O_T.NUM <>0)) THEN \'KO\' ELSE \'OK\' END CHECK_STATUS, N_T.NAME_VIEW_N, O_T.NAME_VIEW from (SELECT count(*) AS NUM_N, \'$dpl_view\'  as NAME_VIEW_N  FROM $dpl_view N) N_T,(SELECT count(*) AS NUM,  \'$psd_view\' as NAME_VIEW  FROM $psd_view O) O_T;\n')


# to manage the list of the PSD view to scan
def listPSDViewfilepath_name(directory_PSD, header_file):
    # paths = sorted(Path(directory_PSD).iterdir(), key=os.path.getmtime, reverse=True)
    paths = sorted(Path(directory_PSD).iterdir())
    lista_patch_filenames = [file for file in paths if (os.path.basename(file).find(header_file) != -1) is True]
    return lista_patch_filenames


def create_new_PipelinePSD(list_of_lines, file_PSD_to_read):
    logger.info("Working on ... " + file_PSD_to_read)
    template_search_head_view = 'CREATE OR REPLACE VIEW'
    # substitute to SELECT
    template_psd_sign = '/*### PSD ###*/'
    template_psd_sign_s = '*### PSD ###*'
    template_code_to_exclude_start = 'JOIN FX '
    template_code_to_exclude_2_start = 'JOIN &&__BIZUSER..FX'
    # template_and_to_delete = "AND (FX_ACCOUNT_ID IS NULL OR FX_ACCOUNT_ID = SAV.BUYER_ORGANIZATION_ID) AND (FX_ACCOUNT_ID_LIST IS NULL OR (INSTR(FX_ACCOUNT_ID_LIST,' | ' || SAV.BUYER_ORGANIZATION_ID || ' | ') > 0))"
    # template_where_to_delete = "WHERE (FX_ACCOUNT_ID IS NULL OR FX_ACCOUNT_ID = BUYER_ORGANIZATION_ID)	AND (FX_ACCOUNT_ID_LIST IS NULL OR (INSTR(FX_ACCOUNT_ID_LIST,' | ' || BUYER_ORGANIZATION_ID || ' | ') > 0))"
    template_and_to_delete_simple = "AND (FX_ACCOUNT_ID IS NULL OR FX_ACCOUNT_ID "
    template_and_to_delete_test_simple = template_and_to_delete_simple.strip().replace("\t", "").replace(" ", "")
    template_where_to_delete_simple = "WHERE (FX_ACCOUNT_ID IS NULL OR FX_ACCOUNT_ID "
    template_where_to_delete_test_simple = template_where_to_delete_simple.strip().replace("\t", "").replace(" ", "")
    changed_list_of_lines = []
    dict_view = {}
    test_script_lines = ['\n' + '-- working on filename = ' + file_PSD_to_read + '\n']
    changed_list_of_lines.append('\n' + '-- working on filename = ' + file_PSD_to_read + '\n')
    new_view = ''
    old_view = ''
    old_view_column = ''
    old_view_column_comment = ''
    for line in list_of_lines:
        if line.find(template_search_head_view) != -1:
            # treat the new name of the view
            token = line.split('"')
            old_view = token[3]
            new_view = token[3] + '_'
            old_view_column = token[3] + ','
            # old_view_comment = "\"&&__BIZUSER.\".\""+"\""+old_view+"\""
            # new_view_comment =  "\"&&__BIZUSER.\".\""+"\""+new_view+"\""
            if len(new_view) > 30:
                logger.warning(
                    "WARNING NEW PSD VIEW DP " + new_view + ' from view = ' + old_view + ' EXCEED the 30 char len = ' + str(
                        len(new_view)) + ' will be shortened')
                old_view_ch = old_view[:29]
                new_view = old_view_ch + '_'

            new_view_create = token[0] + '"' + token[1] + '"' + token[2] + '"' + new_view + '"' + token[4]
            if len(new_view) > 30:
                logger.warning(
                    "CREATING NEW PSD VIEW DP " + new_view + ' from view = ' + old_view + ' EXCEED the 30 char len = ' + str(
                        len(new_view)))
            else:
                logger.info("CREATING NEW PSD VIEW DP " + new_view + ' from view = ' + old_view)
            # save the dictionary mapping old_view new_view
            dict_view[old_view] = new_view
            check_count_sql_string = check_count_psd_view_template.substitute(dpl_view=new_view, psd_view=old_view)
            test_script_lines.append(check_count_sql_string)
            changed_list_of_lines.append(new_view_create)
        else:
            # if (template_code_to_exclude_start not in line) and (template_and_to_delete not in line) and (
            #         template_where_to_delete not in line):
            line_test = line.strip().replace("\t", "").replace(" ", "")
            if (template_code_to_exclude_start not in line) and (template_code_to_exclude_2_start not in line) \
                    and (line_test.find(template_and_to_delete_test_simple) == -1) \
                    and (line_test.find(template_where_to_delete_test_simple) == -1):
                line = line.replace(template_psd_sign, '')
                line = line.replace(template_psd_sign_s, '')
                sub = r'\b%s\b' % re.escape(old_view)
                if line_test.find(old_view_column.strip().replace("\t", "").replace(" ", "")) == -1:
                    line = re.sub(sub, new_view, line, count=1)
                # line = line.replace(old_view, new_view)
                changed_list_of_lines.append(line)
    return changed_list_of_lines, test_script_lines, dict_view


# this function probably must be deleted
# def copy_history(file_name_complete_target):
#     # read the header historical
#     template_start = '-- START HISTORY'
#     template_end = '-- END HISTORY'
#     r_file = open(file_name_complete_target, "r", encoding="utf-8")
#     list_of_lines_header = r_file.readlines()
#     r_file.close()
#     list_lines_history = []
#     start = False
#     for line in list_of_lines_header:
#         if line.strip() == template_start:  # Or whatever test is needed
#             list_lines_history.append(line)
#             start = True
#         else:
#             if (line.strip() != template_end) and (start is True):
#                 list_lines_history.append(line)
#             else:
#                 list_lines_history.append(line)
#                 break
#     return list_lines_history

def compile_PSD_pipeline_working_last(file_name_complete_working, dict_view):
    logger.info(
        "Update all the new PSD file working  = " + file_name_complete_working)
    r_file = open(file_name_complete_working, "r", encoding="utf-8")
    logger.info("Processing file " + file_name_complete_working)
    list_of_lines = r_file.readlines()
    r_file.close()
    list_of_lines_filtered = []
    list_key = '|'.join(dict_view.keys())
    pattern = re.compile(r'\b(' + list_key + r')\b')
    for line in list_of_lines:
        if 'JOIN' in line:
            line = pattern.sub(lambda x: dict_view[x.group()], line)
        list_of_lines_filtered.append(line)
    return list_of_lines_filtered


def compile_PSD_pipeline_working(file_name_complete_working, file_name_complete_test_work, list_filenames_PSD_to_read,
                                 dir_branch_complete,
                                 file_name_complete_target):
    logger.info(
        "Create all the new PSD file working  = " + file_name_complete_working + " the target finale file is = "
        + file_name_complete_target)
    # open file target in write mode
    w_file = open(file_name_complete_working, "w", encoding="utf-8")
    w_file.write("-- File autogenerated by script \n\n")
    # open the script for test
    w_file_test = open(file_name_complete_test_work, "w", encoding="utf-8")
    w_file_test.write("-- File autogenerated by script \n\n")
    dic_view_all ={}
    for file_PSD_to_read in list_filenames_PSD_to_read:
        # the psd with the generic PSD view has to be excluded by analysis
        file_name = os.path.basename(file_PSD_to_read)
        if file_name.find('06-psd_view-0_biz.sql') == -1:
            a_file = open(file_PSD_to_read, "r", encoding="utf-8")
            logger.info("Processing file " + file_name)
            list_of_lines = a_file.readlines()
            a_file.close()
            # extract new version
            list_of_lines_mod, test_line_script, dict_view = create_new_PipelinePSD(list_of_lines, file_name)
            dic_view_all.update(dict_view)
            w_file.writelines(list_of_lines_mod)
            w_file_test.writelines(test_line_script)
    w_file.close()
    w_file_test.close()
    # last filter to substitute the psd view used internally
    test_line_script_last = compile_PSD_pipeline_working_last(file_name_complete_working, dic_view_all)
    w_file = open(file_name_complete_working, "w", encoding="utf-8")
    w_file.writelines(test_line_script_last)
    w_file.close()
    if not autogenerate_flag_only:
        # release the definitive script file
        newPath = shutil.copy(file_name_complete_working, file_name_complete_target)
        logger.info(
            "Released the final file = " + file_name_complete_target + "  from working copy = " + file_name_complete_working)


def help_msg():
    """ help to describe the script"""
    help_str = """ Script to generate shadow PSD views for Data Pipeline
                    It reads all 06_ files (which contain the final PSD views).
                    Rewrite all PSD views by changing their name (putting_ final) and removing all the environment filters all in the file 07-mv_view-dpl_biz.sql
                   """
    return help_str


if __name__ == '__main__':
    print('Script to generate all PSD view for DataPipeline')
    parser = argparse.ArgumentParser(description=help_msg())
    parser.add_argument('-d', '--directory_svn',
                        default='C:\\Users\\u958garofalo\\SVN',
                        help='SVN Directory for DB  ', required=False)
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
    parser.add_argument('-ao', '--autogenerate_only',
                        default=False,
                        help='if True the script generate only the script working target and the TEST script',
                        required=False)
    parser.add_argument('-psd', '--psd_directory',
                        default='\\sql\\master\dmr\psd\\',
                        help='directory where the psd view are located',
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
    autogenerate_flag_only = args.autogenerate_only
    log_file = 'log_file_for_PSD_PIPELINE' + version_branch + '.log'
    log_file_complete = args.log_dir + '\\' + log_file
    base_name_file_PSD = header_file
    dir_psd = args.psd_directory
    #update_dir = '\\database\\branches\\' + version_branch + '\\sql\\master\dmr\psd\\'
    update_dir = '\\database\\branches\\' + version_branch + dir_psd
    repository_work_dir = args.repository_dir + working_dir
    # file name temporary of work
    file_name_complete_working = repository_work_dir + '\\' + file_name_target
    file_name_complete_test_work = repository_work_dir + '\\' + file_name_test_final

    dir_branch_complete = base_dir + '\\' + update_dir
    file_name_complete_target = dir_branch_complete + '\\' + file_name_target_final

    FORMAT = '%(asctime)s %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=FORMAT, encoding='utf-8',
                        handlers=[logging.FileHandler(log_file_complete), logging.StreamHandler()])
    logger = logging.getLogger('exec_psd_dp_log')

    logger.info("Directory SVN PSD to read = " + dir_branch_complete)
    logger.info("Target File name PSD = " + file_name_complete_working)
    logger.info("File name script test final PSD = " + file_name_complete_test_work)
    if autogenerate_flag_only:
        logger.warning("WARNING SCRIPT in AUTOGENERATE ONLY MODE")
    # extract list file PSD to read
    list_filenames_PSD_to_read = listPSDViewfilepath_name(dir_branch_complete, base_name_file_PSD)
    logger.info(list_filenames_PSD_to_read)
    compile_PSD_pipeline_working(file_name_complete_working, file_name_complete_test_work, list_filenames_PSD_to_read,
                                 dir_branch_complete,
                                 file_name_complete_target)
