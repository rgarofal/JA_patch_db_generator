# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import argparse
import shutil
import os
from pathlib import Path
from datetime import date


def parse_file_patch(file_name):
    header_file = 'updateDbFrom'
    separatore_versioni = 'to'
    separatore_schema = '_'
    # test = updateDbFrom142551to142552_usr


# to manage the selection of specific patch files
def searchSVN2filepath_name(directory_patch, svn_last_id):
    paths = sorted(Path(directory_patch).iterdir(), key=os.path.getmtime, reverse=True)
    lista_patch_filenames = [file for file in paths if (os.path.basename(file).find(svn_last_id + '_') != -1) is True]
    return lista_patch_filenames


def last2filepath_name(directory_patch, from_version_id):
    # paths = sorted(Path(directory_patch).iterdir(), key=os.path.getmtime, reverse=True)
    # only2file = [os.path.basename(file) for file in paths[:3]]
    # return only2file
    if from_version_id != -1:
        paths = searchSVN2filepath_name(directory_patch, from_version_id)
    else:
        paths = sorted(Path(directory_patch).iterdir(), key=os.path.getmtime, reverse=True)

    exist_cntx = [True for file in paths[:3] if (os.path.basename(file).find('cntx') != -1) is True]
    len(exist_cntx)
    if exist_cntx:
        only2file = [os.path.basename(file) for file in paths[:3]]
    else:
        only2file = [os.path.basename(file) for file in paths[:2]]
    return only2file


def extract_last_num_patch(last_patch_filename):
    string_list = last_patch_filename.split('to')
    last_svn_i = string_list[1].split('_')
    return last_svn_i[0]


def change_version_init_tab(directory_base, schema_type, new_version, versione_soft):
    find_version = False
    print("Change internal version on init table for schema = " + schema_type)
    # cancello la v iniziale
    versione_soft = versione_soft[1:]

    template_search = 'T_FND_' + schema_type + '_VERSION (VERSION, RELEASE_DATE, DB_SCHEMA_VERSION) values (' + "'" + versione_soft + "'"
    schema_type_lowercase = schema_type.lower()
    init_name_file = directory_base + 'initdb1_' + schema_type_lowercase + '.sql';
    a_file = open(init_name_file, "r", encoding="utf8")
    list_of_lines = a_file.readlines()
    a_file.close()
    list_of_lines_mod = []
    count = 0;
    for line in list_of_lines:
        count = count + 1
        if line.find(template_search) != -1:
            find_version = True
            items = line.split(',')
            version_str = items[5]
            version_str = version_str.split(')')
            version_str[0] = ' \'' + new_version + '\'' + ')'
            new_vers_str = ""
            new_vers_str = new_vers_str.join(version_str)
            items[5] = new_vers_str
            line = ','.join(items)
        list_of_lines_mod.append(line)
    if find_version:
        print("Found the version to change for the schema = " + schema_type)
        a_file = open(init_name_file, "w", encoding="utf8")
        a_file.writelines(list_of_lines_mod)
        a_file.close()
    else:
        print("File init for the schema = " + schema_type + " UNCHANGED")


def change_version_internal(script_file, schema_type, version, riga_descr_baco):
    print("Change internal version for schema = " + schema_type)

    template_search = 'T_FND_' + schema_type + '_VERSION'

    a_file = open(script_file, "r")
    list_of_lines = a_file.readlines()
    list_of_lines_mod = []
    for line in list_of_lines:
        # aggiorna con la nuova versione
        if line.find(template_search) != -1:
            items = line.split('=')
            version_str = items[1].split()
            version_str[0] = '\'' + version + '\' '

            new_vers_str = " "
            new_vers_str = new_vers_str.join(version_str)
            items[1] = new_vers_str
            new_line = " = "
            line = new_line.join(items)
            # aggiungo la linea di descrizione patch se esiste
            if len(riga_descr_baco) != 0:
                list_of_lines_mod.append(line)
                line = riga_descr_baco
        list_of_lines_mod.append(line)
    a_file = open(script_file, "w")
    a_file.writelines(list_of_lines_mod)
    a_file.close()


def message_to_mail_and_patch(directory_working, version_release, commit_last_SVN, subject_bug):
    report_file = 'MESSAGE_PATCH_DB_' + version_release + '_' + commit_last_SVN + str(date.today()) + '.log'
    report_file = directory_working + '\\' + report_file
    print("Created the template file to write MAIL anc ClosureComment on file = " + report_file)
    report_string1 = 'MAIL to send\n'
    subject_mail = 'New patch for ' + version_release[
                                      1:] + ' Version ' + commit_last_SVN + ' - commit <TODO> ' + subject_bug + '\n'
    report_string2 = '\n\nREPORT su patch\n'
    template_script_mail = 'Hi, a new sql patch is available for db ' + version_release + '\n - svn database branch ' + version_release + '\n - version (' + commit_last_SVN + ') (2 files)\n' + 'I already ran it against wedge and jango ba' + version_release[
                                                                                                                                                                                                                                                 1:-2].replace(
        '.', '') + '* schemas. \n Thanks'
    template_script_rep = 'Patch ' + version_release + ' Version ' + commit_last_SVN + '\n Commit <TODO>'
    a_file = open(report_file, "w")
    a_file.writelines(report_string1)
    a_file.writelines(subject_mail)
    a_file.writelines(template_script_mail)
    a_file.writelines(report_string2)
    a_file.writelines(template_script_rep)
    a_file.close()


def help_msg():
    """ help to describe the script"""
    help_str = """ Script per generare i file di update di patch per il DB 
                Crea i due update files per CNT e USR andando a vedere l'ultimo progressimo e creando con il nuovo SVN number i due file.
                Inserisce l'update corretto alla nuova versione.
                Aggiunge l'ID del baco o attività a cui si riferisce
                   """
    return help_str


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print('Selezione e creazione file di patch')
    parser = argparse.ArgumentParser(description=help_msg())
    parser.add_argument('-d', '--directory_svn',
                        default='C:\\Users\\u958garofalo\\SVN',
                        help='Directory dove risiedono SVN per il DB  ', required=False)
    parser.add_argument('-v', '--version',
                        default='v21.1.0',
                        help='version branch',
                        required=False)
    parser.add_argument('-c', '--commit_id',
                        default='',
                        help='SVN number of commit',
                        required=True)
    parser.add_argument('-ic', '--from_commit_id',
                        default='',
                        help='From a specific SVN number of commit',
                        required=False)
    parser.add_argument('-i', '--identificativo_bug',
                        default='',
                        help='identifier and description bugs',
                        required=False)
    parser.add_argument('-r', '--repository_dir',
                        default='C:\\Users\\u958garofalo\\Working',
                        help='directory repository',
                        required=False)
    args = parser.parse_args()
    version_branch = args.version
    commit_last_SVN = args.commit_id
    base_dir = args.directory_svn
    id_baco = args.identificativo_bug  # JASS or altro
    riga_aggiuntiva = ''
    if len(id_baco) != 0:
        riga_aggiuntiva = "-- " + id_baco + " -- (TODO) -- "
    base_name_file = 'updateDbFrom'
    update_dir = '\\database\\branches\\' + version_branch + '\\sql\\upgrade\\esop\\update\\'
    model_dir = '\\database\\trunk\\sql\\master\\esop\\'
    repository_work_dir = args.repository_dir
    init_conf_dir = '03-InitConf'
    dir_model_complete = base_dir + '\\' + model_dir + init_conf_dir + '\\'
    dir_branch_complete = base_dir + '\\' + update_dir + version_branch
    print("FROM a specific SVN commit")
    from_version_patch = args.from_commit_id
    if not from_version_patch:
        print("No from a specific SVN commit")
        from_version_patch = -1
    else:
        print("Specified from SVN commit id = " + from_version_patch)
    print("Directory SVN model init  DB = ", dir_model_complete)
    change_version_init_tab(dir_model_complete, 'CNT', commit_last_SVN, version_branch)
    change_version_init_tab(dir_model_complete, 'USR', commit_last_SVN, version_branch)
    print("Directory SVN patch DB = ", dir_branch_complete)
    # extract the last two name of patch files
    last_2_patch_filenames = last2filepath_name(dir_branch_complete, from_version_patch)
    last_usr_filename = [f for f in last_2_patch_filenames if f.find("usr") != -1]
    last_cnt_filename = [f for f in last_2_patch_filenames if f.find("cnt") != -1 if f.find("cntx") == -1]
    # extract the to version from filename
    last_svn = extract_last_num_patch(last_2_patch_filenames[1])
    cnt_new_patch_file = base_name_file + last_svn + 'to' + commit_last_SVN + '_cnt.sql'
    usr_new_patch_file = base_name_file + last_svn + 'to' + commit_last_SVN + '_usr.sql'
    # copy last file with new file_name
    original = dir_branch_complete + '\\' + last_usr_filename[0]
    target = dir_branch_complete + '\\' + usr_new_patch_file
    shutil.copy(original, target)
    print("Created the template file for USR " + target)
    # change the version in the update VERSION TABLE
    change_version_internal(target, 'USR', commit_last_SVN, riga_aggiuntiva)
    original = dir_branch_complete + '\\' + last_cnt_filename[0]
    target = dir_branch_complete + '\\' + cnt_new_patch_file
    shutil.copy(original, target)
    print("Created the template file for CNT " + target)
    # change the version in the update VERSION TABLE
    change_version_internal(target, 'CNT', commit_last_SVN, riga_aggiuntiva)
    print('Created the template file to write MAIL for Version ' + version_branch + 'and Commit = ' + commit_last_SVN)
    message_to_mail_and_patch(repository_work_dir, version_branch, commit_last_SVN, id_baco)
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
