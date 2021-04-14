import shutil
import os
from pathlib import Path


def change_version_init_tab(directory_base, schema_type, new_version, versione_soft):
    find_version = False
    print("Change internal version on init table for schema = " + schema_type)

    template_search = '_VERSION(VERSION, RELEASE_DATE, DB_SCHEMA_VERSION)    values('

    template_search = 'T_FND_' + schema_type + '_VERSION (VERSION, RELEASE_DATE, DB_SCHEMA_VERSION) values (' + "'" + versione_soft + "'"
    schema_type_lowercase = schema_type.lower()
    init_name_file = directory_base + 'pippo_' + schema_type_lowercase + '.sql';
    a_file = open(init_name_file, "r", encoding="utf8")
    list_of_lines = a_file.readlines()
    a_file.close()
    list_of_lines_mod = []
    for line in list_of_lines:
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


if __name__ == '__main__':
    print('Selezione e creazione file di patch')
    change_version_init_tab('C:\\Users\\u958garofalo\\Working\\', 'CNT', '77777777', '21.1.0')
