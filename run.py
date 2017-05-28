#! /usr/bin/env python3

import sqlite3
import sys
import argparse
import os
import hashlib

class PyCataloguer:

    def __init__(self):

        db_path = os.path.join(os.path.dirname(__file__), 'pycataloguer.db')

        self.c = sqlite3.connect(db_path)
        self.c.row_factory = sqlite3.Row
        self.cu = self.c.cursor()

        self.cu.executescript('''
            CREATE TABLE IF NOT EXISTS `path` (
              `path_id` INTEGER PRIMARY KEY,
              `path` VARCHAR(255) NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS `file` (
              `file_id` INTEGER PRIMARY KEY,
              `file_name` VARCHAR(255) NOT NULL,
              `path_id` INTEGER NOT NULL,
              `path_to_file` VARCHAR(255) NOT NULL,
              `path_to_torrnet_file` VARCHAR(255),
              `url_to_file` VARCHAR(255),
              `md5` VARCHAR(40),
              FOREIGN KEY (path_id) REFERENCES path(path_id)
            );
            CREATE TABLE IF NOT EXISTS `category_name` (
              `category_name_id` INTEGER PRIMARY KEY,
              `category_name` VARCHAR(255)
            );
            CREATE TABLE IF NOT EXISTS `category` (
              `category_id` INTEGER PRIMARY KEY,
              `category_name_id` INTEGER,
              `category_parent` INTEGER,
              FOREIGN KEY (category_name_id) REFERENCES category_name(category_name_id)
            );
            CREATE TABLE IF NOT EXISTS `category_file` (
              `category_file_id` INTEGER PRIMARY KEY,
              `category_id` INTEGER,
              `file_id` INTEGER,
              FOREIGN KEY (category_id) REFERENCES category(category_id),
              FOREIGN KEY (file_id) REFERENCES file(file_id)
            );

        ''')
        self.c.commit()

    def __enter__(self):
        ''' for 'with' statement '''
        return self

    def __exit__(self, Type, Value, Trace):
        ''' for 'with' statement '''
        self.c.close()

        if Type is None:  # Если исключение не возникло
            pass
        else:             # Если возникло исключение
            return False  # False - исключение не обработано
                          # True  - исключение обработано

    def path_select(self):
        rows = self.cu.execute('SELECT * FROM `path`').fetchall()
        return True, rows

    def path_add(self, path):
        path = os.path.abspath(path)

        if not os.path.exists(path):
            return False, 'path doesn\'t find: {0}'.format(path)
        if not os.path.isdir(path):
            return False, 'it\'s not a directory: {0}'.format(path)

        row = self.cu.execute('SELECT `path_id` FROM `path` WHERE `path`=?', (path,)).fetchone()
        if row:
            return False, 'The path already exists! Its id is {0}'.format(row['path_id'])


        self.cu.execute('INSERT INTO `path` (`path`) VALUES (?)', (path,))
        self.c.commit()
        return True, self.cu.lastrowid

    def path_delete(self, path_id):
        self.cu.execute('DELETE FROM `path` WHERE `path_id`=?', (path_id,))
        return True, None

    def file_add(self, name, path_to_file):
        path_to_file = os.path.abspath(path_to_file)

        # определяем путь к директории поиска
        is_success, paths = self.path_select()
        if  not is_success: return paths
        path_id = None
        for path in paths:
            if path_to_file.startswith(path['path']):
                path_id = path['path_id']
                short_path_to_file = path_to_file[len(path['path']):]
                break
        if path_id is None: return False, 'File placed in not allowed place! Please, do "pycat pathselect" to view allowed place.'

        if not os.path.exists(path_to_file):
            return False, 'file doesn\'t find: {0}'.format(path_to_file)
        if not os.path.isfile(path_to_file):
            return False, 'it\'s not a file: {0}'.format(path_to_file)

        with open(path_to_file, 'rb') as f:
            md5 = hashlib.md5(f.read()).hexdigest()

        self.cu.execute('INSERT INTO `file` (`file_name`, `path_id`, `path_to_file`, `md5`) VALUES (?, ?, ?, ?)', (name, path_id, short_path_to_file, md5))
        self.c.commit()
        return True, self.cu.lastrowid

    def file_delete(self, file_id):
        self.cu.execute('DELETE FROM `category_file` WHERE `file_id`=?', (file_id,))
        self.cu.execute('DELETE FROM `file` WHERE `file_id`=?', (file_id,))
        self.c.commit()
        return True, None

    def file_update(self, file_id, fields):
        values = []
        keys = []
        for key, value in fields.items():
            keys.append('`'+key+'`')
            values.append(value)

        values.append(file_id)
        _values = ','.join(['?']*len(keys))
        self.cu.execute('UPDATE `file` ({keys}) VALUES ({_values}) WHERE `file_id`=?'.format(keys=','.join(keys), _values=_values), tuple(values))
        self.c.commit()
        return True, None

    def file_select(self, **fields):
        values = []
        keys = []
        for key, value in fields.items():
            operator, value = value
            operator = operator.lower()
            if operator in ['=', '>', '<', 'is', 'like']:
                _value = '?'
                values.append(value[0])
            elif operator == 'in':
                _value = '('+','.join(['?']*len(value))+')'
                values.extend(value)
            keys.append('`{key}` {operator} {_value}'.format(key=key, operator=operator, _value=_value))

        keys = ' AND '.join(keys)
        rows = self.cu.execute('SELECT * FROM `file`, `path` WHERE {keys} AND `file`.`path_id`=`path`.`path_id`'.format(keys=keys), tuple(values)).fetchall()
        return True, rows

    def query(self, sql):
        rows = self.cu.execute(sql).fetchall()
        return True, rows

if __name__ == "__main__":
    def proc_answer(is_success, arg1):
        if not is_success:
            print(arg1)
            exit(2)

    with PyCataloguer() as cat:

        parser = argparse.ArgumentParser(description='Cataloguer of everything')
        subparsers = parser.add_subparsers(dest='command')
        parser.add_argument('--version', action='version', version='%(prog)s 0.01')

        subparser1 = subparsers.add_parser('fileadd', help='adding your files to db')
        subparser1.add_argument('name')
        subparser1.add_argument('path')

        subparser2 = subparsers.add_parser('fileselect', help='view your files')
        subparser2.add_argument('--name', nargs='+')

        subparser3 = subparsers.add_parser('query', help='do sql-query')
        subparser3.add_argument('sql', help='any sql')

        subparser4 = subparsers.add_parser('fileprop', help='view data about file')
        subparser4.add_argument('file_id', help='id of file')
        subparser4.add_argument('--general', action='store_true')
        subparser4.add_argument('--categories', action='store_true')

        subparser5 = subparsers.add_parser('filerm', help='remove files from db')
        subparser5.add_argument('file_id', help='id of file', nargs='+')

        subparser6 = subparsers.add_parser('pathselect', help='show allowed directories')

        subparser7 = subparsers.add_parser('pathadd', help='add allowed directories')
        subparser7.add_argument('path', help='path', nargs='+')
 
        args = parser.parse_args()
        print(args)
        print()

        if args.command == 'fileadd':

            is_success, file_id = cat.file_add(args.name, args.path)
            proc_answer(is_success, file_id)
            print(file_id)
       
        elif args.command == 'fileselect':

            if len(args.name) == 1: file_name = ('LIKE', args.name)
            else: file_name = ('IN', args.name)

            is_success, files = cat.file_select(file_name=file_name)
            proc_answer(is_success, files)

            for file in files:
                print('---------------')
                print(file['file_id'], file['file_name'])

        elif args.command == 'query':

            is_success, rows = cat.query(args.sql)
            proc_answer(is_success, rows)
            for row in rows:
                print('---------------')
                for key in row.keys(): print(key, '=', row[key])

        elif args.command == 'fileprop':

            is_success, files = cat.file_select(file_id=('=', args.file_id))
            proc_answer(is_success, files)
            if len(files) == 1:
                for key in files[0].keys(): print(key, '=', files[0][key])

        elif args.command == 'filerm':

            for file_id in args.file_id:
                is_success, none = cat.file_delete(file_id)
                proc_answer(is_success, none)

        elif args.command == 'pathselect':

            is_success, paths = cat.path_select()
            proc_answer(is_success, paths)
            for path in paths: print(path['path_id'], path['path'])

        elif args.command == 'pathadd':

            for path in args.path:
                is_success, path_id = cat.path_add(path)
                proc_answer(is_success, path_id)
                print(path_id)