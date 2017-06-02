#! /usr/bin/env python3

import sqlite3
import sys
import os
import hashlib

import sqlalchemy as alch
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

READ = '\033[0;31m'
NORM = '\033[0;0m'
GREEN= '\033[0;32m'
ORANGE = '\033[0;33m'

'''class CustomProxy(alch.engine.ResultProxy):
    def __getitem__(self, key):
        print('/////////', key)
        for table in self:
            if key in table: return table[key]
        #return getattr(self, key)

alch.engine.ResultProxy = CustomProxy'''

Base = declarative_base()

class CustomBase():
    def __getitem__(self, key): return getattr(self, key)
    def keys(self):
        return [column.name for column in  self.__table__.columns]
        #for key in dir(self):
        #    if not key.startswith('_'): yield key

    def row2dict(self):
        return {column.name: str(getattr(self, column.name)) for column in self.__table__.columns}

    def __contains__(self, key):
        for column in self.__table__.columns:
            if key == column.name: return True
        return False


def row2dict(row):
    if isinstance(row, Base): row = [row] # если извлекаются данные из нескольких таюлиц, то данные разделены по таблицам

    d = {}
    for table in row:
        d.update(table.row2dict())
        #for column in table.__table__.columns:
        #    d[column.name] = str(getattr(table, column.name))
    return d
    #return {column.name: str(getattr(row, column.name)) for column in row.__table__.columns}

class TablePath(CustomBase, Base):
    """
    CREATE TABLE IF NOT EXISTS `path` (
        `path_id` INTEGER PRIMARY KEY,
        `path` VARCHAR(255) NOT NULL UNIQUE
    );
    """

    __tablename__ = "path"

    path_id = alch.Column(alch.Integer(),   nullable=False, primary_key=True)
    path =    alch.Column(alch.String(255), nullable=False, unique=True)


class TableCategory(CustomBase, Base):
    """
    CREATE TABLE IF NOT EXISTS `category` (
      `category_id` INTEGER PRIMARY KEY,
      `category_name` VARCHAR(255) NOT NULL,
      `category_parent` INTEGER NOT NULL DEFAULT=0
    );
    """

    __tablename__ = "category"

    category_id =     alch.Column(alch.Integer(),   nullable=False, primary_key=True)
    category_name =   alch.Column(alch.String(255), nullable=False)
    category_parent = alch.Column(alch.Integer(),   nullable=False, server_default='0')


class TableFile(CustomBase, Base):
    """
    CREATE TABLE IF NOT EXISTS `file` (
      `file_id` INTEGER PRIMARY KEY,
      `file_name` VARCHAR(255) NOT NULL,
      `path_id` INTEGER NOT NULL,
      `path_to_file` VARCHAR(255) NOT NULL,
      `path_to_torrent_file` VARCHAR(255),
      `url_to_file` VARCHAR(255),
      `md5` VARCHAR(32) UNIQUE,
      FOREIGN KEY (path_id) REFERENCES path(path_id)
    );
    """

    __tablename__ = "file"

    file_id =      alch.Column(alch.Integer(),   nullable=False, primary_key=True)
    file_name =    alch.Column(alch.String(255), nullable=False)
    path_id =      alch.Column(alch.ForeignKey('path.path_id'), nullable=False)
    path_to_file = alch.Column(alch.String(255), nullable=False)
    path_to_torrent_file = alch.Column(alch.String(255), nullable=True)
    url_to_file =  alch.Column(alch.String(255), nullable=True)
    md5 =          alch.Column(alch.String(40),  nullable=True, unique=True)


class TableCategoryFile(CustomBase, Base):
    '''
    CREATE TABLE IF NOT EXISTS `category_file` (
      `category_file_id` INTEGER PRIMARY KEY,
      `category_id` INTEGER,
      `file_id` INTEGER,
      FOREIGN KEY (category_id) REFERENCES category(category_id),
      FOREIGN KEY (file_id) REFERENCES file(file_id)
    );
    '''
    __tablename__ = 'category_file'

    category_file_id = alch.Column(alch.Integer(), nullable=False, primary_key=True)
    category_id =      alch.Column(alch.ForeignKey('category.category_id'), nullable=False)
    file_id =          alch.Column(alch.ForeignKey('file.file_id'), nullable=False)


class PyCataloguer:

    def __init__(self):

        db_path = os.path.join(os.path.dirname(__file__), 'pycataloguer8.db')

        self.c = sqlite3.connect(db_path)
        self.c.row_factory = sqlite3.Row
        self.cu = self.c.cursor()

        self.engine = alch.create_engine('sqlite:///'+db_path, echo=False)
        self.Session = sessionmaker(self.engine)
        self.session = self.Session()

        #print('----------------------------------------')
        #print(dir(self.engine.connect()))
        #print(dir(self.session.connection().contextual_connect))
        #print('----------------------------------------')

        Base.metadata.create_all(self.engine)

    def __enter__(self):
        ''' for 'with' statement '''
        return self

    def __exit__(self, Type, Value, Trace):
        ''' for 'with' statement '''
        self.session.close()

        if Type is None:  # Если исключение не возникло
            pass
        else:             # Если возникло исключение
            return False  # False - исключение не обработано
                          # True  - исключение обработано

    '''def select(self, keys, fields):
        query = self.session.query(keys)
        for field in fields.items():
            if len(field) == 3:
                key, value, operator = field
            else:
                key, value = field
                if isinstance(value, list): operator = 'in'
                else: operator = '=='

            if operator == 'in': field = key.in_(value)
            elif operator == '==': field = key == value
            elif operator == 'not in': field = ~key.in_(value)
            elif operator == '!=': field = key != value
            elif operator == 'like': field = key.like(value)
            query.filter(field)
        return query'''


    '''def path_select(self, **fields):
        for field in fields.items():
            if field[0] in ['path_id', 'path']: field[0] = getattr(TablePath, field[0])
            else: return False, 'Неизвестное поле: ' + field[0]

        query = self.select(TablePath, fields)

        return True, query'''

    def path_add(self, path):
        path = os.path.abspath(path)

        if not os.path.exists(path):
            return False, 'path doesn\'t find: {0}'.format(path)
        if not os.path.isdir(path):
            return False, 'it\'s not a directory: {0}'.format(path)

        row = self.session.query(TablePath).filter(TablePath.path==path).first()
        #row = self.cu.execute('SELECT `path_id` FROM `path` WHERE `path`=?', (path,)).fetchone()
        if row:
            return False, 'The path already exists! Its id is {0}'.format(row['path_id'])


        tblPath = TablePath(path=path)
        self.session.add(tblPath)
        self.session.commit()
        #self.cu.execute('INSERT INTO `path` (`path`) VALUES (?)', (path,))
        #self.c.commit()

        return True, tblPath.path_id

    def path_delete(self, path_id):
        self.session.query(TablePath).filter(TablePath.path_id==path_id).delete()
        self.session.commit()

        #self.cu.execute('DELETE FROM `path` WHERE `path_id`=?', (path_id,))
        #self.c.commit()
        return True, None

    """def file_is_exists(self, path_to_file):
        if not os.path.exists(path_to_file):
            return False, 'file doesn\'t find: {0}'.format(path_to_file)
        if not os.path.isfile(path_to_file):
            return False, 'it\'s not a file: {0}'.format(path_to_file)
        return True, None

    def file_is_duplicate(self, path_id, short_path_to_file, md5):
        expr = alch.or_(TableFile.md5 == md5, alch.and_(TableFile.path_id == path_id, TableFile.path_to_file == short_path_to_file))
        if self.session.query(TableFile).filter(expr).count():
            return False, 'Данный файл уже содержится в базе!'
        return True, None

    def file_get_path_id(self, path_to_file):
        #is_success, paths = self.path_select()
        #if  not is_success: return paths
        paths = self.session.query(TablePath)
        for path in paths:
            path = row2dict(path)
            if path_to_file.startswith(path['path']):
                short_path_to_file = path_to_file[len(path['path'])+1:] # +1 необходимо для обрезки слэша, так как это не абсолютный путь
                return True, (path['path_id'], short_path_to_file)
        return False, 'File placed in not allowed place! Please, do "pycat pathselect" to view allowed place.'"""

    def path_update(self, path_id, fields):
        allowed = ['path']
        for key in fields:
            if key not in allowed: return False, 'Editing this property is not allowed. You can use next properties: {0}.'.format(', '.join(alloowed))


        self.session.query(TablePath).filter(TablePath.path_id==path_id).update(fields)
        self.session.commit()

        return True, None

    def file_check(self, path_to_file):
        data = {}

        # существование файла
        if not os.path.exists(path_to_file):
            return False, 'file doesn\'t find: {0}'.format(path_to_file)
        if not os.path.isfile(path_to_file):
            return False, 'it\'s not a file: {0}'.format(path_to_file)

        # разрешённость пути вайла
        #is_success, paths = self.path_select()
        #if  not is_success: return paths
        paths = self.session.query(TablePath)
        for path in paths:
            path = row2dict(path)
            if path_to_file.startswith(path['path']):
                data['path_id'] = path['path_id']
                data['path_to_file'] = path_to_file[len(path['path'])+1:] # +1 необходимо для обрезки слэша, так как это не абсолютный путь
        if 'path_id' not in data: return False, 'File placed in not allowed place! Please, do "pycat pathselect" to view allowed place.'

        # берём хеш
        with open(path_to_file, 'rb') as f:
            data['md5'] = hashlib.md5(f.read()).hexdigest()

        # наличие файла в базе
        expr = alch.or_(TableFile.md5 == data['md5'], alch.and_(TableFile.path_id == data['path_id'], TableFile.path_to_file == data['path_to_file']))
        if self.session.query(TableFile).filter(expr).count():
            return False, 'Данный файл уже содержится в базе!'

        return True, data


    def file_add(self, name, full_path_to_file):
        full_path_to_file = os.path.abspath(full_path_to_file)

        is_success, data = self.file_check(full_path_to_file)
        if not is_success: return False, data

        """# определяем путь к директории поиска
        is_success, data = self.file_get_path_id(path_to_file)
        if not is_success: return False, msg
        else: path_id, short_path_to_file = data

        is_success, msg = self.file_is_exists(path_to_file)
        if not is_success: return False, msg

        with open(path_to_file, 'rb') as f:
            md5 = hashlib.md5(f.read()).hexdigest()

        is_success, msg = self.file_is_duplicate(path_id, short_path_to_file, md5)
        if not is_success: return False, msg"""

        tblFile = TableFile(file_name=name, path_id=data['path_id'], path_to_file=data['path_to_file'], md5=data['md5'])
        self.session.add(tblFile)
        self.session.commit()
        #self.cu.execute('INSERT INTO `file` (`file_name`, `path_id`, `path_to_file`, `md5`) VALUES (?, ?, ?, ?)', (name, path_id, short_path_to_file, md5))
        #self.c.commit()
        return True, tblFile.file_id

    def file_delete(self, file_id):
        self.session.query(TableCategoryFile).filter(TableCategoryFile.file_id==file_id).delete()
        self.session.query(TableFile).filter(TableFile.file_id==file_id).delete()
        self.session.commit()
        #self.cu.execute('DELETE FROM `category_file` WHERE `file_id`=?', (file_id,))
        #self.cu.execute('DELETE FROM `file` WHERE `file_id`=?', (file_id,))
        #self.c.commit()
        return True, None

    def file_update(self, file_id, fields):
        allowed = ['file_name', 'path_to_file']
        for key in fields:
            if key not in allowed: return False, 'Editing this property is not allowed. You can use next properties: {0}.'.format(', '.join(alloowed))

        if 'path_to_path' in fields:
            is_success, data = self.file_check(fields['path_to_file'])
            if not is_success: return False, data

            fields.update(data)

        self.session.query(TableFile).filter(TableFile.file_id==file_id).update(fields)
        self.session.commit()

        #values = []
        #keys = []
        #for key, value in fields.items():
        #    keys.append('`'+key+'`=?')
        #    values.append(value)

        #values.append(file_id)
        #self.cu.execute('UPDATE `file` SET {keys} WHERE `file_id`=?'.format(keys=','.join(keys)), tuple(values))
        #self.c.commit()
        return True, None

    '''def file_select(self, **fields):
        for field in fields.items():
            if field[0] in dir(TablePath): field[0] = getattr(TableFile, field[0])
            else: return False, 'Неизвестное поле: ' + field[0]

        query = self.select(TableFile, fields)

        return True, query

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
        return True, rows'''

    def query(self, sql):
        rows = self.cu.execute(sql).fetchall()
        return True, rows

    def show_item_raw(self, dct):
        for key in dct.keys(): print('{GREEN} {key} = {ORANGE} {value} {NORM}'.format(key=key, value=dct[key], GREEN=GREEN, ORANGE=ORANGE, NORM=NORM))

    def show_item_file(self, file):
        file = dict(file)
        file['path_to_file'] = os.path.join(file['path'], file['path_to_file'])
        del file['path_id'], file['path']

        file = {key: value for key, value in file.items() if value is not None}

        self.show_item_raw(file)

    def category_add(self, name, parent=0):
        tblCategory = TableCategory(category_name=name, category_parent=parent)
        res = self.session.add(tblCategory)
        res2 = self.session.commit()
        #print(res, res2)

        #for attr in dir(self.session):
        #    print(attr, getattr(self.session, attr))

        #self.cu.execute('INSERT INTO `category` (`category_name`, `category_parent`) VALUES (?, ?)', (name, parent))
        #self.c.commit()
        return True, tblCategory.category_id

    '''def category_select(self):
        rows = self.engine.execute('SELECT * FROM `category`').fetchall()
        return True, rows'''

    def category_delete(self, category_id):
        # проверяем, не используется ли категория
        category_file = self.session.query(TableCategoryFile).filter(TableCategoryFile.category_id == category_id).count()
        if category_file > 0:
            return False, 'Категория {0} используется в количестве файлов: {1}'.filter(category_id, category_file)

        self.session.query(TableCategory).filter(TableCategory.category_id==category_id).delete()
        self.session.commit()

        #self.cu.execute('DELETE FROM `category` WHERE `category_id`=?', (category_id,))
        #self.c.commit()
        return True, None

    def category_update(self, category_id, fields):
        allowed = ['category_name', 'category_parent']
        for key in fields:
            if key not in allowed: return False, 'Editing this property is not allowed. You can use next properties: {0}.'.format(', '.join(alloowed))

        self.session.query(TableCategory).filter(TableCategory.category_id == category_id).update(fields)
        self.session.commit()

        return True, None

if __name__ == "__main__":
    import argparse

    def proc_answer(is_success, arg1):
        if not is_success:
            print('{READ} {0} {NORM}'.format(arg1, READ=READ, NORM=NORM))
            exit(2)

    with PyCataloguer() as cat:

        parser = argparse.ArgumentParser(description='Cataloguer of everything')
        subparsers = parser.add_subparsers(dest='command')
        parser.add_argument('--version', action='version', version='%(prog)s 0.01')

        subparser1 = subparsers.add_parser('fileadd', help='adding your files to db')
        subparser1.add_argument('name')
        subparser1.add_argument('path')

        subparser2 = subparsers.add_parser('fileselect', help='view your files')
        subparser2.add_argument('name', nargs='*', default=['%'])
        subparser2.add_argument('--view', choices=['simple', 'props', 'paths', 'raw'], default='simple')

        subparser3 = subparsers.add_parser('query', help='do sql-query')
        subparser3.add_argument('sql', help='any sql')

        subparser4 = subparsers.add_parser('fileprops', help='view data about file')
        subparser4.add_argument('file_id', help='id of file')
        subparser4.add_argument('--general', action='store_true')
        subparser4.add_argument('--categories', action='store_true')

        subparser5 = subparsers.add_parser('filerm', help='remove files from db')
        subparser5.add_argument('file_id', help='id of file', nargs='+')

        subparser6 = subparsers.add_parser('pathselect', help='show allowed directories')

        subparser7 = subparsers.add_parser('pathadd', help='add allowed directories')
        subparser7.add_argument('path', help='path', nargs='+')

        subparser8 = subparsers.add_parser('export', help='view dump of database')

        subparser9 = subparsers.add_parser('import', help='view dump of database')
        subparser9.add_argument('sql_file', help='path to sql dump', type=argparse.FileType('r'))

        subparser10 = subparsers.add_parser('pathrm', help='remove paths from db')
        subparser10.add_argument('path_id', help='id of path', nargs='+')

        subparser11 = subparsers.add_parser('filescan', help='scan directories for new files')
        subparser11.add_argument('--path_id', help='id of path', default=None)
 
        subparser12 = subparsers.add_parser('categoryadd', help='add new categroy')
        subparser12.add_argument('--parent', help='id of parent category', default="0")
        subparser12.add_argument('name', help='names of categories', nargs="+")

        subparser13 = subparsers.add_parser('categoryselect', help='view categroies')

        subparser14 = subparsers.add_parser('fileupdate', help='')
        subparser14.add_argument('file_id', help='id of file')
        subparser14.add_argument('property_name')
        subparser14.add_argument('property_value')

        subparser14 = subparsers.add_parser('categoryupdate', help='')
        subparser14.add_argument('category_id', help='id of path')
        subparser14.add_argument('property_name')
        subparser14.add_argument('property_value')

        subparser14 = subparsers.add_parser('pathupdate', help='')
        subparser14.add_argument('category_id', help='id of category')
        subparser14.add_argument('property_name')
        subparser14.add_argument('property_value')

        subparser15 = subparsers.add_parser('categoryrm', help='remove categories from db')
        subparser15.add_argument('category_id', help='id of category', nargs='+')

        args = parser.parse_args()
        #print(args)
        #print()

        if args.command == 'fileadd':

            is_success, file_id = cat.file_add(args.name, args.path)
            proc_answer(is_success, file_id)
            print(file_id)
       
        elif args.command == 'fileselect':

            '''if len(args.name) == 1: file_name = ('LIKE', args.name)
            else: file_name = ('IN', args.name)

            is_success, files = cat.file_select(file_name=file_name)
            proc_answer(is_success, files)'''

            names = [TableFile.path_id == TablePath.path_id]
            for name in args.name:
                names.append(TableFile.file_name.like(name))

            files = cat.session.query(TableFile, TablePath).filter(alch.and_(*names))

            for file in files:
                file = row2dict(file)
                if args.view == 'simple':
                    print(file['file_id'], file['file_name'])
                elif args.view == 'paths':
                    print(os.path.join(file['path'], file['path_to_file']))
                elif args.view == 'props':
                    print('---------------')
                    cat.show_item_file(file)
                elif args.view == 'raw':
                    print('---------------')
                    cat.show_item_raw(file)

        elif args.command == 'query':

            is_success, rows = cat.query(args.sql)
            proc_answer(is_success, rows)
            for row in rows:
                print('---------------')
                for key in row.keys(): print(key, '=', row[key])

        elif args.command == 'fileprops':

            is_success, files = cat.file_select(file_id=('=', args.file_id))
            proc_answer(is_success, files)
            if len(files) == 1:
                cat.show_item_file(files[0])

        elif args.command == 'filerm':

            for file_id in args.file_id:
                is_success, none = cat.file_delete(file_id)
                proc_answer(is_success, none)

        elif args.command == 'pathrm':

            for path_id in args.path_id:
                is_success, none = cat.path_delete(path_id)
                proc_answer(is_success, none)

        elif args.command == 'pathselect':

            #is_success, paths = cat.path_select()
            #proc_answer(is_success, paths)
            paths = cat.session.query(TablePath)
            for path in paths: print(path.path_id, path.path)

        elif args.command == 'pathadd':

            for path in args.path:
                is_success, path_id = cat.path_add(path)
                proc_answer(is_success, path_id)
                print(path_id)

        elif args.command == 'filescan':

            is_success, paths = cat.path_select(args.path_id)
            proc_answer(is_success, paths)
            print(' # Press Enter if you want to ignore (skip) the file')
            for path in paths:
                print(ORANGE, '# scan in {0}'.format(path['path']), NORM, '\n')
                is_break = False
                for root, dirs, files in os.walk(path['path']):
                    for file in files:
                        path_to_file = os.path.join(root, file)
                        # Проверяем наличие файла в базе
                        with open(path_to_file, 'rb') as f:
                            md5 = hashlib.md5(f.read()).hexdigest()
                            is_success, rows = cat.file_select(md5=('=', [md5]))
                            proc_answer(is_success, rows)
                            if len(rows) == 1:
                                continue

                        print(ORANGE, root)
                        print(GREEN, file, NORM)
                        name = input().strip()
                        print('\x1b[4A\x1b[J')# clear = '\x1b[3;J\x1b[H\x1b[2J'

                        if not name: continue
                        # добавляем файл в базу
                        is_success, file_id = cat.file_add(name, path_to_file)
                        proc_answer(is_success, file_id)

                    if is_break: break

        elif args.command == 'export':

            for line in cat.c.iterdump():
                print(line)

        elif args.command == 'import': 

            rows = cat.c.executescript(args.sql_file.read())
            cat.c.commit()
            if (rows):
                for row in rows: print(dict(row))

        elif args.command == 'categoryadd':

            for name in args.name:
                is_success, category_id =  cat.category_add(name, args.parent)
                proc_answer(is_success, category_id)
                print(category_id, end=' ')
            print('\n', end='')

        elif args.command == 'categoryselect':

            #is_success, categories = cat.category_select()
            #proc_answer(is_success, categories)
            categories = cat.session.query(TableCategory)
            for category in categories:
                print(category.category_id, category.category_name, category.category_parent)

        elif args.command == 'categoryrm':

            for category_id in args.category_id:
                is_success, none = cat.category_delete(category_id)
                proc_answer(is_success, none)


        elif args.command == 'fileupdate':

            is_success, none = cat.file_update(args.file_id, {args.property_name:args.property_value})
            proc_answer(is_success, none)

        elif args.command == 'categoryupdate':

            is_success, none = cat.category_update(args.category_id, {args.property_name:args.property_value})
            proc_answer(is_success, none)

        elif args.command == 'pathupdate':

            is_success, none = cat.path_update(args.path_id, {args.property_name:args.property_value})
            proc_answer(is_success, none)            