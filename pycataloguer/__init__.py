#! /usr/bin/env python3

import sqlite3
import sys
import os
import hashlib
import csv

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
        return {column.name: getattr(self, column.name) for column in self.__table__.columns}

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

        db_path = os.path.join(os.path.dirname(__file__), 'pycataloguer.db')

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

    def file_check(self, path_to_file, check_existing=True, check_having=True):
        data = {}

        # существование файла
        if check_existing:
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
        if check_having:
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

        if 'path_to_file' in fields:
            is_success, data = self.file_check(os.path.abspath(fields['path_to_file']), check_having=False)
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

    def file_add2category(self, file_id, category_ids):
        errs = []
        tbls = []

        for category_id in category_ids:
            if self.session.query(TableCategoryFile).filter(alch.and_(TableCategoryFile.file_id==file_id, TableCategoryFile.category_id==category_id)).count():
                errs.append('The file {0} is in category {1} already.'.format(file_id, category_id))
                continue

            tbls.append(TableCategoryFile(file_id=file_id, category_id=category_id))

        self.session.add_all(tbls)
        self.session.commit()

        if errs: return False, '\n'.join(errs)
        return True, None

    def query(self, sql):
        rows = self.cu.execute(sql).fetchall()
        return True, rows

    def show_item_raw(self, dct):
        for key in dct.keys(): print('{GREEN} {key} = {ORANGE} {value} {NORM}'.format(key=key, value=dct[key], GREEN=GREEN, ORANGE=ORANGE, NORM=NORM))

    def show_item_file(self, file):
        file = dict(file)

        # склеиваем путь 
        file['path_to_file'] = os.path.join(file['path'], file['path_to_file'])
        # склеиваем имя с id
        file['file_name'] = '{0} ({1})'.format(file['file_name'], file['file_id'])

        # добавляем категории
        file['categories'] = []
        categories = self.session.query(TableCategoryFile, TableCategory).filter(alch.and_(TableCategoryFile.category_id==TableCategory.category_id, TableCategoryFile.file_id==file['file_id']))
        for category in categories:
            file['categories'].append('{0} ({1})'.format(category.TableCategory.category_name, category.TableCategory.category_id))
        file['categories'] = '; '.join(file['categories'])

        # удаляем пустые илил избыточные значения
        keys = ['path_id', 'path', 'file_id', 'category_file_id', 'category_id']
        file = {key: value for key, value in file.items() if value and key not in keys}

        self.show_item_raw(file)

    def show_items_file_by_format(self, file, view_format):
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

    def category_print(self, parent_id=0, level=0):
        ''' @todo Make it using nested sets '''
        categories = self.session.query(TableCategory).filter(TableCategory.category_parent==parent_id)
        for category in categories:
            print('   '*level, category.category_id, category.category_name, category.category_parent)
            self.category_print(category.category_id, level+1)

        #rows = self.engine.execute('SELECT * FROM `category`').fetchall()
        #return True, rows

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

    def export(self, Table, path, fzip):
        fname = '{0}.csv'.format(Table.__tablename__)
        fpath = os.path.join(path, fname)
        f = open(fpath, 'w')
        fcsv = csv.writer(f)

        # формируем и записываем заголовки полей
        field_names = []
        for column in Table.__table__.columns:
            field_names.append(column.name)
        fcsv.writerow(field_names)

        # вынимаем из базы записываем значения полей
        for row in self.session.query(Table):
            field_values = [getattr(row, name) for name in field_names]
            fcsv.writerow(field_values)

        # архивируем файл, а после - удаляем неархивированный файл
        f.close()
        fzip.write(fpath, fname)
        os.remove(fpath)