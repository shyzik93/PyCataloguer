#! /usr/bin/env python3

import argparse
import os
import zipfile

import sqlalchemy as alch

import pycataloguer as pycat

READ = '\033[0;31m'
NORM = '\033[0;0m'
GREEN= '\033[0;32m'
ORANGE = '\033[0;33m'

def print_error(msg):
    print('{READ} {0} {NORM}'.format(msg, READ=READ, NORM=NORM))

def proc_answer(is_success, arg1):
    if not is_success:
        print_error(arg1)
        exit(2)

class CLI(pycat.PyCataloguer):

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

    def show_items_file_by_format(self, files, view_format):
        for file in files:
            file = pycat.row2dict(file)
            if view_format == 'simple':
                print(file['file_id'], file['file_name'])
            elif view_format == 'paths':
                print(os.path.join(file['path'], file['path_to_file']))
            elif view_format == 'props':
                print('---------------')
                cat.show_item_file(file)
            elif view_format == 'raw':
                print('---------------')
                cat.show_item_raw(file)

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

    def cmd_fileadd(self, args):
        is_success, file_id = self.file_add(args.name, args.path)
        proc_answer(is_success, file_id)
        print(file_id)

    def cmd_fileselect(self, args):
        conds = [pycat.TableFile.path_id == pycat.TablePath.path_id]
        tables = [pycat.TableFile, pycat.TablePath]

        if args.path_id is not None: conds.append(pycat.TablePath.path_id.in_(args.path_id))        
        if args.file_id is not None: conds.append(pycat.TableFile.file_id.in_(args.file_id))
        if args.category_id is not None:
            tables.append(pycat.TableCategoryFile)
            conds.append(pycat.TableCategoryFile.file_id == pycat.TableFile.file_id)
            conds.append(pycat.TableCategoryFile.category_id.in_(args.category_id))
        if args.file_name is not None:
            for file_name in args.file_name:
                conds.append(pycat.TableFile.file_name.ilike(file_name))

        files = self.session.query(*tables).filter(alch.and_(*conds))
        self.show_items_file_by_format(files, args.view)

    def cmd_query(self, args):
        is_success, rows = self.query(args.sql)
        proc_answer(is_success, rows)
        for row in rows:
            print('---------------')
            for key in row.keys(): print(key, '=', row[key])

    def cmd_fileprops(self, args):
        is_success, files = self.file_select(file_id=('=', args.file_id))
        proc_answer(is_success, files)
        if len(files) == 1:
            self.show_item_file(files[0])

    def cmd_filerm(self, args):
        for file_id in args.file_id:
            is_success, none = self.file_delete(file_id)
            proc_answer(is_success, none)

    def cmd_pathrm(self, args):
        for path_id in args.path_id:
            is_success, none = self.path_delete(path_id)
            proc_answer(is_success, none)

    def cmd_pathselect(self, args):
        #is_success, paths = cat.path_select()
        #proc_answer(is_success, paths)
        conds = []
        tables = [pycat.TablePath]

        if args.path_id is not None: conds.append(pycat.TablePath.path_id.in_(args.path_id))        

        paths = self.session.query(*tables).filter(alch.and_(*conds))
        for path in paths: print(path.path_id, path.path)

    def cmd_pathadd(self, args):
        for path in args.path:
            is_success, path_id = self.path_add(path)
            proc_answer(is_success, path_id)
            print(path_id)

    def cmd_filescan(self, args):
            is_success, paths = self.path_select(args.path_id)
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
                            is_success, rows = self.file_select(md5=('=', [md5]))
                            proc_answer(is_success, rows)
                            if len(rows) == 1:
                                continue

                        print(ORANGE, root)
                        print(GREEN, file, NORM)
                        name = input().strip()
                        print('\x1b[4A\x1b[J')# clear = '\x1b[3;J\x1b[H\x1b[2J'

                        if not name: continue
                        # добавляем файл в базу
                        is_success, file_id = self.file_add(name, path_to_file)
                        proc_answer(is_success, file_id)

                    if is_break: break

    def cmd_export(self, args):
            path = os.path.join(os.path.dirname(__file__))

            if  args.format == 'raw':

                f = os.path.join(path, 'dump.sql')
                f = open(f, 'w')
                for line in self.c.iterdump():
                    f.write(line)
                    f.write('\n')
                f.close()

            elif args.format == 'csv':

                fzip = zipfile.ZipFile(os.path.join(path, 'export.zip'), 'w', zipfile.ZIP_DEFLATED)

                self.export(pycat.TablePath, path, fzip)
                self.export(pycat.TableFile, path, fzip)
                self.export(pycat.TableCategory, path, fzip)
                self.export(pycat.TableCategoryFile, path, fzip)

    def cmd_import(self, args):
            path = os.path.join(os.path.dirname(__file__))

            if args.format == 'raw':

                f = os.path.join(path, 'dump.sql')
                f = open(f, 'r')
                rows = self.c.executescript(f.read())
                self.c.commit()
                if (rows):
                    for row in rows: print(dict(row))

            elif args.format == 'csv':

                fzip = zipfile.ZipFile(args.archive, 'r')
                for fname in fzip.namelist():
                    # Определяем класс таблицы
                    Table = fname.split('.')[0]
                    Table = 'Table' + Table.title().replace('_', '')
                    Table = globals()[Table]

                    # Разархивируем файл
                    fpath = os.path.join(path, fname)
                    f2 = open(fpath, 'wb')
                    f = fzip.open(fname, 'r')
                    for line in f:
                        f2.write(line)
                    f2.close

                    # Импортируем
                    f2 = open(fpath, 'r')
                    fcsv = csv.reader(f2)
                    field_names = []
                    for index, field_values in enumerate(fcsv):
                        if index == 0:
                            field_names = field_values
                            continue
                        fields = {key: value for key, value in zip(field_names, field_values)}
                        tbl = Table(**fields)
                        self.session.add(tbl)
                    self.session.commit()

                    os.remove(fpath)

    def cmd_categoryadd(self, args):
        for name in args.name:
            is_success, category_id =  self.category_add(name, args.parent)
            proc_answer(is_success, category_id)
            print(category_id, end=' ')
        print('\n', end='')

    def cmd_categoryselect(self, args):
        #is_success, categories = cat.category_select()
        #proc_answer(is_success, categories)

        #categories = cat.session.query(TableCategory)
        #for category in categories:
        #    print(category.category_id, category.category_name, category.category_parent)

        self.category_print()

    def cmd_categoryrm(self, args):
        for category_id in args.category_id:
            is_success, none = self.category_delete(category_id)
            proc_answer(is_success, none)

    def cmd_fileupdate(self, args):
        is_success, none = self.file_update(args.file_id, {args.property_name:args.property_value})
        proc_answer(is_success, none)


    def cmd_categoryupdate(self, args):
        is_success, none = self.category_update(args.category_id, {args.property_name:args.property_value})
        proc_answer(is_success, none)

    def cmd_pathupdate(self, args):
        is_success, none = self.path_update(args.path_id, {args.property_name:args.property_value})
        proc_answer(is_success, none)            

    def cmd_file2category(self, args):
        for file_id in args.file_id:
            is_success, message = self.file_add2category(file_id, args.category_id)
            if not is_success: print_error(message)

    def cmd_check(self, args):
            conds = [pycat.TableFile.path_id == pycat.TablePath.path_id]
            tables = [pycat.TableFile, pycat.TablePath]

            files = self.session.query(*tables).filter(alch.and_(*conds))

            for file in files:
                file = pycat.row2dict(file)

                path = os.path.join(file['path'], file['path_to_file'])

                if not os.path.exists(path):
                    print_error('File {file_id} doesn\'t exists'.format(file_id=file['file_id']))
                    continue

                with open(path, 'rb') as f:
                    if hashlib.md5(f.read()).hexdigest() != file['md5']:
                        print_error('File {file_id} has unmatched hash'.format(file_id=file['file_id']))
                        continue

    def cmd_filerecalc(self, args):
        conds = [pycat.TableFile.path_id == pycat.TablePath.path_id, pycat.TableFile.file_id == args.file_id]
        file = self.session.query(pycat.TableFile, pycat.TablePath).filter(alch.and_(*conds)).first()
        
        file = pycat.row2dict(file)

        is_success, fields = self.file_check(os.path.join(file['path'], file['path_to_file']), check_having=False)
        proc_answer(is_success, fields)

        self.file_update(file['file_id'], fields)

    def cmd_dbpath(self, args):
        yield 1
        print(self.db_path)

def do_cmd():

    with CLI() as cat:

        parser = argparse.ArgumentParser(description='Cataloguer of everything')
        subparsers = parser.add_subparsers(dest='command')
        parser.add_argument('--version', action='version', version='%(prog)s 0.01')

        subparser = subparsers.add_parser('fileadd', help='adding your files to db')
        subparser.add_argument('name')
        subparser.add_argument('path')

        subparser = subparsers.add_parser('query', help='do sql-query')
        subparser.add_argument('sql', help='any sql')

        subparser = subparsers.add_parser('fileprops', help='view data about file')
        subparser.add_argument('file_id', help='id of file')
        subparser.add_argument('--general', action='store_true')
        subparser.add_argument('--categories', action='store_true')

        subparser = subparsers.add_parser('filerm', help='remove files from db')
        subparser.add_argument('file_id', help='id of file', nargs='+')


        subparser = subparsers.add_parser('pathselect', help='show allowed directories')
        subparser.add_argument('--path_id', nargs='*', default=None)

        subparser = subparsers.add_parser('fileselect', help='view your files')
        subparser.add_argument('--path_id', nargs='*', default=None)
        subparser.add_argument('--file_name', nargs='*', default=None)
        subparser.add_argument('--file_id', nargs='*', default=None)
        subparser.add_argument('--category_id', nargs='*', default=None)
        subparser.add_argument('--view', choices=['simple', 'props', 'paths', 'raw'], default='simple')


        subparser = subparsers.add_parser('categoryselect', help='view categroies')


        subparser = subparsers.add_parser('pathadd', help='add allowed directories')
        subparser.add_argument('path', help='path', nargs='+')

        subparser = subparsers.add_parser('export', help='view dump of database')
        subparser.add_argument('--format', default='csv')

        subparser = subparsers.add_parser('import', help='view dump of database')
        #subparser.add_argument('sql_file', help='path to sql dump', type=argparse.FileType('r'))
        subparser.add_argument('--format', default='csv')
        subparser.add_argument('archive', help='path to archive')

        subparser = subparsers.add_parser('pathrm', help='remove paths from db')
        subparser.add_argument('path_id', help='id of path', nargs='+')

        subparser = subparsers.add_parser('filescan', help='scan directories for new files')
        subparser.add_argument('--path_id', help='id of path', default=None)
 
        subparser = subparsers.add_parser('categoryadd', help='add new categroy')
        subparser.add_argument('--parent', help='id of parent category', default="0")
        subparser.add_argument('name', help='names of categories', nargs="+")


        subparser = subparsers.add_parser('fileupdate', help='')
        subparser.add_argument('file_id', help='id of file')
        subparser.add_argument('property_name')
        subparser.add_argument('property_value')

        subparser = subparsers.add_parser('categoryupdate', help='')
        subparser.add_argument('category_id', help='id of path')
        subparser.add_argument('property_name')
        subparser.add_argument('property_value')

        subparser = subparsers.add_parser('pathupdate', help='')
        subparser.add_argument('path_id', help='id of category')
        subparser.add_argument('property_name')
        subparser.add_argument('property_value')


        subparser = subparsers.add_parser('categoryrm', help='remove categories from db')
        subparser.add_argument('category_id', help='id of category', nargs='+')

        subparser = subparsers.add_parser('file2category', help='add file into categories')
        subparser.add_argument('--file_id', help='id of file', nargs='+')
        subparser.add_argument('--category_id', help='id of file', nargs='+')

        subparser = subparsers.add_parser('check', help='')

        subparser = subparsers.add_parser('filerecalc', help='readd file into categories')
        subparser.add_argument('file_id', help='id of file')

        subparser = subparsers.add_parser('dbpath', help='show path to database')


        args = parser.parse_args()

        func = getattr(cat, 'cmd_'+args.command)
        res = func(args)

        '''for is_success, _res in res:
            if is_success: print(_res)
            else: print_error(_res)
        '''

        '''if isinstance(res, tuple) and len(res) == 2:
            is_success, res = res
            if success: print(res)
            else: print_error(res)
        else:
            print(type(res))
            if res is not None: print(res)
        '''