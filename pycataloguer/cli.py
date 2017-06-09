#! /usr/bin/env python3

import argparse
import os
import zipfile

import sqlalchemy as alch

import pycataloguer as pycat

def print_error(msg):
    print('{READ} {0} {NORM}'.format(msg, READ=pycat.READ, NORM=pycat.NORM))

def proc_answer(is_success, arg1):
    if not is_success:
        print_error(arg1)
        exit(2)

def do_cmd():

    with pycat.PyCataloguer() as cat:

        parser = argparse.ArgumentParser(description='Cataloguer of everything')
        subparsers = parser.add_subparsers(dest='command')
        parser.add_argument('--version', action='version', version='%(prog)s 0.01')

        subparser = subparsers.add_parser('fileadd', help='adding your files to db')
        subparser.add_argument('name')
        subparser.add_argument('path')

        subparser = subparsers.add_parser('fileselect', help='view your files')
        subparser.add_argument('--file_name', nargs='*', default=None)
        subparser.add_argument('--file_id', nargs='*', default=None)
        subparser.add_argument('--category_id', nargs='*', default=None)
        subparser.add_argument('--view', choices=['simple', 'props', 'paths', 'raw'], default='simple')

        subparser = subparsers.add_parser('query', help='do sql-query')
        subparser.add_argument('sql', help='any sql')

        subparser = subparsers.add_parser('fileprops', help='view data about file')
        subparser.add_argument('file_id', help='id of file')
        subparser.add_argument('--general', action='store_true')
        subparser.add_argument('--categories', action='store_true')

        subparser = subparsers.add_parser('filerm', help='remove files from db')
        subparser.add_argument('file_id', help='id of file', nargs='+')

        subparser = subparsers.add_parser('pathselect', help='show allowed directories')

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

        subparser = subparsers.add_parser('categoryselect', help='view categroies')

        subparser = subparsers.add_parser('fileupdate', help='')
        subparser.add_argument('file_id', help='id of file')
        subparser.add_argument('property_name')
        subparser.add_argument('property_value')

        subparser = subparsers.add_parser('categoryupdate', help='')
        subparser.add_argument('category_id', help='id of path')
        subparser.add_argument('property_name')
        subparser.add_argument('property_value')

        subparser = subparsers.add_parser('pathupdate', help='')
        subparser.add_argument('category_id', help='id of category')
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
        #print(args)
        #print()

        if args.command == 'fileadd':

            is_success, file_id = cat.file_add(args.name, args.path)
            proc_answer(is_success, file_id)
            print(file_id)

        elif args.command == 'fileselect':

            conds = [pycat.TableFile.path_id == pycat.TablePath.path_id]
            tables = [pycat.TableFile, pycat.TablePath]

            if args.file_id is not None: conds.append(pycat.TableFile.file_id.in_(args.file_id))
            if args.category_id is not None:
                tables.append(pycat.TableCategoryFile)
                conds.append(pycat.TableCategoryFile.file_id == pycat.TableFile.file_id)
                conds.append(pycat.TableCategoryFile.category_id.in_(args.category_id))
            if args.file_name is not None:
                for file_name in args.file_name:
                    conds.append(pycat.TableFile.file_name.ilike(file_name))

            files = cat.session.query(*tables).filter(alch.and_(*conds))
            cat.show_items_file_by_format(files, args.view)
       
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
            paths = cat.session.query(pycat.TablePath)
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
                print(pycat.ORANGE, '# scan in {0}'.format(path['path']), pycat.NORM, '\n')
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

                        print(pycat.ORANGE, root)
                        print(pycat.GREEN, file, pycat.NORM)
                        name = input().strip()
                        print('\x1b[4A\x1b[J')# clear = '\x1b[3;J\x1b[H\x1b[2J'

                        if not name: continue
                        # добавляем файл в базу
                        is_success, file_id = cat.file_add(name, path_to_file)
                        proc_answer(is_success, file_id)

                    if is_break: break

        elif args.command == 'export':

            path = os.path.join(os.path.dirname(__file__))

            if  args.format == 'raw':

                f = os.path.join(path, 'dump.sql')
                f = open(f, 'w')
                for line in cat.c.iterdump():
                    f.write(line)
                    f.write('\n')
                f.close()

            elif args.format == 'csv':

                fzip = zipfile.ZipFile(os.path.join(path, 'export.zip'), 'w', zipfile.ZIP_DEFLATED)

                cat.export(pycat.TablePath, path, fzip)
                cat.export(pycat.TableFile, path, fzip)
                cat.export(pycat.TableCategory, path, fzip)
                cat.export(pycat.TableCategoryFile, path, fzip)

        elif args.command == 'import': 

            path = os.path.join(os.path.dirname(__file__))

            if args.format == 'raw':

                f = os.path.join(path, 'dump.sql')
                f = open(f, 'r')
                rows = cat.c.executescript(f.read())
                cat.c.commit()
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
                        cat.session.add(tbl)
                    cat.session.commit()

                    os.remove(fpath)

        elif args.command == 'categoryadd':

            for name in args.name:
                is_success, category_id =  cat.category_add(name, args.parent)
                proc_answer(is_success, category_id)
                print(category_id, end=' ')
            print('\n', end='')

        elif args.command == 'categoryselect':

            #is_success, categories = cat.category_select()
            #proc_answer(is_success, categories)

            #categories = cat.session.query(TableCategory)
            #for category in categories:
            #    print(category.category_id, category.category_name, category.category_parent)

            cat.category_print()

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

        elif args.command == 'file2category':

            for file_id in args.file_id:
                is_success, message = cat.file_add2category(file_id, args.category_id)
                if not is_success: print_error(message)

        elif args.command == 'check':

            conds = [pycat.TableFile.path_id == pycat.TablePath.path_id]
            tables = [pycat.TableFile, pycat.TablePath]

            files = cat.session.query(*tables).filter(alch.and_(*conds))

            for file in files:
                file = row2dict(file)

                path = os.path.join(file['path'], file['path_to_file'])

                if not os.path.exists(path):
                    print_error('File {file_id} doesn\'t exists'.format(file_id=file['file_id']))
                    continue

                with open(path, 'rb') as f:
                    if hashlib.md5(f.read()).hexdigest() != file['md5']:
                        print_error('File {file_id} has unmatched hash'.format(file_id=file['file_id']))
                        continue

        elif args.command == 'filerecalc':

            conds = [pycat.TableFile.path_id == pycat.TablePath.path_id, pycat.TableFile.file_id == args.file_id]
            file = cat.session.query(pycat.TableFile, pycat.TablePath).filter(alch.and_(*conds)).first()
            
            file = row2dict(file)

            is_success, fields = cat.file_check(os.path.join(file['path'], file['path_to_file']), check_having=False)
            proc_answer(is_success, fields)

            cat.file_update(file['file_id'], fields)

        elif args.command == 'dbpath':

            print(cat.db_path)