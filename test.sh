#!/usr/bin/env bash

pycat=`dirname $0`
pycat="$pycat/run.py"

mkdir /tmp/dir1 /tmp/dir2

echo 'text1' > /tmp/dir1/file1
echo 'text2' > /tmp/dir1/file2
echo 'text3' > /tmp/dir1/file3

# проверяем, что всё чисто

$pycat pathselect
$pycat fileselect
$pycat categoryselect

# добавляем данные

$pycat pathadd /tmp/dir1
$pycat pathadd /tmp/dir2

$pycat fileadd 'My File1' /tmp/dir1/file1
$pycat fileadd 'My File2' /tmp/dir1/file2
$pycat fileadd 'My File3' /tmp/dir1/file3

$pycat categoryadd 'My category1' 'My category2' 'My category3'
$pycat categoryadd --parent 2 'My category4' 'My category5'
$pycat categoryadd --parent 5 'My category6'

# меняем данные

#$pycat pathupdate 1 path_name 'Do not do this without reason'
$pycat fileupdate 2 file_name 'My updated file'
$pycat categoryupdate 6 category_name 'My updated category'

# выводи данные

echo -e '\033[0;32mPaths:\033[0;0m'
$pycat pathselect
echo -e '\033[0;32mFiles:\033[0;0m'
$pycat fileselect
echo -e '\033[0;32mCategories:\033[0;0m'
$pycat categoryselect
echo -e '\033[0;32mFiles with properties:\033[0;0m'
$pycat fileselect --view props

# удаляем данные

$pycat pathrm 1 2
$pycat filerm 1 2 3
$pycat categoryrm 1 2 3 4 5 6

# проверяем, что всё чисто

$pycat pathselect
$pycat fileselect
$pycat categoryselect