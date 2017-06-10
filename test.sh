#!/usr/bin/env bash

do_test () {
	echo "$3 $1"
	echo -e "\033[0;32m------- Output is true\n$2\033[0;0m"
	r=`$1`

    #if [ $3 == 59 ]
    #then
    #    echo "`echo -e \"$2\"`" > c_true
    #    echo "$r" > c_res
    #fi

	if [ "$r" != "`echo -e \"$2\"`" ]
    then
	    echo -e "\033[0;31m------- Output is false\n$r\033[0;0m"
	    exit
	fi
}

mkdir /tmp/dir1 /tmp/dir2 /tmp/dir3

echo 'text1' > /tmp/dir1/file1
echo 'text2' > /tmp/dir1/file2
echo 'text3' > /tmp/dir1/file3

# проверяем, что всё чисто
echo -e '\n\033[0;33mIs database clear?:\033[0;0m\n'

do_test 'pycat pathselect'       ""   $LINENO
do_test 'pycat fileselect'       ""   $LINENO
do_test 'pycat categoryselect'   ""   $LINENO

# Тестируем Paths
echo -e '\n\033[0;33mTest paths:\033[0;0m\n'

do_test 'pycat pathadd /tmp/dir1'   "1"             $LINENO
do_test 'pycat pathselect'          "1 /tmp/dir1"   $LINENO
do_test 'pycat pathadd /tmp/dir2'   "2"             $LINENO

do_test 'pycat pathselect'                    "1 /tmp/dir1\n2 /tmp/dir2"    $LINENO
do_test 'pycat pathselect --path_id 2'        "2 /tmp/dir2"                 $LINENO
do_test 'pycat pathupdate 2 path /tmp/dir3'   ""                            $LINENO
do_test 'pycat pathselect --path_id 2'        "2 /tmp/dir3"                 $LINENO

do_test 'pycat pathadd /tmp/dir2'   "3"                                     $LINENO
do_test 'pycat pathrm 2'            ""                                      $LINENO
do_test 'pycat pathselect'          "1 /tmp/dir1\n3 /tmp/dir2"              $LINENO
do_test 'pycat pathrm 1 3'          ""                                      $LINENO
do_test 'pycat pathselect'          ""   $LINENO

# Тестируем Categories
echo -e '\n\033[0;33mTest categories:\033[0;0m\n'

do_test "pycat categoryadd MyCategory1 MyCategory2 MyCategory3"   "1 2 3 \n"   $LINENO
do_test 'pycat categoryadd --parent 2 MyCategory4 MyCategory5'    "4 5 \n"     $LINENO
do_test 'pycat categoryadd --parent 5 MyCategory6'                 "6 \n"      $LINENO

do_test 'pycat categoryselect' "MyCategory1 (1)
MyCategory2 (2)
    MyCategory4 (4)
    MyCategory5 (5)
        MyCategory6 (6)
MyCategory3 (3)" $LINENO

do_test 'pycat categoryupdate 5 category_name MycategoryUpdated5'   ""    $LINENO
do_test 'pycat categoryselect' "MyCategory1 (1)
MyCategory2 (2)
    MyCategory4 (4)
    MycategoryUpdated5 (5)
        MyCategory6 (6)
MyCategory3 (3)" $LINENO

do_test 'pycat categoryrm 6'   ""    $LINENO
do_test 'pycat categoryselect' "MyCategory1 (1)
MyCategory2 (2)
    MyCategory4 (4)
    MycategoryUpdated5 (5)
MyCategory3 (3)" $LINENO
do_test 'pycat categoryrm 1 3 4 5 2'   ""    $LINENO
do_test 'pycat categoryselect' "" $LINENO

# Тестируем Files
echo -e '\n\033[0;33mTest files:\033[0;0m\n'

a=`pycat pathadd /tmp/dir1`

do_test 'pycat fileadd MyFile1 /tmp/dir1/file1'          "1"      $LINENO
do_test 'pycat fileadd MyFile2 /tmp/dir1/file2'          "2"      $LINENO

do_test 'pycat fileselect' "1 MyFile1
2 MyFile2" $LINENO

do_test 'pycat filerm 1 2' "" $LINENO
do_test 'pycat fileselect' "" $LINENO

a=`pycat pathrm 1`