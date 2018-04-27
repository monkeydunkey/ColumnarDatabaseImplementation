#!/usr/bin/env bash

# How to run this program
# Execute this from javaminibase/build folder:
#
# cd javaminibase/build
# export JVM_HOME_LOCATION={{ YOUR PATH HERE }}
# ./build.sh
#
# cd ../src
# make db
#

JVM_HOME_LOCATION="/usr/lib/jvm/java-8-oracle"

ensure_vars_are_set_or_exit(){
# exit the script if someone forgets to set the JVM_HOME_LOCATION VAR

if [ -z ${JVM_HOME_LOCATION+x} ];
then echo "var is unset" && exit 1;
else echo "JDKPATH is set to '$JVM_HOME_LOCATION'";
fi
}


#copy all makes files over
copy_all_makefiles(){
echo "copying all make files"

cp ./makefiles/Makefile ../src
cp ./makefiles/btree/Makefile ../src/btree
cp ./makefiles/chainexception/Makefile ../src/chainexception
cp ./makefiles/bufmgr/Makefile ../src/bufmgr
cp ./makefiles/bitmap/Makefile ../src/bitmap
cp ./makefiles/catalog/Makefile ../src/catalog
cp ./makefiles/columnar/Makefile ../src/columnar
cp ./makefiles/diskmgr/Makefile ../src/diskmgr
cp ./makefiles/global/Makefile ../src/global
cp ./makefiles/heap/Makefile ../src/heap
cp ./makefiles/index/Makefile ../src/index
cp ./makefiles/iterator/Makefile ../src/iterator
cp ./makefiles/tests/Makefile ../src/tests
}

#update jvm location in all files
update_jvm_location_in_make_files(){
echo "updating JDKPATH in all Make files"

find ../src -name 'Makefile' -print0 |
xargs -0 sed -i -E "s@^JDKPATH.+\$@JDKPATH=$JVM_HOME_LOCATION@g"
}

# clean up Make files from src directory:
# find . -name 'Makefile' -delete
# from src

ensure_vars_are_set_or_exit
copy_all_makefiles
update_jvm_location_in_make_files
echo "complete"

