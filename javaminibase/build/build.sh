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

#JVM_HOME_LOCATION="/usr/lib/jvm/java-99-oracle"

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

cp ./makefiles/Makefile ../src/main/java
cp ./makefiles/btree/Makefile ../src/main/java/btree
cp ./makefiles/chainexception/Makefile ../src/main/java/chainexception
cp ./makefiles/bufmgr/Makefile ../src/main/java/bufmgr
cp ./makefiles/bitmap/Makefile ../src/main/java/bitmap
cp ./makefiles/catalog/Makefile ../src/main/java/catalog
cp ./makefiles/columnar/Makefile ../src/main/java/columnar
cp ./makefiles/diskmgr/Makefile ../src/main/java/diskmgr
cp ./makefiles/global/Makefile ../src/main/java/global
cp ./makefiles/heap/Makefile ../src/main/java/heap
cp ./makefiles/index/Makefile ../src/main/java/index
cp ./makefiles/iterator/Makefile ../src/main/java/iterator
cp ./makefiles/tests/Makefile ../src/main/java/tests
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