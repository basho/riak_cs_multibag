#!/bin/bash

# just bail out if things go south
set -e

# You need to use this script once to build a set of devrels for prior
# releases of Riak CS (for mixed version / upgrade testing). You should
# create a directory and then run this script from within that directory.
# I have ~/test-releases that I created once, and then re-use for testing.
#
# See rtdev-setup-releases.sh as an example of setting up mixed version layout
# for testing.

# Different versions of Riak were released using different Erlang versions,
# make sure to build with the appropriate version.

# This script builds released package(s) and one additional devrel's
# from git, patched variant of 1.5.4. This is because some riak_test
# cases require riak_cs_multibag's bug fixes after 1.5.4 but 1.5.5 is
# not yet released.  This script builds devrel's based on the branches
# 'release/1.5' of riak_cs and riak_cs_multibag at 2015-03-25.
#
# cf: Corresponding PRs
# - Refactor/multibag simpler state transition
#   https://github.com/basho/riak_cs_multibag/pull/21
# - Refactor/multibag simpler state transition
#    https://github.com/basho/riak_cs/pull/1080

R15B01=${R15B01:-$HOME/erlang/R15B01-64}
R16B02=${R16B02:-$HOME/erlang/R16B02-64}
: ${RTCS_DEST_DIR:="$HOME/rt/riak_cs"}

checkbuild()
{
    ERLROOT=$1

    if [ ! -d $ERLROOT ]; then
        echo -n "$ERLROOT cannot be found, install kerl? [y|N]: "
        read ans
        if [[ $ans == n || $ans == N ]]; then
            exit 1
        fi
    fi
}

kerl()
{
    RELEASE=$1
    BUILDNAME=$2

    if [ ! -x kerl ]; then
        curl -O https://raw.github.com/spawngrid/kerl/master/kerl; chmod a+x kerl
    fi

    ./kerl build $RELEASE $BUILDNAME
    ./kerl install $BUILDNAME $HOME/$BUILDNAME
}

build()
{
    SRCDIR=$1
    ERLROOT=$2

    if [ ! -d $ERLROOT ]; then
        BUILDNAME=`basename $ERLROOT`
        RELEASE=`echo $BUILDNAME | awk -F- '{ print $2 }'`
        kerl $RELEASE $BUILDNAME
    fi

    echo
    echo "Building $SRCDIR"
    cd $SRCDIR

    RUN="env PATH=$ERLROOT/bin:$ERLROOT/lib/erlang/bin:$PATH \
             C_INCLUDE_PATH=$ERLROOT/usr/include \
             LD_LIBRARY_PATH=$ERLROOT/usr/lib \
             DEVNODES=8"
    echo $RUN
    $RUN make -j 8 && $RUN make -j 8 devrel
    cd ..
}

setup()
{
    SRCDIR=$1
    cd $SRCDIR
    VERSION=$SRCDIR
    echo " - Copying devrel to $RTCS_DEST_DIR/$VERSION "
    mkdir -p $RTCS_DEST_DIR/$VERSION/
    cp -p -P -R dev $RTCS_DEST_DIR/$VERSION/
    ## echo " - Writing $RTCS_DEST_DIR/$VERSION/VERSION"
    ## echo -n $VERSION > $RTCS_DEST_DIR/$VERSION/VERSION
    cd $RTCS_DEST_DIR
    echo " - Adding $VERSION to git state of $RTCS_DEST_DIR"
    git add $VERSION
    git commit -a -m "riak_test adding version $VERSION" ## > /dev/null 2>&1
}

download()
{
  URI=$1
  FILENAME=`echo $URI | awk -F/ '{ print $8 }'`
  if [ ! -f $FILENAME ]; then
    wget $URI
  fi
}

checkbuild $R15B01
checkbuild $R16B02

if ! env | grep -q 'RIAK_CS_EE_DEPS='
then
    echo "RIAK_CS_EE_DEPS is not set"
    echo "This script if for EE version."
    echo "set RIAK_CS_EE_DEPS or use script for oss build."
    exit 1
fi

echo "RIAK_CS_EE_DEPS is set to \"$RIAK_CS_EE_DEPS\"."
echo "Download and build ee package..."

## *** CAUTION ***
## This URL is public readable but *hide* only by URL, then
## should not be written public readable repo, gist, etc.
download http://private.downloads.basho.com/riak-cs-ee/164aef/1.5/1.5.4/riak-cs-1.5.4.tar.gz

tar -xf riak-cs-1.5.4.tar.gz
build "riak-cs-1.5.4" $R15B01

## Special build for unreleased version from git, 1.5.4 patch 1
rm -rf riak-cs-1.5.4p1
git clone git@github.com:basho/riak_cs.git riak-cs-1.5.4p1
cd riak-cs-1.5.4p1
git checkout 751485d7eeae450233219575e831b890c93acd5e
make deps
(cd deps/riak_cs_multibag; git checkout 9d5647550980ebe2e0d03ced423e803fc7f9743c)
cd ..

build "riak-cs-1.5.4p1" $R15B01
