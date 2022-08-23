#!/usr/bin/env bash
# -*- mode:text; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
# vim: ts=8 sw=2 smarttab

# /home/ronen-fr/.vscode-server/data/User/globalStorage/buenon.scratchpads/scratchpads/7475955ad6cf860243c5f86cedf1cae6/scratch2.txt

# test the handling of a corrupted SnapMapper DB by Scrub

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/scrub/scrub-helpers.sh

function run() {
  local dir=$1
  shift

  export CEPH_MON="127.0.0.1:7144" # git grep '\<7144\>' : there must be only one
  export CEPH_ARGS
  CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
  CEPH_ARGS+="--mon-host=$CEPH_MON "

  export -n CEPH_CLI_TEST_DUP_COMMAND
  local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
  for func in $funcs ; do
    setup $dir || return 1
    $func $dir || return 1
    teardown $dir || return 1
  done
}

# one clone & multiple snaps (according to the number of parameters)
function make_a_clone()
{
  #turn off '-x' (but remember previous state)
  local saved_echo_flag=${-//[^x]/}
  set -x
  local pool=$1
  local obj=$2
  echo $RANDOM | rados -p $poolname put $obj - || return 1

  shift 2
  for snap in $@ ; do
    rados -p $poolname mksnap $snap || return 1
  done
  if [[ -n "$saved_echo_flag" ]]; then set -x; fi
}

function TEST_truncated_sna_record() {
    local dir=$1
    local -A cluster_conf=(
        ['osds_num']="3" 
        ['pgs_in_pool']="4"
        ['pool_name']="test"
    )

    echo "RRR Dir: $dir"
    standard_scrub_cluster $dir cluster_conf
    ceph osd set noscrub || return 1
    ceph osd set nodeep-scrub || return 1
    sleep 3 # the 'noscrub' command takes a long time to reach the OSDs

    local osdn=${cluster_conf['osds_num']}
    local poolid=${cluster_conf['pool_id']}
    local poolname=${cluster_conf['pool_name']}
    local objname="objxxx"
    # create an object and clone it
    make_a_clone $poolname $objname snap01 snap02 || return 1
    make_a_clone $poolname $objname snap13 || return 1
    make_a_clone $poolname $objname snap24 snap25 || return 1
    echo $RANDOM | rados -p $poolname put $objname - || return 1

    echo "CEPH_OSD_ARGS: $CEPH_OSD_ARGS"
    echo "CEPH_ARGS: $CEPH_ARGS"

    #identify the PG and the primary OSD
    local pgid=`ceph --format=json-pretty osd map $poolname $objname | jq -r '.pgid'`
    local osd=`ceph --format=json-pretty osd map $poolname $objname | jq -r '.up[0]'`
    echo "pgid is $pgid (primary: osd.$osd)"
    # turn on the publishing of test data in the 'scrubber' section of 'pg query' output
    set_query_debug $pgid

    # verify the existence of these clones
    rados --format json-pretty -p $poolname listsnaps $objname

    # scrub the PG (todo - use lines 481-.. from qa/standalone/scrub/osd-scrub-test.sh)
    ceph osd unset noscrub || return 1
    ceph osd unset nodeep-scrub || return 1
    sleep 4
    ceph pg $pgid deep_scrub || return 1

    # wait for the scrub to finish
    # fix
    sleep 4
    ceph pg dump pgs
    ceph osd set noscrub || return 1
    ceph osd set nodeep-scrub || return 1
    sleep 3 # the 'noscrub' command takes a long time to reach the OSDs

    sleep 10
    grep -a -q -v "ERR" $dir/osd.$osd.log || return 1

    # kill the OSDs
    kill_daemons $dir TERM osd || return 1

    bin/ceph-kvstore-tool bluestore-kv $dir/2 dump p 2> /dev/null > /tmp/oo2.dump
    echo "RRR Dump: /tmp/oo2.dump"
    grep -a SNA_ /tmp/oo2.dump
    bin/ceph-kvstore-tool bluestore-kv $dir/1 dump p 2> /dev/null > /tmp/oo1.dump

    for sdn in $(seq 0 $(expr $osdn - 1))
    do
        kvdir=$dir/$sdn
        #kvdir=dev/osd.$sdn
        echo "corrupting the SnapMapper DB of osd.$sdn ($kvdir)"
        bin/ceph-kvstore-tool bluestore-kv $kvdir dump p 2> /dev/null >> /tmp/oooo$sdn.dump

        # truncate the 'mapping' (SNA_) entry corresponding to the snap13 clone
        tmp_fn1=`mktemp -p /tmp --suffix="sna1"`
        echo "Temp file: $tmp_fn1"
        #bin/ceph-kvstore-tool bluestore-kv $kvdir dump p 2> /dev/null | cat -v
        echo " RRRRRRRRRRRRRRRRRRRRRRRRRR "
        #bin/ceph-kvstore-tool bluestore-kv $kvdir dump p  | grep -a 'SNA_2_0000000000000003_000000000000000' | cat -v
        SN=`bin/ceph-kvstore-tool bluestore-kv $kvdir dump p 2> /dev/null | grep -a -e 'SNA_[0-9]_0000000000000003_000000000000000' | awk -e '{print $2;}'`
        echo "SNA key: $SN"
        KY="${SN:0:-3}"

        tmp_fn2=`mktemp -p /tmp --suffix="_the_val"`
        echo "Temp file: $tmp_fn2"
        bin/ceph-kvstore-tool bluestore-kv $kvdir get p $KY out $tmp_fn2 2> /dev/null
        od -xc $tmp_fn2

        NKY=${KY:0:-30}
        bin/ceph-kvstore-tool bluestore-kv $kvdir rm p "$KY" 2> /dev/null
        bin/ceph-kvstore-tool bluestore-kv $kvdir set p "$NKY" in $tmp_fn2 2> /dev/null
    done

    #restart_scrub_osds $dir cluster_conf
    orig_osd_args=" ${cluster_conf['osd_args']}"
    orig_osd_args=" $(echo $orig_osd_args)"

    echo "CEPH_OSD_ARGS: $CEPH_OSD_ARGS"
    echo "CEPH_ARGS: $CEPH_ARGS"

    echo "Copied OSD args: /$orig_osd_args/ /${orig_osd_args:1}/"
    echo "RRR Dir: $dir"
    for sdn in $(seq 0 $(expr $osdn - 1))
    do
      CEPH_ARGS="$CEPH_ARGS $orig_osd_args" activate_osd $dir $sdn
    done
    sleep 3
    for sdn in $(seq 0 $(expr $osdn - 1))
    do
      timeout 60 ceph tell osd.$sdn version
    done

    # when scrubbing now - we expect the scrub to emit a cluster log ERR message regarding SnapMapper internal inconsistency
    ceph osd unset noscrub || return 1
    ceph osd unset nodeep-scrub || return 1
    ceph pg dump pgs
    sleep 2
    ceph pg $pgid deep_scrub || return 1
    sleep 5
    ceph pg dump pgs
    grep -a "ERR" $dir/osd.$osd.log
    grep -a -q "ERR" $dir/osd.$osd.log || return 1


# 
# 
# 
#     objectstore_tool $dir $osd --op list $objname > /tmp/outO1
#     echo "Json 89: " $JSON
#     JSON=`objectstore_tool $dir $osd --op list $objname | grep snapid.:3`
#     echo "Json 91: " $JSON
#     objectstore_tool $dir $osd "$JSON" get-attr _ > $dir/atr1 || return 1
#     objectstore_tool $dir $osd "$JSON" get-bytes $dir/data1 || return 1
# 
# 
# 
#     # kill the OSDs
#     #kill_daemons $dir TERM osd || return 1
# 
#     #truncate the 'mapping' (SNA_) entry corresponding to the snap13 clone
#     ceph-kvstore-tool bluestore-kv $dir/${osd} list 2> /dev/null > $dir/drk.log
# 
#     grep -a SNA_ /tmp/drk.log | cat -v
#     grep -a SNA_ /tmp/drk.log > /tmp/drk2.log
# 

}



function notest_recover_unexpected() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1

    ceph osd pool create foo 1
    rados -p foo put foo /etc/passwd
    rados -p foo mksnap snap
    rados -p foo put foo /etc/group

    wait_for_clean || return 1

    local osd=$(get_primary foo foo)

    JSON=`objectstore_tool $dir $osd --op list foo | grep snapid.:1`
    echo "JSON is $JSON"
    rm -f $dir/_ $dir/data
    objectstore_tool $dir $osd "$JSON" get-attr _ > $dir/_ || return 1
    objectstore_tool $dir $osd "$JSON" get-bytes $dir/data || return 1

    rados -p foo rmsnap snap

    sleep 5

    objectstore_tool $dir $osd "$JSON" set-bytes $dir/data || return 1
    objectstore_tool $dir $osd "$JSON" set-attr _ $dir/_ || return 1

    sleep 5

    ceph pg repair 1.0 || return 1

    sleep 10

    ceph log last

    # make sure osds are still up
    timeout 60 ceph tell osd.0 version || return 1
    timeout 60 ceph tell osd.1 version || return 1
    timeout 60 ceph tell osd.2 version || return 1
}


main osd-mapper "$@"
