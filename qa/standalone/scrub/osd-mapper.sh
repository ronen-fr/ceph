#!/usr/bin/env bash
# -*- mode:text; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
# vim: ts=8 sw=2 smarttab
#
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
  set +x
  local pool=$1
  local obj=$2
  echo $RANDOM | rados -p $pool put $obj - || return 1
  shift 2
  for snap in $@ ; do
    rados -p $pool mksnap $snap || return 1
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

    local extr_dbg=1
    (( extr_dbg > 1 )) && echo "Dir: $dir"
    standard_scrub_cluster $dir cluster_conf
    ceph tell osd.* config set osd_stats_update_period_not_scrubbing "1"
    ceph tell osd.* config set osd_stats_update_period_scrubbing "1"
    #ceph osd set noscrub || return 1
    #ceph osd set nodeep-scrub || return 1
    #sleep 3 # the 'noscrub' command takes a long time to reach the OSDs

    local osdn=${cluster_conf['osds_num']}
    local poolid=${cluster_conf['pool_id']}
    echo "Pool id: $poolid"
    local poolname=${cluster_conf['pool_name']}
    echo "Pool name: $poolname"
    local objname="objxxx"
    # create an object and clone it
    make_a_clone $poolname $objname snap01 snap02 || return 1
    make_a_clone $poolname $objname snap13 || return 1
    make_a_clone $poolname $objname snap24 snap25 || return 1
    echo $RANDOM | rados -p $poolname put $objname - || return 1

    #identify the PG and the primary OSD
    local pgid=`ceph --format=json-pretty osd map $poolname $objname | jq -r '.pgid'`
    local osd=`ceph --format=json-pretty osd map $poolname $objname | jq -r '.up[0]'`
    echo "pgid is $pgid (primary: osd.$osd)"
    # turn on the publishing of test data in the 'scrubber' section of 'pg query' output
    set_query_debug $pgid

    # verify the existence of these clones
    (( extr_dbg >= 1 )) && rados --format json-pretty -p $poolname listsnaps $objname

    # scrub the PG (todo - use lines 481-.. from qa/standalone/scrub/osd-scrub-test.sh)
    #ceph osd unset nodeep-scrub || return 1
    #ceph osd unset noscrub || return 1
    #sleep 4
    #ceph pg $pgid deep_scrub || return 1
    pg_deep_scrub $pgid || return 1

    # wait for the scrub to finish
    # fix
    #sleep 4
    ceph pg dump pgs
    ceph osd set noscrub || return 1
    ceph osd set nodeep-scrub || return 1

    sleep 5 # wait for the log to be flushed (and for the no-scrub to reach OSDs)
    grep -a -q -v "ERR" $dir/osd.$osd.log || return 1

    # kill the OSDs
    kill_daemons $dir TERM osd || return 1

    (( extr_dbg >= 2 )) && bin/ceph-kvstore-tool bluestore-kv $dir/2 dump p 2> /dev/null > /tmp/oo2.dump
    (( extr_dbg >= 2 )) && grep -a SNA_ /tmp/oo2.dump
    (( extr_dbg >= 2 )) && bin/ceph-kvstore-tool bluestore-kv $dir/1 dump p 2> /dev/null > /tmp/oo1.dump

    for sdn in $(seq 0 $(expr $osdn - 1))
    do
        kvdir=$dir/$sdn
        echo "corrupting the SnapMapper DB of osd.$sdn (db: $kvdir)"
        (( extr_dbg >= 3 )) && bin/ceph-kvstore-tool bluestore-kv $kvdir dump p 2> /dev/null >> /tmp/oooo$sdn.dump

        # truncate the 'mapping' (SNA_) entry corresponding to the snap13 clone
        SN=`bin/ceph-kvstore-tool bluestore-kv $kvdir dump p 2> /dev/null | grep -a -e 'SNA_[0-9]_0000000000000003_000000000000000' \
            | awk -e '{print $2;}'`
        KY="${SN:0:-3}"
        (( extr_dbg >= 1 )) && echo "SNA key: $KY"

        tmp_fn1=`mktemp -p /tmp --suffix="_the_val"`
        (( extr_dbg >= 1 )) && echo "Value dumped in: $tmp_fn1"
        bin/ceph-kvstore-tool bluestore-kv $kvdir get p $KY out $tmp_fn1 2> /dev/null
        (( extr_dbg >= 2 )) && od -xc $tmp_fn1

        NKY=${KY:0:-30}
        bin/ceph-kvstore-tool bluestore-kv $kvdir rm p "$KY" 2> /dev/null
        bin/ceph-kvstore-tool bluestore-kv $kvdir set p "$NKY" in $tmp_fn1 2> /dev/null

        (( extr_dbg >= 1 )) || rm $tmp_fn1
    done

    orig_osd_args=" ${cluster_conf['osd_args']}"
    orig_osd_args=" $(echo $orig_osd_args)"
    (( extr_dbg >= 2 )) && echo "Copied OSD args: /$orig_osd_args/ /${orig_osd_args:1}/"
    for sdn in $(seq 0 $(expr $osdn - 1))
    do
      CEPH_ARGS="$CEPH_ARGS $orig_osd_args" activate_osd $dir $sdn
    done
    sleep 1

    for sdn in $(seq 0 $(expr $osdn - 1))
    do
      timeout 60 ceph tell osd.$sdn version
    done

    # when scrubbing now - we expect the scrub to emit a cluster log ERR message regarding SnapMapper internal inconsistency
    ceph osd unset nodeep-scrub || return 1
    ceph osd unset noscrub || return 1

    # what is the primary now?
    local cur_prim=`ceph --format=json-pretty osd map $poolname $objname | jq -r '.up[0]'`
    ceph pg dump pgs
    sleep 2
    ceph pg $pgid deep_scrub || return 1
    sleep 5
    ceph pg dump pgs
    (( extr_dbg >= 1 )) && grep -a "ERR" $dir/osd.$cur_prim.log
    grep -a -q "ERR" $dir/osd.$cur_prim.log || return 1
}



main osd-mapper "$@"
