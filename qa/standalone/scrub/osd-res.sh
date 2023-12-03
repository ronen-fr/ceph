#!/usr/bin/env bash
# -*- mode:text; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
# vim: ts=8 sw=2 smarttab
#
# test the handling of a corrupted SnapMapper DB by Scrub

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/scrub/scrub-helpers.sh


# measuring the time it would take for all PGs in an EC cluster to perform two
# scrubs. We are testing the effect of various replica reservation techniques
# on the:
#  number of aborted attempts at securing the replicas;
#  total time to complete the scrubs;
#  total time spent in the reservation stage;
#  'inequality', defined as the number of PGs that start their second scrub before
#  all PGs have completed the first round.

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

function initial_pool_fill()
{
  local dir=$1
  local pool=$2
  local OBJS=1000


  dd if=/dev/urandom of=$dir/datafile bs=1k count=1
  for j in $(seq 1 $OBJS)
  do
    rados -p $pool put obj$j $dir/datafile || return 1
  done
}

function collect_log()
{


}


function TEST_two_rounds()
{
  local dir=$1
  local -A cluster_conf=(
      ['osds_num']="6"
      ['pgs_in_pool']="12"
      ['pool_name']="test"
  )

  local extr_dbg=3
  (( extr_dbg > 1 )) && echo "Dir: $dir"
  ec_scrub_cluster $dir cluster_conf

  # also set the chunk sizes
  # set max scrubs

  # write some data
  initial_pool_fill $dir ${cluster_conf['pool_name']}

  # start scrubbing
  ceph set osd.* osd_deep_scrub_interval "1.0"


  # wait to see enough scrubs terminated in the cluster log

  sleep 100
}

main scrub-reservations "$@"
