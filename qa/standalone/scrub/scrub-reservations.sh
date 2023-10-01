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
  #local OBJS=10000
  local OBJS=500
  local saved_echo_flag=${-//[^x]/}
  set +x

  #dd if=/dev/urandom of=$dir/datafile bs=4k count=8
  dd if=/dev/urandom of=$dir/datafile bs=2k count=1
  for j in $(seq 1 $OBJS)
  do
    rados -p $pool put obj$j $dir/datafile || return 1
  done
  if [[ -n "$saved_echo_flag" ]]; then set -x; fi
}

function collect_log()
{
  # collect the log
  echo collect logs
}

# function handle_1pg_scrubbing()
# {
#   local dir=$1
#   local pool=$2
#   local pg=$3
#   local times=$5
#   local saved_echo_flag=${-//[^x]/}
#   set -x
#
#
#   if [[ -n "$saved_echo_flag" ]]; then set -x; fi
# }

function TEST_ec_pool_reserv()
{
  local dir=$1
  local -A cluster_conf=(
      ['osds_num']="7"
      ['pgs_in_pool']="16"
      ['pool_name']="ec_test"
      ['erasure_k']="4"
      ['erasure_m']="2"
      ['extras']=" --osd_op_queue=wpq --osd_scrub_sleep=0.9"
  )

  local extr_dbg=3
  (( extr_dbg > 1 )) && echo "Dir: $dir"
  ec_scrub_cluster $dir cluster_conf

  sleep 4

  # write some data
  initial_pool_fill $dir ${cluster_conf['pool_name']}
  ceph tell osd.* config set osd_max_scrubs 2
  ceph tell osd.* config set debug_osd 20/20
  ceph tell osd.* config set osd_scrub_chunk_max 4
  ceph tell osd.* config set osd_scrub_chunk_min 2
  sleep 3

  # start scrubbing
  pwd
  ceph pg dump pgs >> /tmp/pgs${pool_id}.json
  ceph pg dump pgs
  bin/ceph pg ls-by-osd 1

  # wait to see enough scrubs terminated in the cluster log
  #/home/rfriedma/pgs.py
  rounds=3
  ../qa/standalone/scrub/multi_scrubs.py --repeat $rounds --pool-type ec --outlog ev_logZ.json
  ceph pg dump pgs

  # currently I am having problems with different TZ in both files
  sed 's/T08/T03/g' < ev_logZ.json > ev_log.json
  ls ./td/scrub-reservations
  ../qa/standalone/scrub/collect_scrub_events.py --title "ec-4-2/$rounds" --logs-dir ./td/scrub-reservations \
    --csv /tmp/pgs${pool_id} --prev ev_log.json
  #../qa/standalone/scrub/collect_scrub_events.py --title "ec-4-2/3" --logs-dir ./td/scrub-reservations  --csv /tmp/pgs${pool_id}.csv --prev ev_log.json
}

function TEST_rep_pool_reserv()
{
  local dir=$1
  local -A cluster_conf=(
      ['osds_num']="6"
      ['pgs_in_pool']="16"
      ['pool_name']="rep_test"
      ['extras']=" --osd_op_queue=wpq --osd_scrub_sleep=0.9"
  )

  local extr_dbg=3
  (( extr_dbg > 1 )) && echo "Dir: $dir"
  standard_scrub_cluster $dir cluster_conf

  sleep 4

  # write some data
  initial_pool_fill $dir ${cluster_conf['pool_name']}
  ceph tell osd.* config set osd_max_scrubs 2
  ceph tell osd.* config set debug_osd 20/20
  ceph tell osd.* config set osd_scrub_chunk_max 4
  ceph tell osd.* config set osd_scrub_chunk_min 2
  sleep 3

  # start scrubbing
  pwd
  ceph pg dump pgs >> /tmp/pgs${pool_id}.json
  ceph pg dump pgs
  bin/ceph pg ls-by-osd 1

  # wait to see enough scrubs terminated in the cluster log
  #/home/rfriedma/pgs.py
  rounds=3
  ../qa/standalone/scrub/multi_scrubs.py --repeat $rounds  --outlog rep_ev_logZ.json
  ceph pg dump pgs

  # currently I am having problems with different TZ in both files
  sed 's/T08/T03/g' < rep_ev_logZ.json > rep_ev_log.json
  ls ./td/scrub-reservations
  ../qa/standalone/scrub/collect_scrub_events.py --title "rep/$rounds" --logs-dir ./td/scrub-reservations \
    --csv /tmp/pgs${pool_id} --prev rep_ev_log.json
}

main scrub-reservations "$@"
