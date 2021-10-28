#!/usr/bin/env bash
#
# Copyright (C) 2018 Red Hat <contact@redhat.com>
#
# Author: David Zafman <dzafman@redhat.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#
source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7138" # git grep '\<7138\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    export -n CEPH_CLI_TEST_DUP_COMMAND
    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        $func $dir || return 1
    done
}

function TEST_scrub_test() {
    local dir=$1
    local poolname=test
    local OSDS=3
    local objects=15

    TESTDATA="testdata.$$"

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=3 || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd || return 1
    done

    # Create a pool with a single pg
    create_pool $poolname 1 1
    wait_for_clean || return 1
    poolid=$(ceph osd dump | grep "^pool.*[']${poolname}[']" | awk '{ print $2 }')

    dd if=/dev/urandom of=$TESTDATA bs=1032 count=1
    for i in `seq 1 $objects`
    do
        rados -p $poolname put obj${i} $TESTDATA
    done
    rm -f $TESTDATA

    local primary=$(get_primary $poolname obj1)
    local otherosd=$(get_not_primary $poolname obj1)
    if [ "$otherosd" = "2" ];
    then
      local anotherosd="0"
    else
      local anotherosd="2"
    fi

    objectstore_tool $dir $anotherosd obj1 set-bytes /etc/fstab

    local pgid="${poolid}.0"
    pg_deep_scrub "$pgid" || return 1

    ceph pg dump pgs | grep ^${pgid} | grep -q -- +inconsistent || return 1
    test "$(ceph pg $pgid query | jq '.info.stats.stat_sum.num_scrub_errors')" = "2" || return 1

    ceph osd out $primary
    wait_for_clean || return 1

    pg_deep_scrub "$pgid" || return 1

    test "$(ceph pg $pgid query | jq '.info.stats.stat_sum.num_scrub_errors')" = "2" || return 1
    test "$(ceph pg $pgid query | jq '.peer_info[0].stats.stat_sum.num_scrub_errors')" = "2" || return 1
    ceph pg dump pgs | grep ^${pgid} | grep -q -- +inconsistent || return 1

    ceph osd in $primary
    wait_for_clean || return 1

    repair "$pgid" || return 1
    wait_for_clean || return 1

    # This sets up the test after we've repaired with previous primary has old value
    test "$(ceph pg $pgid query | jq '.peer_info[0].stats.stat_sum.num_scrub_errors')" = "2" || return 1
    ceph pg dump pgs | grep ^${pgid} | grep -vq -- +inconsistent || return 1

    ceph osd out $primary
    wait_for_clean || return 1

    test "$(ceph pg $pgid query | jq '.info.stats.stat_sum.num_scrub_errors')" = "0" || return 1
    test "$(ceph pg $pgid query | jq '.peer_info[0].stats.stat_sum.num_scrub_errors')" = "0" || return 1
    test "$(ceph pg $pgid query | jq '.peer_info[1].stats.stat_sum.num_scrub_errors')" = "0" || return 1
    ceph pg dump pgs | grep ^${pgid} | grep -vq -- +inconsistent || return 1

    teardown $dir || return 1
}

# Grab year-month-day
DATESED="s/\([0-9]*-[0-9]*-[0-9]*\).*/\1/"
DATEFORMAT="%Y-%m-%d"

function check_dump_scrubs() {
    local primary=$1
    local sched_time_check="$2"
    local deadline_check="$3"

    DS="$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) dump_scrubs)"
    # use eval to drop double-quotes
    eval SCHED_TIME=$(echo $DS | jq '.[0].sched_time')
    test $(echo $SCHED_TIME | sed $DATESED) = $(date +${DATEFORMAT} -d "now + $sched_time_check") || return 1
    # use eval to drop double-quotes
    eval DEADLINE=$(echo $DS | jq '.[0].deadline')
    test $(echo $DEADLINE | sed $DATESED) = $(date +${DATEFORMAT} -d "now + $deadline_check") || return 1
}

function TEST_interval_changes() {
    local poolname=test
    local OSDS=2
    local objects=10
    # Don't assume how internal defaults are set
    local day="$(expr 24 \* 60 \* 60)"
    local week="$(expr $day \* 7)"
    local min_interval=$day
    local max_interval=$week
    local WAIT_FOR_UPDATE=15

    TESTDATA="testdata.$$"

    setup $dir || return 1
    # This min scrub interval results in 30 seconds backoff time
    run_mon $dir a --osd_pool_default_size=$OSDS || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd --osd_scrub_min_interval=$min_interval --osd_scrub_max_interval=$max_interval --osd_scrub_interval_randomize_ratio=0 || return 1
    done

    # Create a pool with a single pg
    create_pool $poolname 1 1
    wait_for_clean || return 1
    local poolid=$(ceph osd dump | grep "^pool.*[']${poolname}[']" | awk '{ print $2 }')

    dd if=/dev/urandom of=$TESTDATA bs=1032 count=1
    for i in `seq 1 $objects`
    do
        rados -p $poolname put obj${i} $TESTDATA
    done
    rm -f $TESTDATA

    local primary=$(get_primary $poolname obj1)

    # Check initial settings from above (min 1 day, min 1 week)
    check_dump_scrubs $primary "1 day" "1 week" || return 1

    # Change global osd_scrub_min_interval to 2 days
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) config set osd_scrub_min_interval $(expr $day \* 2)
    sleep $WAIT_FOR_UPDATE
    check_dump_scrubs $primary "2 days" "1 week" || return 1

    # Change global osd_scrub_max_interval to 2 weeks
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) config set osd_scrub_max_interval $(expr $week \* 2)
    sleep $WAIT_FOR_UPDATE
    check_dump_scrubs $primary "2 days" "2 week" || return 1

    # Change pool osd_scrub_min_interval to 3 days
    ceph osd pool set $poolname scrub_min_interval $(expr $day \* 3)
    sleep $WAIT_FOR_UPDATE
    check_dump_scrubs $primary "3 days" "2 week" || return 1

    # Change pool osd_scrub_max_interval to 3 weeks
    ceph osd pool set $poolname scrub_max_interval $(expr $week \* 3)
    sleep $WAIT_FOR_UPDATE
    check_dump_scrubs $primary "3 days" "3 week" || return 1

    teardown $dir || return 1
}

function TEST_scrub_extended_sleep() {
    local dir=$1
    local poolname=test
    local OSDS=3
    local objects=15

    TESTDATA="testdata.$$"

    DAY=$(date +%w)
    # Handle wrap
    if [ "$DAY" -ge "4" ];
    then
      DAY="0"
    fi
    # Start after 2 days in case we are near midnight
    DAY_START=$(expr $DAY + 2)
    DAY_END=$(expr $DAY + 3)

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=3 || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd --osd_scrub_sleep=0 \
                        --osd_scrub_extended_sleep=20 \
                        --bluestore_cache_autotune=false \
	                --osd_deep_scrub_randomize_ratio=0.0 \
	                --osd_scrub_interval_randomize_ratio=0 \
			--osd_scrub_begin_week_day=$DAY_START \
			--osd_scrub_end_week_day=$DAY_END \
			|| return 1
    done

    # Create a pool with a single pg
    create_pool $poolname 1 1
    wait_for_clean || return 1

    # Trigger a scrub on a PG
    local pgid=$(get_pg $poolname SOMETHING)
    local primary=$(get_primary $poolname SOMETHING)
    local last_scrub=$(get_last_scrub_stamp $pgid)
    ceph tell $pgid scrub || return 1

    # Allow scrub to start extended sleep
    PASSED="false"
    for ((i=0; i < 15; i++)); do
      if grep -q "scrub state.*, sleeping" $dir/osd.${primary}.log
      then
	PASSED="true"
        break
      fi
      sleep 1
    done

    # Check that extended sleep was triggered
    if [ $PASSED = "false" ];
    then
      return 1
    fi

    # release scrub to run after extended sleep finishes
    ceph tell osd.$primary config set osd_scrub_begin_week_day 0
    ceph tell osd.$primary config set osd_scrub_end_week_day 0

    # Due to extended sleep, the scrub should not be done within 20 seconds
    # but test up to 10 seconds and make sure it happens by 25 seconds.
    count=0
    PASSED="false"
    for ((i=0; i < 25; i++)); do
	count=$(expr $count + 1)
        if test "$(get_last_scrub_stamp $pgid)" '>' "$last_scrub" ; then
	    # Did scrub run too soon?
	    if [ $count -lt "10" ];
	    then
              return 1
            fi
	    PASSED="true"
	    break
        fi
        sleep 1
    done

    # Make sure scrub eventually ran
    if [ $PASSED = "false" ];
    then
      return 1
    fi

    teardown $dir || return 1
}

function _scrub_abort() {
    local dir=$1
    local poolname=test
    local OSDS=3
    local objects=1000
    local type=$2

    TESTDATA="testdata.$$"
    if test $type = "scrub";
    then
      stopscrub="noscrub"
      check="noscrub"
    else
      stopscrub="nodeep-scrub"
      check="nodeep_scrub"
    fi


    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=3 || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
        # Set scheduler to "wpq" until there's a reliable way to query scrub
        # states with "--osd-scrub-sleep" set to 0. The "mclock_scheduler"
        # overrides the scrub sleep to 0 and as a result the checks in the
        # test fail.
        run_osd $dir $osd --osd_pool_default_pg_autoscale_mode=off \
            --osd_deep_scrub_randomize_ratio=0.0 \
            --osd_scrub_sleep=5.0 \
            --osd_scrub_interval_randomize_ratio=0 \
            --osd_op_queue=wpq || return 1
    done

    # Create a pool with a single pg
    create_pool $poolname 1 1
    wait_for_clean || return 1
    poolid=$(ceph osd dump | grep "^pool.*[']${poolname}[']" | awk '{ print $2 }')

    dd if=/dev/urandom of=$TESTDATA bs=1032 count=1
    for i in `seq 1 $objects`
    do
        rados -p $poolname put obj${i} $TESTDATA
    done
    rm -f $TESTDATA

    local primary=$(get_primary $poolname obj1)
    local pgid="${poolid}.0"

    ceph tell $pgid $type || return 1
    # deep-scrub won't start without scrub noticing
    if [ "$type" = "deep_scrub" ];
    then
      ceph tell $pgid scrub || return 1
    fi

    # Wait for scrubbing to start
    set -o pipefail
    found="no"
    for i in $(seq 0 200)
    do
      flush_pg_stats
      if ceph pg dump pgs | grep  ^$pgid| grep -q "scrubbing"
      then
        found="yes"
        #ceph pg dump pgs
        break
      fi
    done
    set +o pipefail

    if test $found = "no";
    then
      echo "Scrubbing never started"
      return 1
    fi

    ceph osd set $stopscrub
    if [ "$type" = "deep_scrub" ];
    then
      ceph osd set noscrub
    fi

    # Wait for scrubbing to end
    set -o pipefail
    for i in $(seq 0 200)
    do
      flush_pg_stats
      if ceph pg dump pgs | grep ^$pgid | grep -q "scrubbing"
      then
        continue
      fi
      #ceph pg dump pgs
      break
    done
    set +o pipefail

    sleep 5

    if ! grep "$check set, aborting" $dir/osd.${primary}.log
    then
      echo "Abort not seen in log"
      return 1
    fi

    local last_scrub=$(get_last_scrub_stamp $pgid)
    ceph config set osd "osd_scrub_sleep" "0.1"

    ceph osd unset $stopscrub
    if [ "$type" = "deep_scrub" ];
    then
      ceph osd unset noscrub
    fi
    TIMEOUT=$(($objects / 2))
    wait_for_scrub $pgid "$last_scrub" || return 1

    teardown $dir || return 1
}

function TEST_scrub_abort() {
    local dir=$1
    _scrub_abort $dir scrub
}

function TEST_deep_scrub_abort() {
    local dir=$1
    _scrub_abort $dir deep_scrub
}

function TEST_scrub_permit_time() {
    local dir=$1
    local poolname=test
    local OSDS=3
    local objects=15

    TESTDATA="testdata.$$"

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=3 || return 1
    run_mgr $dir x || return 1
    local scrub_begin_hour=$(date -d '2 hour ago' +"%H" | sed 's/^0//')
    local scrub_end_hour=$(date -d '1 hour ago' +"%H" | sed 's/^0//')
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd --bluestore_cache_autotune=false \
	                --osd_deep_scrub_randomize_ratio=0.0 \
	                --osd_scrub_interval_randomize_ratio=0 \
                        --osd_scrub_begin_hour=$scrub_begin_hour \
                        --osd_scrub_end_hour=$scrub_end_hour || return 1
    done

    # Create a pool with a single pg
    create_pool $poolname 1 1
    wait_for_clean || return 1

    # Trigger a scrub on a PG
    local pgid=$(get_pg $poolname SOMETHING)
    local primary=$(get_primary $poolname SOMETHING)
    local last_scrub=$(get_last_scrub_stamp $pgid)
    # If we don't specify an amount of time to subtract from
    # current time to set last_scrub_stamp, it sets the deadline
    # back by osd_max_interval which would cause the time permit checking
    # to be skipped.  Set back 1 day, the default scrub_min_interval.
    ceph tell $pgid scrub $(( 24 * 60 * 60 )) || return 1

    # Scrub should not run
    for ((i=0; i < 30; i++)); do
        if test "$(get_last_scrub_stamp $pgid)" '>' "$last_scrub" ; then
            return 1
        fi
        sleep 1
    done

    teardown $dir || return 1
}

function TEST_pg_dump_scrub_duration() {
    local dir=$1
    local poolname=test
    local OSDS=3
    local objects=15

    TESTDATA="testdata.$$"

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=$OSDS || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd || return 1
    done

    # Create a pool with a single pg
    create_pool $poolname 1 1
    wait_for_clean || return 1
    poolid=$(ceph osd dump | grep "^pool.*[']${poolname}[']" | awk '{ print $2 }')

    dd if=/dev/urandom of=$TESTDATA bs=1032 count=1
    for i in `seq 1 $objects`
    do
        rados -p $poolname put obj${i} $TESTDATA
    done
    rm -f $TESTDATA

    local pgid="${poolid}.0"
    pg_scrub $pgid || return 1

    ceph pg $pgid query | jq '.info.stats.scrub_duration'
    test "$(ceph pg $pgid query | jq '.info.stats.scrub_duration')" '>' "0" || return 1

    teardown $dir || return 1
}

# Use the output from both 'ceph pg dump pgs' and 'ceph pg x.x query' commands to determine
# the published scrub scheduling status of a given PG.
#
# arg 1: pg id
# arg 2: in/out dictionary
# arg 3: 'current' time to compare to
# arg 4: an additional time point to compare to
#
function extract_published_sch() {
  local pgn="$1"
  local -n dict=$4 # a ref to the in/out dictionary
  local current_time=$2
  local extra_time=$3
  local extr_dbg=1

  #turn off '-x' (but remember previous state)
  local saved_echo_flag=${-//[^x]/}
  set +x

  (( extr_dbg >= 3 )) && bin/ceph pg dump pgs -f json-pretty >> /tmp/a_dmp$$

  from_dmp=`bin/ceph pg dump pgs -f json-pretty | jq -r --arg pgn "$pgn" --arg extra_dt "$extra_time" --arg current_dt "$current_time" '[
    [[.pg_stats[]] | group_by(.pg_stats)][0][0] | 
    [.[] |
    select(has("pgid") and .pgid == $pgn) |

        (.dmp_stat_part=(.scrub_schedule | if test(".*@.*") then (split(" @ ")|first) else . end)) |
        (.dmp_when_part=(.scrub_schedule | if test(".*@.*") then (split(" @ ")|last) else "0" end)) |

     [ {
       dmp_pg_state: .state,
       dmp_state_has_scrubbing: (.state | test(".*scrub.*";"i")),
       dmp_last_duration:.last_scrub_duration,
       dmp_schedule: .dmp_stat_part,
       dmp_schedule_at: .dmp_when_part,
       dmp_future: ( .dmp_when_part > $current_dt ),
       dmp_vs_date: ( .dmp_when_part > $extra_dt  ),
       dmp_reported_epoch: .reported_epoch
      }] ]][][][]'`

  (( extr_dbg >= 2 )) && echo "from pg dump pg:"
  (( extr_dbg >= 2 )) && echo $from_dmp

  (( extr_dbg >= 2 )) && echo "query out==="
  (( extr_dbg >= 2 )) && bin/ceph pg $1 query -f json-pretty | awk -e '/scrubber/,/agent_state/ {print;}'

  (( extr_dbg >= 3 )) && bin/ceph pg $1 query -f json-pretty >> /tmp/a_qry$$

  from_qry=`bin/ceph pg $1 query -f json-pretty |  jq -r --arg extra_dt "$extra_time" --arg current_dt "$current_time"  --arg spt "'" '
    . |
        (.q_stat_part=((.scrubber.schedule// "-") | if test(".*@.*") then (split(" @ ")|first) else . end)) |
        (.q_when_part=((.scrubber.schedule// "0") | if test(".*@.*") then (split(" @ ")|last) else "0" end)) |
	(.q_when_is_future=(.q_when_part > $current_dt)) |
	(.q_vs_date=(.q_when_part > $extra_dt)) |	
      {
        query_epoch: .epoch,
        query_active: (.scrubber | if has("active") then .active else "boo" end),
        query_schedule: .q_stat_part,
        query_schedule_at: .q_when_part,
        query_last_duration: .info.stats.last_scrub_duration,
        query_last_stamp: .info.history.last_scrub_stamp,
        query_last_scrub: (.info.history.last_scrub| sub($spt;"x") ),
	query_is_future: .q_when_is_future,
	query_vs_date: .q_vs_date,

      }
   '`

  (( extr_dbg >= 2 )) && echo "combined:"
  #echo $from_qry " " $from_dmp | jq -s -r 'add | "(",(to_entries | .[] | "["+(.key|@sh)+"]="+(.value|@sh)),")"'

  # note that using a ref to an associative array is tricky. Instead - we are copying
  (( extr_dbg >= 1 )) && echo $from_qry " " $from_dmp | jq -s -r 'add | "(",(to_entries | .[] | "["+(.key)+"]="+(.value|@sh)),")"'
  local -A dict_src=`echo $from_qry " " $from_dmp | jq -s -r 'add | "(",(to_entries | .[] | "["+(.key)+"]="+(.value|@sh)),")"'`
  dict=()
  for k in "${!dict_src[@]}"; do dict[$k]=${dict_src[$k]}; done

  if [[ -n "$saved_echo_flag" ]]; then set -x; else set +x; fi
}

function schedule_against_expected() {
  local -n dict=$1 # a ref to the published state
  local -n ep=$2  # the expected results
  local extr_dbg=1

  #turn off '-x' (but remember previous state)
  local saved_echo_flag=${-//[^x]/}
  set +x

  #echo "printing the expected values: "
  #for w in "${!ep[@]}"
  #do
  #  echo $w " -> " ${ep[$w]}
  #done

  (( extr_dbg >= 1 )) && echo "- - comparing:"
  for k_ref in "${!ep[@]}"
  do
    local act_val=${dict[$k_ref]}
    local exp_val=${ep[$k_ref]}
    (( extr_dbg >= 1 )) && echo "key is " $k_ref "  expected: " $exp_val " in actual: " $act_val
    if [[ $exp_val != $act_val ]]
    then
      echo "$3 - '$k_ref' actual value ($act_val) differs from expected ($exp_val)"
      echo '####################################################^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^'

      if [[ -n "$saved_echo_flag" ]]; then set -x; fi
      return 1
      #break
    fi
  done

  if [[ -n "$saved_echo_flag" ]]; then set -x; fi
  return 0
}


# wait for either the value of 'last scrub' or 'last_scrub_duration' to change and
#
# $1: pg id
# $2: timestamp to compare to
# $3: max retries
# $4: debug message
function wait_fut_past_switch() {
  # query the PG, until such time that the 'scheduled time' turns from being in the future into
  # a fake date in the past, or for the last_scrub_stamp to change (meaning we've missed the scrub)
  echo TBD $4
  ceph pg scrub $pgid
  for i in $(seq 1 $3)
  do
    exit 1 # not finished RRRR
    sleep 0.5
    unset sc_data
    declare -A sc_data
    extract_published_sch $pgid $now_is $saved_last_stamp sc_data
    echo "${sc_data['dmp_last_duration']}"
    echo "----> loop:  $i  ~ ${sc_data['dmp_last_duration']}  / " ${sc_data['query_vs_date']}
    #(( ${sc_data['dmp_last_duration']} == 0)) || return 0
    [[ ${sc_data['dmp_vs_date']} ]] && return 0
  done
  echo "$4: wait_fut_past_switch(): failure. ${sc_data['query_last_stamp']}"
  return 1
}

 [[ -z $2 ]] || {
    local -n oa=$2
  }

# query the PG, until such time that the 'scheduled time' turns from being in the future into
# a fake date in the past, or for the last_scrub_stamp to change (meaning we've missed the scrub)
#
# $1: pg id
# $2: timestamp to compare to
# $3: max retries
# $4: debug message
# $5: [optional, out] the results array
function wait_future_past_switch() {
  #turn off '-x' (but remember previous state)
  local saved_echo_flag=${-//[^x]/}
  set +x

  local pgid=$1
  local cmp_date="$2"
  local retries=$3
  [[ -z $5 ]] || {
    local -n out_array=$5
  }

  echo " waiting... $4: $retries retries, with timestamp $2"
  local -A sc_data

  local now_is=`date -I"ns"`
  for i in $(seq 1 $3)
  do
    sleep 2
    #sc_data=()
    extract_published_sch $pgid $now_is $cmp_date sc_data
    echo "${sc_data['dmp_last_duration']}"
    echo "----> loop:  $i  ~ ${sc_data['dmp_last_duration']}  / " ${sc_data['query_vs_date']} " /   ${sc_data['dmp_future']}"
    #(( ${sc_data['dmp_last_duration']} == 0)) || break
    [[ ${sc_data['dmp_future']} ]] || break
    [[ ${sc_data['dmp_vs_date']} ]] || break
  done

  if (( $i < $retries )); then
    # success. Copy the array into the out param, if it's there:
    [[ -z $5 ]] || {
      for k in "${!sc_data[@]}"; do out_array[$k]=${sc_data[$k]}; done
    }
    if [[ -n "$saved_echo_flag" ]]; then set -x; fi
    return 0
  fi

  echo "$4: wait_future_past_switch(): failure. ${sc_data['query_last_stamp']}"
  if [[ -n "$saved_echo_flag" ]]; then set -x; fi
  return 1
}


# query the PG, until such time that the 'scheduled time' turns from being in the future into
# a fake date in the past, or for the last_scrub_stamp to change (meaning we've missed the scrub)
#
# $1: pg id
# $2: previous last_scrub_stamp
# $3: max retries
# $4: debug message
# $5: [out] the results array
function wait_for_active_scrub() {
  local pgid="$1"
  local cmp_date=$2
  local retries=$3
  echo " waiting for active... $4: $retries retries"
  local -n out_array=$5
  local -A sc_data
  local extr_dbg=1

  #turn off '-x' (but remember previous state)
  local saved_echo_flag=${-//[^x]/}
  set +x

  for i in $(seq 1 $3)
  do
    sleep 0.5
    extract_published_sch $pgid $2 $2 sc_data
    (( extr_dbg >= 1 )) && echo "${sc_data['dmp_last_duration']}"
    (( extr_dbg >= 1 )) && echo "----> loop:  $i  ~ ${sc_data['dmp_last_duration']}  / " ${sc_data['query_vs_date']} " /   ${sc_data['dmp_future']}"
    #(( ${sc_data['dmp_last_duration']} == 0)) || break
    [[ ${sc_data['query_active']} ]] && break
    [[ ${sc_data['query_vs_date']} ]] && break
  done

  if (( $i < retries )); then
    # success. Copy the array into the 'out' dictionary:
    for k in "${!sc_data[@]}"; do out_array[$k]=${sc_data[$k]}; done
    return 0
  fi

  echo "$4: wait_for_active_scrub(): failure. ${sc_data['query_last_stamp']}"
  return 1
}


# query the PG, until any of the conditions in the 'expected' array is met
#
# $1: pg id
# $2: max retries
# $3: a date to use in comparisons
# $4: set of K/V conditions
# $5: debug message
# $6: [out] the results array
function wait_any_cond() {
  local pgid="$1"
  local retries=$2
  local cmp_date=$3
  local -n ep=$4
  local -n out_array=$6
  local -A sc_data
  local extr_dbg=1

  (( extr_dbg >= 1 )) && echo " waiting for cond... " "$5 ($retries retries)"

  #turn off '-x' (but remember previous state)
  local saved_echo_flag=${-//[^x]/}
  set +x

  for i in $(seq 1 $retries)
  do
    sleep 0.5
    extract_published_sch $pgid 0 0 sc_data
    (( extr_dbg >= 2 )) && echo "${sc_data['dmp_last_duration']}"
    (( extr_dbg >= 2 )) && echo "----> loop:  $i  ~ ${sc_data['dmp_last_duration']}  / " ${sc_data['query_vs_date']} " /   ${sc_data['dmp_future']}"

    # perform schedule_against_expected(), but with slightly different out-messages behaviour
    for k_ref in "${!ep[@]}"
    do
      local act_val=${sc_data[$k_ref]}
      local exp_val=${ep[$k_ref]}
      (( extr_dbg >= 1 )) && echo "key is " $k_ref "  expected: " $exp_val " in actual: " $act_val
      if [[ $exp_val == $act_val ]]
      then
        echo "$5 - '$k_ref' actual value ($act_val) matches expected ($exp_val)"
        for k in "${!sc_data[@]}"; do out_array[$k]=${sc_data[$k]}; done
        if [[ -n "$saved_echo_flag" ]]; then set -x; fi
        return 0
      fi
    done
  done

  echo "$5: wait_any_cond(): failure. ${sc_data['query_active']}"
  if [[ -n "$saved_echo_flag" ]]; then set -x; fi
  return 1
}


# query the PG, until any of the conditions in the 'expected' array is met
#
# A condition may be negated by an additional entry in the 'expected' array. Its
# form should be:
#  key: the original key, with a "_neg" suffix;
#  Value: not checked
#
#
# $1: pg id
# $2: max retries
# $3: a date to use in comparisons
# $4: set of K/V conditions
# $5: debug message
# $6: [out] the results array
function wait_any_cond_w_neg() {
  local pgid="$1"
  local retries=$2
  local cmp_date=$3
  local -n ep=$4
  local -n out_array=$6
  local -A sc_data
  local extr_dbg=3

  echo "waiting for cond+ ($5): pg:$pgid dt:$cmp_date ($retries retries)"

  #turn off '-x' (but remember previous state)
  local saved_echo_flag=${-//[^x]/}
  set +x
  local now_is=`date -I"ns"`

  for i in $(seq 1 $retries)
  do
    sleep 0.5
    extract_published_sch $pgid $now_is $cmp_date sc_data
    (( extr_dbg >= 4 )) && echo "${sc_data['dmp_last_duration']}"
    (( extr_dbg >= 4 )) && echo "----> loop:  $i  ~ ${sc_data['dmp_last_duration']}  / " ${sc_data['query_vs_date']} " /   ${sc_data['dmp_future']}"
    (( extr_dbg >= 2 )) && echo "--> loop:  $i ~ ${sc_data['query_active']} / ${sc_data['query_vs_date']} " \
                      "/ ${sc_data['query_is_future']} / ${sc_data['query_is_future']} / ${sc_data['query_schedule']}  %%% ${!ep[@]}"

    # perform schedule_against_expected(), but with slightly different out-messages behaviour
    for k_ref in "${!ep[@]}"
    do
      (( extr_dbg >= 2 )) && echo "key is $k_ref"
      # is this a real key, or just a negation flag for another key??
      [[ $k_ref =~ "_neg" ]] && continue

      local act_val=${sc_data[$k_ref]}
      local exp_val=${ep[$k_ref]}

      # possible negation? look for a matching key
      local neg_key="${k_ref}_neg"
      (( extr_dbg >= 2 )) && echo "neg-key is $neg_key"
      if [ -v 'ep[$neg_key]' ]; then
        is_neg=1
      else
        is_neg=0
      fi

      (( extr_dbg >= 1 )) && echo "key is $k_ref: negation:$is_neg # expected: $exp_val # in actual: $act_val"
      is_eq=0
      [[ $exp_val == $act_val ]] && is_eq=1
      if (($is_eq ^ $is_neg))  
      then
        echo "$5 - '$k_ref' actual value ($act_val) matches expected ($exp_val) (negation: $is_neg)"
        for k in "${!sc_data[@]}"; do out_array[$k]=${sc_data[$k]}; done
        if [[ -n "$saved_echo_flag" ]]; then set -x; fi
        return 0
      fi
    done
  done

  echo "$5: wait_any_cond(): failure. ${sc_data['query_active']}"
  if [[ -n "$saved_echo_flag" ]]; then set -x; fi
  return 1
}


# query the PG, until such time that the 'scheduled time' turns from being in the future into
# a fake date in the past, or for the last_scrub_stamp to change (meaning we've missed the scrub)
#
# $1: pg id
# $2: previous last_scrub_stamp
# $3: max retries
# $4: debug message
# $5: [optional, out] the results array
function wait_for_scrub_done() {
  local pgid="$1"
  local saved_last_stamp="$2"
  local retries=$3
  local -n dict=$5 # a ref to the in/out dictionary
  local extr_dbg=1

  #turn off '-x' (but remember previous state)
  local saved_echo_flag=${-//[^x]/}
  set +x

  (( extr_dbg >= 2 )) && echo " waiting for completion... " "$4"
  [[ -z $5 ]] || {
    local -n out_array=$5
  }
  local -A sc_data
  for i in $(seq 1 $retries)
  do
    sleep 3
    #sc_data=()
    extract_published_sch $pgid  $saved_last_stamp  $saved_last_stamp sc_data
    (( extr_dbg >= 2 )) && echo "${sc_data['dmp_last_duration']}"
    (( extr_dbg >= 1 )) && echo "----> loop:  $i  ~ ${sc_data['dmp_last_duration']}  / " ${sc_data['query_vs_date']} " /   ${sc_data['dmp_future']}"
    [[ ${sc_data['dmp_future']} ]] && break
    [[ ${sc_data['dmp_vs_date']} ]] && break
  done

  if (( $i < $retries )); then
    # success. Copy the array into the out param, if it's there:
    [[ -z $5 ]] || {
      for k in "${!sc_data[@]}"; do out_array[$k]=${sc_data[$k]}; done
    }

    if [[ -n "$saved_echo_flag" ]]; then set -x; fi
    return 0
  fi

  echo "$4: wait_future_past_switch(): failure. ${sc_data['query_last_stamp']}"
  if [[ -n "$saved_echo_flag" ]]; then set -x; fi
  return 1
}



function TEST_dump_scrub_schedule() {
    local dir=$1
    local poolname=test
    local OSDS=3
    local objects=15

    TESTDATA="testdata.$$"

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=$OSDS || return 1
    run_mgr $dir x || return 1

    # Set scheduler to "wpq" until there's a reliable way to query scrub states
    # with "--osd-scrub-sleep" set to 0. The "mclock_scheduler" overrides the
    # scrub sleep to 0 and as a result the checks in the test fail.
    local ceph_osd_args="--osd_deep_scrub_randomize_ratio=0 \
            --osd_scrub_interval_randomize_ratio=0 \
            --osd_op_queue=wpq \
            --osd_scrub_sleep=2.0"

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd $ceph_osd_args|| return 1
    done

    # Create a pool with a single pg
    create_pool $poolname 1 1
    wait_for_clean || return 1
    poolid=$(ceph osd dump | grep "^pool.*[']${poolname}[']" | awk '{ print $2 }')
    # ceph osd set noscrub || return 1

    dd if=/dev/urandom of=$TESTDATA bs=1032 count=10
    for i in `seq 1 $objects`
    do
        rados -p $poolname put obj${i} $TESTDATA
    done
    rm -f $TESTDATA

    local pgid="${poolid}.0"
    local now_is=`date -I"ns"`

    # before the scrubbing starts

    # last scrub duration should be 0. The scheduling data should show
    # a time in the future:
    # e.g. 'periodic scrub scheduled @ 2021-10-12T20:32:43.645168+0000'

    declare -A expct_starting=( ['query_active']="false" ['query_is_future']="true" ['query_schedule']="scrub scheduled" )
    declare -A sched_data
    extract_published_sch $pgid $now_is "2019-10-12T20:32:43.645168+0000" sched_data
    schedule_against_expected sched_data expct_starting
    (( ${sched_data['dmp_last_duration']} == 0)) || return 1
    echo "last-scrub  --- " ${sched_data['query_last_scrub']}
# RRR can also check [dmp_schedule]='periodic scrub scheduled'

    #
    # step 1: scrub once (mainly to ensure there is no urgency to scrub)
    #

    saved_last_stamp=${sched_data['query_last_stamp']}
    ceph tell osd.* config set osd_scrub_sleep "0"
    ceph pg deep-scrub $pgid
    ceph pg scrub $pgid

    # wait for the 'last duration' entries to change. Note that the 'dump' one will need
    # up to 5 seconds to sync

    sleep 3
    sched_data=()
    declare -A expct_qry_duration=( ['query_last_duration']="0" ['query_last_duration_neg']="not0" )
    wait_any_cond_w_neg $pgid 10 $saved_last_stamp expct_qry_duration "WaitingAfterScrub " sched_data || return 1
    # verify that 'pg dump' also shows the change in last_scrub_duration
    sched_data=()
    declare -A expct_dmp_duration=( ['dmp_last_duration']="0" ['dmp_last_duration_neg']="not0" )
    wait_any_cond_w_neg $pgid 10 $saved_last_stamp expct_dmp_duration "WaitingAfterScrub_dmp " sched_data || return 1

    sleep 2

    #
    # step 2: set noscrub and request a "periodic scrub". Watch for the change in the 'is the scrub
    #         scheduled for the future' value
    #

    ceph tell osd.* config set osd_scrub_chunk_max "3" || return 1
    ceph tell osd.* config set osd_scrub_sleep "1.0" || return 1
    ceph osd set noscrub || return 1
    sleep 2
    saved_last_stamp=${sched_data['query_last_stamp']}
    ## RRRRRRRRR  ## ceph pg scrub $pgid || return 1


    ceph pg $pgid scrub
    #pg_scrub $pgid || return 1
    sleep 1 # needed?
    sched_data=()
    declare -A expct_scrub_peri_sched=( ['query_is_future']="false" )
    wait_any_cond_w_neg $pgid 10 $saved_last_stamp expct_scrub_peri_sched "waitingBeingScheduled" sched_data || return 1

    # note: the induced change in 'last_scrub_stamp' that we've caused above, is by itself not a publish-stats
    # trigger. Thus it might happen that the information in 'pg dump' will not get updated here. Do not expect
    # 'dmp_future' to follow 'query_is_future' without a good reason
    ## declare -A expct_scrub_peri_sched_dmp=( ['dmp_future']="false" )
    ## wait_any_cond_w_neg $pgid 15 $saved_last_stamp expct_scrub_peri_sched_dmp "waitingBeingScheduled" sched_data || echo "must be fixed"

    #
    # step 3: allow scrubs. Watch for the conditions during the scrubbing
    #

    saved_last_stamp=${sched_data['query_last_stamp']}
    ceph osd unset noscrub

    declare -A cond_active=( ['query_active']="true" )
    sched_data=()
    wait_any_cond $pgid 10 $saved_last_stamp cond_active "WaitingActive " sched_data || return 1

    # check for pg-dump to show being active. But if we see 'query_active' being reset - we've just
    # missed it.
    declare -A cond_active_dmp=( ['dmp_state_has_scrubbing']="true" ['query_active']="false" )
    sched_data=()
    wait_any_cond $pgid 10 $saved_last_stamp cond_active_dmp "WaitingActive " sched_data || return 1

    teardown $dir || return 1
}

main osd-scrub-test "$@"

# Local Variables:
# compile-command: "cd build ; make -j4 && \
#    ../qa/run-standalone.sh osd-scrub-test.sh"
# End:
#MDS=0 MGR=1 OSD=3 MON=1 ../src/vstart.sh -n --without-dashboard --memstore -X -o "osd_scrub_auto_repair=true" -o "osd_deep_scrub_randomize_ratio=0" -o 
#"osd_scrub_interval_randomize_ratio=0" -o "osd_op_queue=wpq"  -o "osd_scrub_sleep=4.0" -o "memstore_device_bytes=68435456"