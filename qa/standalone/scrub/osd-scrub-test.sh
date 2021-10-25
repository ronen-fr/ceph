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

  bin/ceph pg dump pgs -f json-pretty >> /tmp/a_dmp$$

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

  echo "from pg dump pg:"
  echo $from_dmp

  echo "query out==="
  bin/ceph pg $1 query -f json-pretty | awk -e '/scrubber/,/agent_state/ {print;}'

  bin/ceph pg $1 query -f json-pretty >> /tmp/a_qry$$

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

  # [.][0][][].scrubber | to_entries[] | .key 
  #echo "from pg x query:"
  #echo $from_qry

  echo "combined:"
  #echo $from_qry " " $from_dmp | jq -s -r 'add | "(",(to_entries | .[] | "["+(.key|@sh)+"]="+(.value|@sh)),")"'

  # note that using a ref to an associative array is tricky. Instead - we are copying
  echo $from_qry " " $from_dmp | jq -s -r 'add | "(",(to_entries | .[] | "["+(.key)+"]="+(.value|@sh)),")"'
  local -A dict_src=`echo $from_qry " " $from_dmp | jq -s -r 'add | "(",(to_entries | .[] | "["+(.key)+"]="+(.value|@sh)),")"'`
  for k in "${!dict_src[@]}"; do dict[$k]=${dict_src[$k]}; done
}

function schedule_against_expected() {
  local -n dict=$1 # a ref to the published state
  local -n ep=$2  # the expected results

  #echo "printing the expected values: "
  #for w in "${!ep[@]}"
  #do
  #  echo $w " -> " ${ep[$w]}
  #done

  echo "- - comparing:"
  for k_ref in "${!ep[@]}"
  do
    local act_val=${dict[$k_ref]}
    local exp_val=${ep[$k_ref]}
    echo "key is " $k_ref "  expected: " $exp_val " in actual: " $act_val
    if [[ $exp_val != $act_val ]]
    then
      echo "$3 - '$k_ref' actual value ($act_val) differs from expected ($exp_val)"
      echo '####################################################^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^'
      #return 1
    fi
  done
  return 0
}

function extract_published_sch__() {
  # $1 is the pg
  # $2 is the 'current time' to use as ref
  declare -A dict=(['last']="17"
                   ['state']="periodic deep scrub scheduled"
                   ['active']="False"
                   ['at']="2021-10-12T20:40:03.393135+0000"
                   ['at_in_future']="True"
                   ['is_deep']="True"
                   ['query_state']="deep scrub scheduled"
                   ['query_deep']='true'
                  )
  sched_dict = dict
  echo '('
  for key in  "${!dict[@]}" ; do
    echo "['$key']='${dict[$key]}'"
  done
  echo ')'
}

function wait_fut_past_switch() {

  # query the PG, until such time that the 'scheduled time' turns from being in the future into
  # a fake date in the past, or for the last_scrub_stamp to change (meaning we've missed the scrub)
  echo TBD

}

# expected results at specific points during the test:
declare -A expct_starting=( ['query_active']="false" ['query_is_future']="true" ['query_schedule']="scrub scheduled" );
declare -A expct_cantrun=( ['query_active']="false" ['query_is_future']="false" ['query_schedule']="queued for scrub" );

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
    declare -A sched_data
    extract_published_sch $pgid $now_is "2019-10-12T20:32:43.645168+0000" sched_data
    schedule_against_expected sched_data expct_starting
    echo "last-scrub  --- " ${sched_data['query_last_scrub']}

    #for w in "${!sched_data[@]}"
    #do
    #  echo $w " -> " ${sched_data[$w]}
    #done

    #
    # step 1: scrub once (mainly to ensure there is no urgency to scrub)
    #

    # verify that we have '0' as the last-scrub-duration value
    (( ${sched_data['dmp_last_duration']} == 0)) || return 1
    saved_last_stamp=${sched_data['query_last_stamp']}
    ceph tell osd.* config set osd_scrub_sleep "0"
    ceph pg deep-scrub $pgid
    ceph pg scrub $pgid
    for i in $(seq 0 10)
    do
      sleep 0.5
      unset sched_data
      declare -A sched_data
      extract_published_sch $pgid $now_is saved_last_stamp sched_data
      echo "${sched_data['dmp_last_duration']}"
      echo "----> loop:  $i  ~ ${sched_data['dmp_last_duration']}  / " ${sched_data['query_vs_date']}
      (( ${sched_data['dmp_last_duration']} == 0)) || break
      [[ ${sched_data['query_vs_date']} ]] && break
    done
    echo ${sched_data['query_last_stamp']}

    schedule_against_expected sched_data expct_starting

    # last scrub duration should be 0. The scheduling data should show
    # a time in the future:
    # e.g. 'periodic scrub scheduled @ 2021-10-12T20:32:43.645168+0000'

    #local pgid="${poolid}.0"
    ceph tell osd.* config set osd_scrub_chunk_max "3"
    ceph tell osd.* config set osd_scrub_sleep "1.0"

    ceph osd set noscrub || return 1
    sleep 2
    ceph pg scrub $pgid
    #pg_scrub $pgid || return 1
    sleep 1
    extract_published_sch $pgid $now_is "2019-10-12T20:32:43.645168+0000" sched_data
    schedule_against_expected sched_data expct_cantrun
    sleep 0.5
    extract_published_sch $pgid $now_is "2019-10-12T20:32:43.645168+0000" sched_data
    sleep 1.5
    extract_published_sch $pgid $now_is "2019-10-12T20:32:43.645168+0000" sched_data
    sleep 1.5
    extract_published_sch $pgid $now_is "2019-10-12T20:32:43.645168+0000" sched_data
    sleep 2.0
    extract_published_sch $pgid $now_is "2019-10-12T20:32:43.645168+0000" sched_data
    sleep 10.0

    ceph pg deep-scrub $pgid
    extract_published_sch $pgid $now_is "2019-10-12T20:32:43.645168+0000" sched_data
    sleep 0.5
    extract_published_sch $pgid $now_is "2019-10-12T20:32:43.645168+0000" sched_data
    sleep 1.5

    ceph pg scrub $pgid
    sleep 0.5
    extract_published_sch $pgid $now_is "2019-10-12T20:32:43.645168+0000" sched_data
    sleep 1.5
    extract_published_sch $pgid $now_is "2019-10-12T20:32:43.645168+0000" sched_data
    sleep 1.5
    extract_published_sch $pgid $now_is "2019-10-12T20:32:43.645168+0000" sched_data
    sleep 2.0
    extract_published_sch $pgid $now_is "2019-10-12T20:32:43.645168+0000" sched_data

    ceph pg $pgid query | jq '.info.stats.scrub_duration'
    test "$(ceph pg $pgid query | jq '.info.stats.scrub_duration')" '>' "0" || return 1

    teardown $dir || return 1
}

main osd-scrub-test "$@"

# Local Variables:
# compile-command: "cd build ; make -j4 && \
#    ../qa/run-standalone.sh osd-scrub-test.sh"
# End:
#MDS=0 MGR=1 OSD=3 MON=1 ../src/vstart.sh -n --without-dashboard --memstore -X -o "osd_scrub_auto_repair=true" -o "osd_deep_scrub_randomize_ratio=0" -o 
#"osd_scrub_interval_randomize_ratio=0" -o "osd_op_queue=wpq"  -o "osd_scrub_sleep=4.0" -o "memstore_device_bytes=68435456"