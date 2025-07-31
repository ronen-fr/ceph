#!/usr/bin/env bash
#
# Copyright (C) 2019 Red Hat <contact@redhat.com>
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


# 30.11.2023: the test is now disabled, as the reservation mechanism has been
# thoroughly reworked and the test is no longer valid.  The test is left here
# as a basis for a new set of primary vs. replicas scrub activation tests.

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/scrub/scrub-helpers.sh


MAX_SCRUBS=4
SCRUB_SLEEP=3
POOL_SIZE=3

function run() {
    local dir=$1
    shift
    local CHUNK_MAX=5

    export CEPH_MON="127.0.0.1:7184" # git grep '\<7184\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    CEPH_ARGS+="--osd_max_scrubs=$MAX_SCRUBS "
    CEPH_ARGS+="--osd_shallow_scrub_chunk_max=$CHUNK_MAX "
    CEPH_ARGS+="--osd_scrub_sleep=$SCRUB_SLEEP "
    CEPH_ARGS+="--osd_pool_default_size=$POOL_SIZE "
    # Set scheduler to "wpq" until there's a reliable way to query scrub states
    # with "--osd-scrub-sleep" set to 0. The "mclock_scheduler" overrides the
    # scrub sleep to 0 and as a result the checks in the test fail.
    CEPH_ARGS+="--osd_op_queue=wpq "

    export -n CEPH_CLI_TEST_DUP_COMMAND
    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

##
# a modified version of wait_for_scrub(), which terminates if the Primary
# of the to-be-scrubbed PG changes
#
# Given the *last_scrub*, wait for scrub to happen on **pgid**. It
# will fail if scrub does not complete within $TIMEOUT seconds. The
# repair is complete whenever the **get_last_scrub_stamp** function
# reports a timestamp different from the one given in argument.
#
# @param pgid the id of the PG
# @param the primary OSD when started
# @param last_scrub timestamp of the last scrub for *pgid*
# @return 0 on success, 1 on error
#
function wait_for_scrub_mod() {
    local pgid=$1
    local orig_primary=$2
    local last_scrub="$3"
    local sname=${4:-last_scrub_stamp}
    local tout=${5:-3000}

    for ((i=0; i < $tout; i++)); do
        sleep 0.1
        if test "$(get_last_scrub_stamp $pgid $sname)" '>' "$last_scrub" ; then
            return 0
        fi
        sleep 0.1
        # are we still the primary?
        local current_primary=`./bin/ceph pg $pgid query | jq '.acting[0]' `
        if [ $orig_primary != $current_primary ]; then
            echo $orig_primary no longer primary for $pgid
            return 0
        fi
    done
    return 1
}


function objs_to_prim_dict()
{
    local dir=$1
    local poolname=$2
    local basename=$3
    local obj_num=$4
    local -n obj_pgid_dict=$5
    local -n obj_prim_dict=$6

    for i in $(seq 1 $obj_num ); do
        obj="${basename}${i}"
        pgid=$(ceph --format=json-pretty osd map $poolname $obj | jq -r '.pgid')
        obj_pgid_dict["$obj"]=$pgid
        primary_osd=$(ceph --format=json-pretty osd map $poolname $obj | jq -r '.acting_primary')
        obj_prim_dict["$obj"]=$primary_osd
    done
}


# modify object for which the downed OSD is primary.
# Up to $3 on the named OSD. The actual number will be returned.
function modify_()
{
  local dir=$1
  local target_osd=$2
  local count=$3
  #passed a dictionary of PG->primary OSDs

  printf "%s\n" "${!D[@]}" | shuf -n "$E" | while read -r selected_key; do
    #FUN "$selected_key" "${D[$selected_key]}"
    echo "Modifying $selected_key ${D[$selected_key]}"
  done

  # list objects for which we are primary

  # take that OSD down


}

# modify object for which the downed OSD is primary / secondary.
function modify_obs_of_an_osd()
{
  local dir=$1
  local target_osd=$2
  # list of objects to modify
  local objects_to_modify=$3

  # Skip if no objects to modify
  if [[ -z "$objects_to_modify" ]]; then
        return 0
  fi


  # printf "%s\n" "${!D[@]}" | shuf -n "$E" | while read -r selected_key; do
  #   FUN "$selected_key" "${D[$selected_key]}"

  # the objects to modify:
  echo "Objects to modify on osd.$target_osd: ${objects_to_modify[@]}"

  # take that OSD down


}


# Parameters:
# 1: directory
# 2: number of OSDs (default 3)
# 3: number of PGs (no default)
# 4: average # of objects per PG
# 5: # elements to currupt their Primary version
# 6: # elements to currupt one their replicas
function corrupt_and_measure()
{
    local dir=$1
    #local OSDS=${2:-3}
    local OSDS=$2
    local PGS=$3
    local OBJS_PER_PG=$4
    local CORRUPT_PRIMARY=$5
    local CORRUPT_REPLICA=$6
    local objects=$(($PGS * $OBJS_PER_PG))
    # the total number of corrupted objects cannot exceed the number of objects
    if [ $(($CORRUPT_PRIMARY + $CORRUPT_REPLICA)) -gt $objects ]; then
        echo "ERROR: too many corruptions requested ($CORRUPT_PRIMARY + $CORRUPT_REPLICA > $objects)"
        return 1
    fi

    local -A cluster_conf=(
        ['osds_num']="$OSDS"
        ['pgs_in_pool']="$PGS"
        ['pool_name']="test"
    )
    local extr_dbg=3 # note: 3 and above leave some temp files around
    standard_scrub_wpq_cluster "$dir" cluster_conf 0 || return 1

    local poolid=${cluster_conf['pool_id']}
    local poolname=${cluster_conf['pool_name']}
    echo "Pool: $poolname : $poolid"
    # prevent scrubbing while we corrupt objects
    bin/ceph osd pool set $poolname noscrub 1
    bin/ceph osd pool set $poolname nodeep-scrub 1

    #turn off '-x' (but remember previous state)
    local saved_echo_flag=${-//[^x]/}
    set +x

    # Create some objects
    local testdata_file=$(file_with_random_data 1024)
    for i in `seq 1 $objects`
    do
        rados -p $poolname put obj${i} $testdata_file || return 1
    done
    rm $testdata_file
    wait_for_clean || return 1
    bin/ceph osd pool stats

    declare -A obj_to_pgid
    declare -A obj_to_primary
    objs_to_prim_dict "$dir" $poolname "obj" $objects obj_to_pgid obj_to_primary
    # select a subset of "objects on their primary"
    mapfile -t selected_keys < <(printf "%s\n" "${!obj_to_primary[@]}" | shuf -n "$CORRUPT_PRIMARY")
    declare -A prim_objs_to_corrupt
    # group by the primary OSD (the dict value)
    for k in "${selected_keys[@]}"; do
        prim_osd=${obj_to_primary[$k]}
        prim_objs_to_corrupt["$prim_osd"]+="$k "
    done
    if [[ -n "$saved_echo_flag" ]]; then set -x; fi

    # for each OSD:
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
        # get the objects to modify for this OSD
        #local -n D=${prim_objs_to_corrupt[$osd]}
        #modify_obs_of_an_osd "$dir" "$osd" D
        modify_obs_of_an_osd "$dir" "$osd" "${prim_objs_to_corrupt[$osd]}"
    done


    # disable rescheduling of the queue due to 'no-scrub' flags
    bin/ceph tell osd.* config set osd_scrub_backoff_ratio 0.9999
    ceph tell osd.* config set osd_scrub_sleep "0"


    # --------------------------  step 2: corruption of objects --------------------------

    # create object errors
    #shuf -i 0-$T -n $N

    # ---------------------------  step 3: scrub & measure -------------------------------

    # set the scrub parameters and the update frequency for low latencies
    ceph tell osd.* config set osd_scrub_sleep "0"
    ceph tell osd.* config set osd_max_scrubs 1  # for now, only one scrub at a time
    ceph tell osd.* config set osd_stats_update_period_not_scrubbing 1
    ceph tell osd.* config set osd_stats_update_period_scrubbing 1
    ceph tell osd.* config set osd_scrub_chunk_max 5
    ceph tell osd.* config set osd_shallow_scrub_chunk_max 5


    #create the dictionary of the PGs in the pool
    declare -A pg_pr
    declare -A pg_ac
    declare -A pg_po
    build_pg_dicts "$dir" pg_pr pg_ac pg_po "-"
    (( extr_dbg >= 1 )) && echo "PGs table:"
    for pg in "${!pg_pr[@]}"; do
      wait_for_pg_clean $pg || return 1
      (( extr_dbg >= 1 )) && echo "Got: $pg: ${pg_pr[$pg]} ( ${pg_ac[$pg]} ) ${pg_po[$pg]}"
    done
    local -A saved_last_stamp
    for pg in "${!pg_pr[@]}"; do
        saved_last_stamp[$pg]=$(get_last_scrub_stamp $pg last_scrub_stamp)
    done

    #local start_time=$(date +%s%N)
    local start_time=$(date +%s)
    for pg in "${!pg_pr[@]}"; do
        ceph pg $pg deep-scrub || return 1
        wait_for_scrub_mod $pg ${pg_pr[$pg]} ${saved_last_stamp[$pg]} last_scrub_stamp 6000 || return 1
    done
    local end_time=$(date +%s)
    #local duration=$(( (end_time - start_time)/1000000 ))
    local duration=$(( end_time - start_time ))
    printf 'MSR %3d %3d %3d %6d\n' "$OSDS" "$PGS" "$CORRUPT_PRIMARY" "$duration"
    return 0
}


function TEST_time_measurements_basic()
{
  corrupt_and_measure "$1" 3 4 6 5 0 || return 1

}

function T__EST_recover_unexpected() {
    local dir=$1
    shift
    local OSDS=6
    local PGS=16
    local POOLS=3
    local OBJS=1000

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for o in $(seq 0 $(expr $OSDS - 1))
    do
        run_osd $dir $o
    done

    for i in $(seq 1 $POOLS)
    do
        create_pool test$i $PGS $PGS
    done

    wait_for_clean || return 1

    dd if=/dev/urandom of=datafile bs=4k count=2
    for i in $(seq 1 $POOLS)
    do
       for j in $(seq 1 $OBJS)
       do
	       rados -p test$i put obj$j datafile
       done
    done
    rm datafile

    ceph osd set noscrub
    ceph osd set nodeep-scrub

    for qpg in $(ceph pg dump pgs --format=json-pretty | jq '.pg_stats[].pgid')
    do
	eval pg=$qpg   # strip quotes around qpg
	ceph tell $pg scrub
    done

    ceph pg dump pgs

    max=$(CEPH_ARGS='' ceph daemon $(get_asok_path osd.0) dump_scrub_reservations | jq '.osd_max_scrubs')
    if [ $max != $MAX_SCRUBS ]; then
        echo "ERROR: Incorrect osd_max_scrubs from dump_scrub_reservations"
        return 1
    fi

    ceph osd unset noscrub

    ok=false
    for i in $(seq 0 300)
    do
	ceph pg dump pgs
	if ceph pg dump pgs | grep '+scrubbing'; then
	    ok=true
	    break
	fi
	sleep 1
    done
    if test $ok = "false"; then
	echo "ERROR: Test set-up failed no scrubbing"
	return 1
    fi

    local total=0
    local zerocount=0
    local maxzerocount=3
    while(true)
    do
	pass=0
	for o in $(seq 0 $(expr $OSDS - 1))
	do
		CEPH_ARGS='' ceph daemon $(get_asok_path osd.$o) dump_scrub_reservations
		scrubs=$(CEPH_ARGS='' ceph daemon $(get_asok_path osd.$o) dump_scrub_reservations | jq '.scrubs_local + .granted_reservations')
		if [ $scrubs -gt $MAX_SCRUBS ]; then
		    echo "ERROR: More than $MAX_SCRUBS currently reserved"
		    return 1
	        fi
		pass=$(expr $pass + $scrubs)
        done
	if [ $pass = "0" ]; then
	    zerocount=$(expr $zerocount + 1)
	fi
	if [ $zerocount -gt $maxzerocount ]; then
	    break
	fi
	total=$(expr $total + $pass)
	if [ $total -gt 0 ]; then
	    # already saw some reservations, so wait longer to avoid excessive over-counting.
	    # Note the loop itself takes about 2-3 seconds
	    sleep $(expr $SCRUB_SLEEP - 2)
	else
	    sleep 0.5
	fi
    done

    # Check that there are no more scrubs
    for i in $(seq 0 5)
    do
        if ceph pg dump pgs | grep '+scrubbing'; then
	    echo "ERROR: Extra scrubs after test completion...not expected"
	    return 1
        fi
	sleep $SCRUB_SLEEP
    done

    echo $total total reservations seen

    # Sort of arbitraty number based on PGS * POOLS * POOL_SIZE as the number of total scrub
    # reservations that must occur.  However, the loop above might see the same reservation more
    # than once.
    actual_reservations=$(expr $PGS \* $POOLS \* $POOL_SIZE)
    if [ $total -lt $actual_reservations ]; then
	echo "ERROR: Unexpectedly low amount of scrub reservations seen during test"
	return 1
    fi

    return 0
}


main osd-scrub-dump "$@"

# Local Variables:
# compile-command: "cd build ; make check && \
#    ../qa/run-standalone.sh osd-scrub-dump.sh"
# End:
