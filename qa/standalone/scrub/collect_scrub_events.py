#!/usr/bin/python3

import re
import requests
import argparse
import subprocess
import sys
import os
import pathlib
import json
import time
import datetime
from subprocess import Popen

datetime_ptrn_txt = r'(?P<TM>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3})([+-]0[0-9]00 )'
datetime_ptrn = re.compile(datetime_ptrn_txt)

pr_and_osd_ptrn_txt = r'[ ]+[0-9]+ osd.(?P<osdn>[0-9]+) '
pr_and_osd_ptrn = re.compile(pr_and_osd_ptrn_txt)

regline_ptrn_txt = datetime_ptrn.pattern + r'([0-9A-Fa-f]+)' + pr_and_osd_ptrn.pattern + r'(?P<rest>.*)$'
regline_ptrn = re.compile(regline_ptrn_txt)

# patterns for the 'rest' of the line:

#osd-scrub:initiate_scrub: initiating scrub on pg[1.bs0>]
init_on_pg_ptrn_txt = r'initiating scrub on pg\[(?P<pgid>[0-9]+.[a-f0-9]+)'
init_on_pg_ptrn = re.compile(init_on_pg_ptrn_txt)

#...scrubber::ReplicaReservations pg[1.0s0>]: handle_reserve_reject: rejected by 0(1) (MOSDScrubReserve(1.0s0 REJECT e40) v1)
reject_by_ptrn_txt = r'handle_reserve_reject: rejected by (?P<from_osd>[0-9]+)\((?P<from_rep>[0-9]+)\) \(MOSDScrubReserve\((?P<pgid>[0-9]+.[a-f0-9]+) REJECT e(?P<epoch>[0-9]+) v1\)\)'
reject_by_ptrn = re.compile(reject_by_ptrn_txt)

# and here we can find the 'deepness' of the scrub:
# 2023-09-27T06:13:48.064-0500 7fc93c14c6c0 10 osd.1 40 dequeue_op MOSDScrubReserve(1.0s0 REJECT e40) v1 prio 127 cost 0 latency 0.000223 MOSDScrubReserve(1.0s0 REJECT e40) v1 pg pg[1.0s0( v 40'41 (0'0,40'41] local-lis/les=37/38 n=41 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [1,0,2,4,3,5]p1(0) r=0 lpr=37 crt=40'41 lcod 40'40 mlcod 40'40 active+clean+scrubbing+deep [ 1.0s0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB]
frjct_by_ptrn_txt = r'MOSDScrubReserve\((?P<pgid>[0-9]+.[a-f0-9]+)s[0-9a-f]+ REJECT e(?P<epoch>[0-9]+)\).*scrubbing(?P<is_deep>\+deep)*'
frjct_by_ptrn = re.compile(frjct_by_ptrn_txt)

#...scrubber<ReservingReplicas>: scrubber event -->> send_remotes_reserved epoch: 40
resrvd_ptrn_txt = r'pg\[(?P<pgid>[0-9]+.[a-f0-9]+).*scrubbing(?P<is_deep>\+deep)*.*scrubber<ReservingReplicas>: scrubber event -->> send_remotes_reserved epoch: (?P<epoch>[0-9]+)'
resrvd_ptrn = re.compile(resrvd_ptrn_txt)

#...scrubber<Act/WaitDigestUpdate>: scrubber event -->> send_scrub_is_finished epoch: 40
finished_ptrn_txt = r'pg\[(?P<pgid>[0-9]+.[a-f0-9]+).*scrubber<Act/WaitDigestUpdate>: scrubber event -->> send_scrub_is_finished epoch: (?P<epoch>[0-9]+)'
finished_ptrn = re.compile(finished_ptrn_txt)


# failure to inc local resources:
# osd-scrub:log_fwd: can_inc_scrubs== false. 1 (local) + 1 (remote) >= max (2)
inc_failed_ptrn_txt = r'can_inc_scrubs== false. (?P<local>[0-9]+) \(local\) \+ (?P<remote>[0-9]+) \(remote\) >= max \((?P<max>[0-9]+)\)'
inc_failed_ptrn = re.compile(inc_failed_ptrn_txt)


#+ r'(?P<pgid>[0-9]+.[0-9]+) '


# patterns of interest:
# reservation messages & states:
#pat_osd_num=re.compile(r'(ceph-)*osd.(?P<osdn>[0-9]+).log(.gz)*')
#core_path_parts=re.compile(r'.*/(?P<rmt>((gibba)|(smithi))[0-9]+)/coredump/(?P<unzped_name>.*core).gz')

# 2023-09-27T06:13:37.595-0500 7f2d1baf76c0 20 osd.6 pg_epoch: 40 pg[1.fs0( v 40'31 (0'0,40'31] local-lis/les=37/38 n=31 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [6,0,2,3,4,1]p6(0) r=0 lpr=37 crt=40'31 lcod 40'30 mlcod 40'30 active+clean+scrubbing+deep [ 1.fs0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] forward_scrub_event: ScrubFinished queued at: 40



def print_ev_log(ev_log):
  for x in ev_log:
    print(x)

def basic_log_parse(line) -> tuple((str, str, str)):
  #print(f'basic_log_parse():  {line}')
  m = regline_ptrn.search(line)
  if m:
    print(f'   {m.group(1)}//{m.group(2)}//{m.group(3)}/    <{m.group("osdn")}> - {m.group("rest")}')
    return (m.group(1), m.group("osdn"), m.group("rest"))
  else:
    print(f'   no match\n\n')
    return None


def log_line_parse(line) -> tuple((str, str, str, str, str, ...)):
  print(f'log_line_parse():  {line}')
  hdr = basic_log_parse(line)
  if hdr:
     #print(hdr)
     dt = hdr[0]
     osd = hdr[1]
     #print(f' XXXXXXXX  {dt} XXXXXXXXX {osd}')

     # try to match the 'rest' of the line against a set of patterns
     rst = hdr[2]

     m = init_on_pg_ptrn.search(rst)
     if m:
       pgid = m.group("pgid")
       print(f'   {pgid}')
       return ('scrub-initiated', dt, osd, pgid, 'x', rst)

     m = reject_by_ptrn.search(rst)
     if m:
       pgid = m.group("pgid")
       from_osd = m.group("from_osd")
       from_rep = m.group("from_rep")
       epoch = m.group("epoch")
       print(f'    from osd.{from_osd}: rep={from_rep} epoch={epoch}')
       return ('reject-by', dt, osd, pgid, 'x', f'from osd.{from_osd}: rep={from_rep} epoch={epoch}')

     m = frjct_by_ptrn.search(rst)
     if m:
       pgid = m.group("pgid")
       epoch = m.group("epoch")
       deep = 'd' if m.group("is_deep") else 's'
       print(f'    epoch={epoch} <<{m.group("is_deep")}>>')
       return ('reject-by', dt, osd, pgid, deep, f'epoch={epoch}')
       #return ('reject-by', dt, osd, pgid, deep, f'<<<{rst}>>>')

     m = resrvd_ptrn.search(rst)
     if m:
       pgid = m.group("pgid")
       deep = 'd' if m.group("is_deep") else 's'
       print(f'   reserved {pgid} {deep}')
       return ('rep-reserved', dt, osd, pgid, deep, m.group("epoch"))

     m = finished_ptrn.search(rst)
     if m:
       pgid = m.group("pgid")
       print(f'   {pgid}')
       return ('scrub-completed', dt, osd, pgid, 'x', 'epoch='+m.group("epoch"))

     m = inc_failed_ptrn.search(rst)
     if m:
       print(f'   inc failure: {m.group("local")} + {m.group("remote")} >= {m.group("max")}')
       return ('inc-failed', dt, osd, 'x', 'x', f'{m.group("local")} / {m.group("remote")} / {m.group("max")}')





  return None



















# called with the name of the file holding a previously
# collected partial events log
def add_from_file(elog, fn) :
        with open(fn, 'r') as ev_fd:
                ev_log = json.load(ev_fd)
                for x in ev_log:
                        elog.append(x)

# search the logs for events of interest

# search one specific log for events of interest
def osd_events(elog, osd_num, fn) :
  fd = open(fn, 'r')
  all_lines = fd.readlines()
  fd.close()
  #cit = re.finditer(



# test data
testdt=r'''
2023-09-27T06:14:11.945-0500 7fc93c14c6c0 10 osd.1 pg_epoch: 40 pg[1.0s0( v 40'41 (0'0,40'41] local-lis/les=37/38 n=41 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [1,0,2,4,3,5]p1(0) r=0 lpr=37 crt=40'41 lcod 40'40 mlcod 40'40 active+clean+scrubbing+deep [ 1.0s0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] _handle_message: MOSDScrubReserve(1.0s0 GRANT e40) v1
2023-09-27T06:14:11.945-0500 7fc93c14c6c0 10 osd.1 pg_epoch: 40 pg[1.0s0( v 40'41 (0'0,40'41] local-lis/les=37/38 n=41 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [1,0,2,4,3,5]p1(0) r=0 lpr=37 crt=40'41 lcod 40'40 mlcod 40'40 active+clean+scrubbing+deep [ 1.0s0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] scrubber<ReservingReplicas>: handle_scrub_reserve_grant MOSDScrubReserve(1.0s0 GRANT e40) v1
2023-09-27T06:14:11.945-0500 7fc93c14c6c0 10 osd.1 ep: 40 scrubber::ReplicaReservations pg[1.0s0>]: handle_reserve_grant: granted by 5(5) (5 of 5) in 0ms
2023-09-27T06:14:11.945-0500 7fc93c14c6c0 10 osd.1 ep: 40 scrubber::ReplicaReservations pg[1.0s0>]: handle_reserve_grant: osd.5(5) scrub reserve = success
2023-09-27T06:14:11.945-0500 7fc93c14c6c0 15 osd.1 40 queue a scrub event (PGScrubResourcesOK(pgid=1.0s0epoch_queued=40 scrub-token=0)) for pg[1.0s0( v 40'41 (0'0,40'41] local-lis/les=37/38 n=41 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [1,0,2,4,3,5]p1(0) r=0 lpr=37 crt=40'41 lcod 40'40 mlcod 40'40 active+clean+scrubbing+deep [ 1.0s0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB]. Epoch: 40
2023-09-27T06:14:11.945-0500 7fc93c14c6c0 20 osd.1 op_wq(0) _enqueue OpSchedulerItem(1.0s0> PGScrubResourcesOK(pgid=1.0s0> epoch_queued=40 scrub-token=0) class_id 1 prio 120 cost 52428800 e40)
2023-09-27T06:14:11.945-0500 7fc93c14c6c0 10 osd.1 40 dequeue_op MOSDScrubReserve(1.0s0 GRANT e40) v1 finish
2023-09-27T06:14:11.945-0500 7fc93c14c6c0 20 osd.1 op_wq(0) _process 1.0s0 to_process <> waiting <> waiting_peering {}
2023-09-27T06:14:11.945-0500 7fc93c14c6c0 20 osd.1 op_wq(0) _process OpSchedulerItem(1.0s0 PGScrubResourcesOK(pgid=1.0s0epoch_queued=40 scrub-token=0) class_id 1 prio 120 cost 52428800 e40) queued
2023-09-27T06:14:11.945-0500 7fc93c14c6c0 20 osd.1 op_wq(0) _process 1.0s0 to_process <OpSchedulerItem(1.0s0 PGScrubResourcesOK(pgid=1.0s0epoch_queued=40 scrub-token=0) class_id 1 prio 120 cost 52428800 e40)> waiting <> waiting_peering {}
2023-09-27T06:14:11.945-0500 7fc93c14c6c0 20 osd.1 op_wq(0) _process OpSchedulerItem(1.0s0 PGScrubResourcesOK(pgid=1.0s0epoch_queued=40 scrub-token=0) class_id 1 prio 120 cost 52428800 e40) pg 0x616c000
2023-09-27T06:14:11.945-0500 7fc9401546c0 20 osd.1 op_wq(0) _process empty q, waiting
2023-09-27T06:14:11.945-0500 7fc93c14c6c0 20 osd.1 pg_epoch: 40 pg[1.0s0( v 40'41 (0'0,40'41] local-lis/les=37/38 n=41 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [1,0,2,4,3,5]p1(0) r=0 lpr=37 crt=40'41 lcod 40'40 mlcod 40'40 active+clean+scrubbing+deep [ 1.0s0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] forward_scrub_event: RemotesReserved queued at: 40
2023-09-27T06:14:11.945-0500 7fc93c14c6c0 10 osd.1 pg_epoch: 40 pg[1.0s0( v 40'41 (0'0,40'41] local-lis/les=37/38 n=41 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [1,0,2,4,3,5]p1(0) r=0 lpr=37 crt=40'41 lcod 40'40 mlcod 40'40 active+clean+scrubbing+deep [ 1.0s0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] scrubber<ReservingReplicas>: scrubber event -->> send_remotes_reserved epoch: 40
2023-09-27T06:14:11.945-0500 7fc93c14c6c0 20  scrubberFSM  event: --vvvv---- RemotesReserved
2023-09-27T06:14:11.945-0500 7fc93c14c6c0 10 osd.1 pg_epoch: 40 pg[1.0s0( v 40'41 (0'0,40'41] local-lis/les=37/38 n=41 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [1,0,2,4,3,5]p1(0) r=0 lpr=37 crt=40'41 lcod 40'40 mlcod 40'40 active+clean+scrubbing+deep [ 1.0s0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] scrubber<ActiveScrubbing>: FSM: -- state -->> ActiveScrubbing
2023-09-27T06:14:11.945-0500 7fc93c14c6c0 20 osd.1 pg_epoch: 40 pg[1.0s0( v 40'41 (0'0,40'41] local-lis/les=37/38 n=41 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [1,0,2,4,3,5]p1(0) r=0 lpr=37 crt=40'41 lcod 40'40 mlcod 40'40 active+clean+scrubbing+deep [ 1.0s0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] PeeringState::prepare_stats_for_publish reporting purged_snaps []
2023-09-27T06:14:11.945-0500 7fc93c14c6c0 15 osd.1 pg_epoch: 40 pg[1.0s0( v 40'41 (0'0,40'41] local-lis/les=37/38 n=41 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [1,0,2,4,3,5]p1(0) r=0 lpr=37 crt=40'41 lcod 40'40 mlcod 40'40 active+clean+scrubbing+deep [ 1.0s0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] PeeringState::prepare_stats_for_publish publish_stats_to_osd 40:102
2023-09-27T06:13:37.595-0500 7f2d1baf76c0 20 osd.6 op_wq(7) _process OpSchedulerItem(1.fs0 PGScrubScrubFinished(pgid=1.fs0epoch_queued=40 scrub-token=0) class_id 1 prio 120 cost 52428800 e40) queued
2023-09-27T06:13:37.595-0500 7f2d1baf76c0 20 osd.6 op_wq(7) _process 1.fs0 to_process <OpSchedulerItem(1.fs0 PGScrubScrubFinished(pgid=1.fs0epoch_queued=40 scrub-token=0) class_id 1 prio 120 cost 52428800 e40)> waiting <> waiting_peering {}
2023-09-27T06:13:37.595-0500 7f2d1baf76c0 20 osd.6 op_wq(7) _process OpSchedulerItem(1.fs0 PGScrubScrubFinished(pgid=1.fs0epoch_queued=40 scrub-token=0) class_id 1 prio 120 cost 52428800 e40) pg 0x58ce000
2023-09-27T06:13:37.595-0500 7f2d1baf76c0 20 osd.6 pg_epoch: 40 pg[1.fs0( v 40'31 (0'0,40'31] local-lis/les=37/38 n=31 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [6,0,2,3,4,1]p6(0) r=0 lpr=37 crt=40'31 lcod 40'30 mlcod 40'30 active+clean+scrubbing+deep [ 1.fs0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] forward_scrub_event: ScrubFinished queued at: 40
2023-09-27T06:13:37.595-0500 7f2d1baf76c0 10 osd.6 pg_epoch: 40 pg[1.fs0( v 40'31 (0'0,40'31] local-lis/les=37/38 n=31 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [6,0,2,3,4,1]p6(0) r=0 lpr=37 crt=40'31 lcod 40'30 mlcod 40'30 active+clean+scrubbing+deep [ 1.fs0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] scrubber<Act/WaitDigestUpdate>: scrubber event -->> send_scrub_is_finished epoch: 40
2023-09-27T06:13:37.595-0500 7f2d1baf76c0 20  scrubberFSM  event: --vvvv---- ScrubFinished
2023-09-27T06:13:37.595-0500 7f2d1baf76c0 10 osd.6 pg_epoch: 40 pg[1.fs0( v 40'31 (0'0,40'31] local-lis/les=37/38 n=31 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [6,0,2,3,4,1]p6(0) r=0 lpr=37 crt=40'31 lcod 40'30 mlcod 40'30 active+clean+scrubbing+deep [ 1.fs0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] scrubber<Act/WaitDigestUpdate>: FSM: WaitDigestUpdate::react(const ScrubFinished&)
2023-09-27T06:13:37.595-0500 7f2d1baf76c0 20 osd.6 pg_epoch: 40 pg[1.fs0( v 40'31 (0'0,40'31] local-lis/les=37/38 n=31 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [6,0,2,3,4,1]p6(0) r=0 lpr=37 crt=40'31 lcod 40'30 mlcod 40'30 active+clean+scrubbing+deep [ 1.fs0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] PeeringState::prepare_stats_for_publish reporting purged_snaps []
2023-09-27T06:13:37.595-0500 7f2d1baf76c0 15 osd.6 pg_epoch: 40 pg[1.fs0( v 40'31 (0'0,40'31] local-lis/les=37/38 n=31 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [6,0,2,3,4,1]p6(0) r=0 lpr=37 crt=40'31 lcod 40'30 mlcod 40'30 active+clean+scrubbing+deep [ 1.fs0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] PeeringState::prepare_stats_for_publish publish_stats_to_osd 40:63
2023-09-27T06:13:37.595-0500 7f2d1baf76c0 10 osd.6 pg_epoch: 40 pg[1.fs0( v 40'31 (0'0,40'31] local-lis/les=37/38 n=31 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [6,0,2,3,4,1]p6(0) r=0 lpr=37 crt=40'31 lcod 40'30 mlcod 40'30 active+clean+scrubbing+deep [ 1.fs0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] scrubber<Act/WaitDigestUpdate>: scrub_finish before flags:  REQ_SCRUB. repair state: no-repair. deep_scrub_on_error: 0
2023-09-27T06:13:37.595-0500 7f2d1faff6c0 20 osd.6 op_wq(7) _process empty q, waiting
2023-09-27T06:13:37.595-0500 7f2d1baf76c0 15 osd.6 pg_epoch: 40 pg[1.fs0( v 40'31 (0'0,40'31] local-lis/les=37/38 n=31 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [6,0,2,3,4,1]p6(0) r=0 lpr=37 crt=40'31 lcod 40'30 mlcod 40'30 active+clean+scrubbing+deep [ 1.fs0:  REQ_SCRUB ] ] scrubber<Act/WaitDigestUpdate>:  b.e.: update_repair_status: repair state set to :false
2023-09-27T06:13:37.595-0500 7f2d1baf76c0 10 osd.6 pg_epoch: 40 pg[1.fs0( v 40'31 (0'0,40'31] local-lis/les=37/38 n=31 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [6,0,2,3,4,1]p6(0) r=0 lpr=37 crt=40'31 lcod 40'30 mlcod 40'30 active+clean+scrubbing+deep [ 1.fs0:  REQ_SCRUB ] ] scrubber<Act/WaitDigestUpdate>: _scrub_finish info stats: valid m_is_repair: 0
2023-09-27T06:13:37.595-0500 7f2d1baf76c0 10 osd.6 pg_epoch: 40 pg[1.fs0( v 40'31 (0'0,40'31] local-lis/les=37/38 n=31 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [6,0,2,3,4,1]p6(0) r=0 lpr=37 crt=40'31 lcod 40'30 mlcod 40'30 active+clean+scrubbing+deep [ 1.fs0:  REQ_SCRUB ] ] scrubber<Act/WaitDigestUpdate>: deep-scrub got 31/31 objects, 0/0 clones, 31/31 dirty, 0/0 omap, 0/0 pinned, 0/0 hit_set_archive, 63488/63488 bytes, 0/0 manifest objects, 0/0 hit_set_archive bytes.
2023-09-27T06:13:37.595-0500 7f2d1baf76c0  0 log_channel(cluster) log [DBG] : 1.f deep-scrub ok
2023-09-27T06:13:37.595-0500 7f2d1baf76c0 10 osd.6 pg_epoch: 40 pg[1.fs0( v 40'31 (0'0,40'31] local-lis/les=37/38 n=31 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [6,0,2,3,4,1]p6(0) r=0 lpr=37 crt=40'31 lcod 40'30 mlcod 40'30 active+clean+scrubbing+deep [ 1.fs0:  REQ_SCRUB ] ] scrubber<Act/WaitDigestUpdate>: m_pg->recovery_state.update_stats() errors:0/0 deep? 1
2023-09-27T06:13:37.595-0500 7f2d1baf76c0 19 osd.6 pg_epoch: 40 pg[1.fs0( v 40'31 (0'0,40'31] local-lis/les=37/38 n=31 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [6,0,2,3,4,1]p6(0) r=0 lpr=37 crt=40'31 lcod 40'30 mlcod 40'30 active+clean+scrubbing+deep [ 1.fs0:  REQ_SCRUB ] ] scrubber<Act/WaitDigestUpdate>: scrub_finish shard 6(0) num_omap_bytes = 0 num_omap_keys = 0
2023-09-27T06:13:37.595-0500 7f2d1baf76c0 20 osd.6 pg_epoch: 40 pg[1.fs0( v 40'31 (0'0,40'31] local-lis/les=37/38 n=31 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [6,0,2,3,4,1]p6(0) r=0 lpr=37 crt=40'31 lcod 40'30 mlcod 40'30 active+clean+scrubbing+deep [ 1.fs0:  REQ_SCRUB ] ] PeeringState::prepare_stats_for_publish reporting purged_snaps []
2023-09-27T06:13:37.595-0500 7f2d1baf76c0 15 osd.6 pg_epoch: 40 pg[1.fs0( v 40'31 (0'0,40'31] local-lis/les=37/38 n=31 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [6,0,2,3,4,1]p6(0) r=0 lpr=37 crt=40'31 lcod 40'30 mlcod 40'30 active+clean+scrubbing+deep [ 1.fs0:  REQ_SCRUB ] ] PeeringState::prepare_stats_for_publish publish_stats_to_osd 40:64
2023-09-27T06:13:37.595-0500 7f2d1baf76c0 10 log is not dirty
2023-09-27T06:13:34.787-0500 7fc9529796c0 20 osd.1 osd-scrub:initiate_scrub: initiating scrub on pg[1.es0>]
2023-09-27T06:13:37.596-0500 7f2d1baf76c0 10 osd.6 pg_epoch: 40 pg[1.fs0( v 40'31 (0'0,40'31] local-lis/les=37/38 n=31 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [6,0,2,3,4,1]p6(0) r=0 lpr=37 crt=40'31 lcod 40'30 mlcod 40'30 active+clean+scrubbing+deep [ 1.fs0:  REQ_SCRUB ] ] scrubber<Act/WaitDigestUpdate>: update_scrub_job: flags:<(plnd:)>
2023-09-27T06:13:48.063-0500 7fc93c14c6c0 20  scrubberFSM  event: --vvvv---- StartScrub
2023-09-27T06:13:48.063-0500 7fc93c14c6c0 10 osd.1 pg_epoch: 40 pg[1.0s0( v 40'41 (0'0,40'41] local-lis/les=37/38 n=41 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [1,0,2,4,3,5]p1(0) r=0 lpr=37 crt=40'41 lcod 40'40 mlcod 40'40 active+clean+scrubbing+deep [ 1.0s0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] scrubber<NotActive>: FSM: NotActive::react(const StartScrub&)
2023-09-27T06:13:48.063-0500 7fc93c14c6c0  0 log_channel(cluster) log [DBG] : 1.0 deep-scrub starts
2023-09-27T06:13:48.063-0500 7fc93c14c6c0 10 osd.1 pg_epoch: 40 pg[1.0s0( v 40'41 (0'0,40'41] local-lis/les=37/38 n=41 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [1,0,2,4,3,5]p1(0) r=0 lpr=37 crt=40'41 lcod 40'40 mlcod 40'40 active+clean+scrubbing+deep [ 1.0s0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] scrubber<ReservingReplicas>: FSM: -- state -->> ReservingReplicas
2023-09-27T06:13:48.063-0500 7fc93c14c6c0 10 osd.1 pg_epoch: 40 pg[1.0s0( v 40'41 (0'0,40'41] local-lis/les=37/38 n=41 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [1,0,2,4,3,5]p1(0) r=0 lpr=37 crt=40'41 lcod 40'40 mlcod 40'40 active+clean+scrubbing+deep [ 1.0s0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] scrubber<ReservingReplicas>: reserve_replicas
2023-09-27T06:13:48.063-0500 7fc93c14c6c0 10 osd.1 ep: 40 scrubber::ReplicaReservations pg[1.0s0>]: ReplicaReservations: acting: [0(1), 2(2), 3(4), 4(3), 5(5)]
2023-09-27T06:13:48.063-0500 7fc93c14c6c0 20 osd.1 40 get_con_osd_cluster to osd.0 from_epoch 40
2023-09-27T06:13:48.063-0500 7fc93c14c6c0 20 osd.1 40 get_nextmap_reserved map_reservations: {40=1}
2023-09-27T06:13:48.063-0500 7fc93c14c6c0 20 osd.1 40 release_map epoch: 40
2023-09-27T06:13:48.063-0500 7fc93c14c6c0  1 -- [v2:127.0.0.1:6812/3073983952,v1:127.0.0.1:6813/3073983952] --> [v2:127.0.0.1:6804/3392379165,v1:127.0.0.1:6805/3392379165] -- MOSDScrubReserve(1.0s1 REQUEST e40) v1 -- 0x6d03ce0 con 0x5dea800
2023-09-27T06:13:48.063-0500 7fc93c14c6c0 10 osd.1 ep: 40 scrubber::ReplicaReservations pg[1.0s0>]: send_request_to_replica: reserving 0(1) (1 of 5)
2023-09-27T06:13:48.063-0500 7fc93c14c6c0 20  scrubberFSM  event: --^^^^---- StartScrub
2023-09-27T06:13:48.063-0500 7fc93c14c6c0 10 osd.1 pg_epoch: 40 pg[1.0s0( v 40'41 (0'0,40'41] local-lis/les=37/38 n=41 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [1,0,2,4,3,5]p1(0) r=0 lpr=37 crt=40'41 lcod 40'40 mlcod 40'40 active+clean+scrubbing+deep [ 1.0s0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] scrubber<ReservingReplicas>: scrubber event --<< StartScrub
2023-09-27T06:13:48.063-0500 7fc93c14c6c0 20 osd.1 op_wq(0) _process empty q, waiting
2023-09-27T06:13:48.064-0500 7fc9569816c0  1 -- [v2:127.0.0.1:6812/3073983952,v1:127.0.0.1:6813/3073983952] <== osd.0 v2:127.0.0.1:6804/3392379165 471 ==== MOSDScrubReserve(1.0s0 REJECT e40) v1 ==== 43+0+0 (unknown 0 0 0) 0x6d03ce0 con 0x5dea800
2023-09-27T06:13:48.064-0500 7fc9569816c0 15 osd.1 40 enqueue_op MOSDScrubReserve(1.0s0 REJECT e40) v1 prio 127 type 92 cost 0 latency 0.000049 epoch 40 MOSDScrubReserve(1.0s0 REJECT e40) v1
2023-09-27T06:13:48.064-0500 7fc9569816c0 20 osd.1 op_wq(0) _enqueue OpSchedulerItem(1.0s0> PGOpItem(op=MOSDScrubReserve(1.0s0 REJECT e40) v1) class_id 2 prio 127 cost 0 e40)
2023-09-27T06:13:48.064-0500 7fc93c14c6c0 20 osd.1 op_wq(0) _process 1.0s0 to_process <> waiting <> waiting_peering {}
2023-09-27T06:13:48.064-0500 7fc93c14c6c0 20 osd.1 op_wq(0) _process OpSchedulerItem(1.0s0 PGOpItem(op=MOSDScrubReserve(1.0s0 REJECT e40) v1) class_id 2 prio 127 cost 0 e40) queued
2023-09-27T06:13:48.064-0500 7fc93c14c6c0 20 osd.1 op_wq(0) _process 1.0s0 to_process <OpSchedulerItem(1.0s0 PGOpItem(op=MOSDScrubReserve(1.0s0 REJECT e40) v1) class_id 2 prio 127 cost 0 e40)> waiting <> waiting_peering {}
2023-09-27T06:13:48.064-0500 7fc93c14c6c0 20 osd.1 op_wq(0) _process OpSchedulerItem(1.0s0 PGOpItem(op=MOSDScrubReserve(1.0s0 REJECT e40) v1) class_id 2 prio 127 cost 0 e40) pg 0x616c000
2023-09-27T06:13:48.064-0500 7fc9401546c0 20 osd.1 op_wq(0) _process empty q, waiting
2023-09-27T06:13:48.064-0500 7fc93c14c6c0 10 osd.1 40 dequeue_op MOSDScrubReserve(1.0s0 REJECT e40) v1 prio 127 cost 0 latency 0.000223 MOSDScrubReserve(1.0s0 REJECT e40) v1 pg pg[1.0s0( v 40'41 (0'0,40'41] local-lis/les=37/38 n=41 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [1,0,2,4,3,5]p1(0) r=0 lpr=37 crt=40'41 lcod 40'40 mlcod 40'40 active+clean+scrubbing+deep [ 1.0s0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB]
2023-09-27T06:13:48.064-0500 7fc93c14c6c0 10 osd.1 40 maybe_share_map: con v2:127.0.0.1:6804/3392379165 our osdmap epoch of 40 is not newer than session's projected_epoch of 40
2023-09-27T06:13:48.064-0500 7fc93c14c6c0 10 osd.1 pg_epoch: 40 pg[1.0s0( v 40'41 (0'0,40'41] local-lis/les=37/38 n=41 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [1,0,2,4,3,5]p1(0) r=0 lpr=37 crt=40'41 lcod 40'40 mlcod 40'40 active+clean+scrubbing+deep [ 1.0s0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] _handle_message: MOSDScrubReserve(1.0s0 REJECT e40) v1
2023-09-27T06:13:48.064-0500 7fc93c14c6c0 10 osd.1 pg_epoch: 40 pg[1.0s0( v 40'41 (0'0,40'41] local-lis/les=37/38 n=41 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [1,0,2,4,3,5]p1(0) r=0 lpr=37 crt=40'41 lcod 40'40 mlcod 40'40 active+clean+scrubbing+deep [ 1.0s0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] scrubber<ReservingReplicas>: handle_scrub_reserve_reject MOSDScrubReserve(1.0s0 REJECT e40) v1
2023-09-27T06:13:48.064-0500 7fc93c14c6c0 10 osd.1 ep: 40 scrubber::ReplicaReservations pg[1.0s0>]: handle_reserve_reject: rejected by 0(1) (MOSDScrubReserve(1.0s0 REJECT e40) v1)
2023-09-27T06:13:48.064-0500 7fc93c14c6c0 10 osd.1 ep: 40 scrubber::ReplicaReservations pg[1.0s0>]: handle_reserve_reject: osd.0(1) scrub reserve = fail
2023-09-27T06:13:48.064-0500 7fc93c14c6c0 10 osd.1 ep: 40 scrubber::ReplicaReservations pg[1.0s0>]: release_all: releasing []
2023-09-27T06:13:48.064-0500 7fc93c14c6c0 15 osd.1 40 queue a scrub event (PGScrubDenied(pgid=1.0s0epoch_queued=40 scrub-token=0)) for pg[1.0s0( v 40'41 (0'0,40'41] local-lis/les=37/38 n=41 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [1,0,2,4,3,5]p1(0) r=0 lpr=37 crt=40'41 lcod 40'40 mlcod 40'40 active+clean+scrubbing+deep [ 1.0s0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB]. Epoch: 40
2023-09-27T06:13:48.064-0500 7fc93c14c6c0 20 osd.1 op_wq(0) _enqueue OpSchedulerItem(1.0s0> PGScrubDenied(pgid=1.0s0> epoch_queued=40 scrub-token=0) class_id 1 prio 120 cost 52428800 e40)
2023-09-27T06:13:48.064-0500 7fc93c14c6c0 10 osd.1 40 dequeue_op MOSDScrubReserve(1.0s0 REJECT e40) v1 finish
2023-10-27T06:13:48.064-0500 7fc93c14c6c0 10 osd.1 40 dequeue_op MOSDScrubReserve(1.0s0 REJECT e40) v1 prio 127 cost 0 latency 0.000223 MOSDScrubReserve(1.0s0 REJECT e40) v1 pg pg[1.0s0( v 40'41 (0'0,40'41] local-lis/les=37/38 n=41 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [1,0,2,4,3,5]p1(0) r=0 lpr=37 crt=40'41 lcod 40'40 mlcod 40'40 active+clean+scrubbing+deep [ 1.0s0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB]
2023-01-27T06:13:48.064-0500 7fc93c14c6c0 10 osd.1 40 dequeue_op MOSDScrubReserve(1.0s0 REJECT e40) v1 prio 127 cost 0 latency 0.000223 MOSDScrubReserve(1.0s0 REJECT e40) v1 pg pg[1.0s0( v 40'41 (0'0,40'41] local-lis/les=37/38 n=41 ec=37/37 lis/c=37/37 les/c/f=38/38/0 sis=37) [1,0,2,4,3,5]p1(0) r=0 lpr=37 crt=40'41 lcod 40'40 mlcod 40'40 active+clean+scrubbing [ 1.0s0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB]
2023-09-27T06:13:35.818-0500 7fc9529796c0 20 osd.1 osd-scrub:log_fwd: can_inc_scrubs== false. 1 (local) + 1 (remote) >= max (2)
'''


ev_log = [ ('dummy', '2000-09-27T02:16:55.598469-0500', 0, '1.0', 'd', '') ]

def test_data_run() -> None:
  i1 = testdt
  for x in i1.split('\n'):
    l1a = log_line_parse(x)
    if l1a:
          print(f'   {l1a}')
          ev_log.append(l1a)
    else:
          print(f'   no match')

  print_ev_log(ev_log)


def run_from_file(fn) -> None:
  with open(fn, 'r') as f1:
    for x in f1:
      l1a = log_line_parse(x)
      if l1a:
        print(f'   {l1a}')
        ev_log.append(l1a)
      else:
        print(f'   no match')

  print_ev_log(ev_log)

run_from_file('/tmp/o2.log')
