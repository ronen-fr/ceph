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
#import pandas as pd
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

# reservation process started:
resv_started_ptrn_txt = r'pg\[(?P<pgid>[0-9]+.[a-f0-9]+).*scrubbing(?P<is_deep>\+deep)*.*scrubber<ReservingReplicas>: reserve_replicas'
resv_started_ptrn = re.compile(resv_started_ptrn_txt)

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

# # --------------------------------------------------------------------------
# # --------------------------------------------------------------------------

# # ----- the events log:


def print_ev_log(ev_log, hdr_msg):
  print(f'\n\n{hdr_msg}')
  for x in ev_log:
    print(x)


def evlog_to_csv(ev_log, ofn):
   with open(ofn+'.csv', 'w') as ofd:
     for x in ev_log:
       ofd.write(f'{x[0]},{x[1]},{x[2]},{x[3]},{x[4]},{x[5]},{x[6]},{x[7]}\n')


# def collect_durations(ev_log):
#   durs_temp = {}
#   res = []
#   for ev, dt_str, osdn, pgn, ds, dur_in, tf_in, rst in ev_log:
#     dt = datetime.datetime.fromisoformat(dt_str)
#     if ev == 'scrub-initiated':
#       #as_dt = datetime.datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S.%f')
#       durs_temp[pgn] = dt
# 
#     elif ev == 'rep-reserved' and pgn in durs_temp:
#       df : datetime.timedelta = dt - durs_temp[pgn]
#       #durs_temp[pgn] = df.total_seconds()
#       print(f'   r+:{pgn} {df.total_seconds()}')
#       res.append(('resv-duration', dt_str, osdn, pgn, ds, f'{df.total_seconds()}', 't', ' '))
# 
#     elif ev == 'reject-by' and pgn in durs_temp:
#       print(f'==-> pgn: {pgn}')
#       if pgn in durs_temp:
#         print(f'==-> dur_temp: {durs_temp[pgn]}')
#       df : datetime.timedelta = dt - durs_temp[pgn]
#       print(f'   r-:{pgn} {df.total_seconds()}')
#       res.append(('resv-duration', dt_str, osdn, pgn, ds, f'{df.total_seconds()}', 'f', ' '))
# 
#   return res

# times from 'reserve_replicas' to 'send_scrub_is_finished'
def collect_total_durations(ev_log, is_v):
  durs_temp = {}
  res = []
  for ev, dt_str, osdn, pgn, ds, dur_in, tf_in, rst in ev_log:
    dt = datetime.datetime.fromisoformat(dt_str)
    if ev == 'resrv-started':
      #as_dt = datetime.datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S.%f')
      durs_temp[pgn] = (dt, ds)

    elif ev == 'rep-reserved' and pgn in durs_temp:
      df : datetime.timedelta = dt - durs_temp[pgn][0]
      #durs_temp[pgn] = df.total_seconds()
      if is_v:
        print(f'   r+:{pgn} {df.total_seconds()}')
      res.append(('resv-duration', dt_str, osdn, pgn, ds, f'{df.total_seconds()}', 't', ' '))

    elif ev == 'reject-by' and pgn in durs_temp:
      if is_v:
        print(f'==-> pgn: {pgn}')
        if pgn in durs_temp:
          print(f'==-> dur_temp: {durs_temp[pgn]}')
      df : datetime.timedelta = dt - durs_temp[pgn][0]
      if is_v:
        print(f'   r-:{pgn} {df.total_seconds()}')
      res.append(('resv-duration', dt_str, osdn, pgn, ds, f'{df.total_seconds()}', 'f', ' '))

    elif ev == 'scrub-completed' and pgn in durs_temp:
      if is_v:
        print(f'==-> pgn: {pgn}')
        if pgn in durs_temp:
          print(f'==-> dur_temp: {durs_temp[pgn]}')
      df : datetime.timedelta = dt - durs_temp[pgn][0]
      ds = durs_temp[pgn][1]
      if is_v:
        print(f'   *+:{pgn} {df.total_seconds()}')
      res.append(('scrub-duration', dt_str, osdn, pgn, ds, f'{df.total_seconds()}', 'f', ' '))
  return res


# # --------------------------------------------------------------------------
# # --------------------------------------------------------------------------



def basic_log_parse(line, is_v) -> tuple((str, str, str)):
  #print(f'basic_log_parse():  {line}')
  m = regline_ptrn.search(line)
  if m:
    if is_v:
       print(f'   {m.group(1)}//{m.group(2)}//{m.group(3)}/    <{m.group("osdn")}> - {m.group("rest")}')
    return (m.group(1), m.group("osdn"), m.group("rest"))
  else:
    if is_v:
       print(f'   no match\n\n')
    return None


def log_line_parse(line, is_v) -> tuple((str, str, str, str, str, ...)):
  if is_v:
    print(f'log_line_parse():  {line}')
  hdr = basic_log_parse(line, is_v)
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
       if is_v:
          print(f'   {pgid}')
       return ('scrub-initiated', dt, osd, pgid, 'x', '0.000', 't', rst)

     m = resv_started_ptrn.search(rst)
     if m:
       pgid = m.group("pgid")
       deep = 'd' if m.group("is_deep") else 's'
       if is_v:
          print(f'   {pgid}/{deep}')
       return ('resrv-started', dt, osd, pgid, deep, '0.000', 't', ' ')

     m = reject_by_ptrn.search(rst)
     if m:
       pgid = m.group("pgid")
       from_osd = m.group("from_osd")
       from_rep = m.group("from_rep")
       epoch = m.group("epoch")
       if is_v:
          print(f'    from osd.{from_osd}: rep={from_rep} epoch={epoch}')
       return ('reject-by', dt, osd, pgid, 'x', '0.000', 'f', f'from osd.{from_osd}: rep={from_rep} epoch={epoch}')

     m = frjct_by_ptrn.search(rst)
     if m:
       pgid = m.group("pgid")
       epoch = m.group("epoch")
       deep = 'd' if m.group("is_deep") else 's'
       if is_v:
          print(f'    epoch={epoch} <<{m.group("is_deep")}>>')
       return ('reject-by', dt, osd, pgid, deep, '0.000', 'f', f'epoch={epoch}')

     m = resrvd_ptrn.search(rst)
     if m:
       pgid = m.group("pgid")
       deep = 'd' if m.group("is_deep") else 's'
       if is_v:
          print(f'   reserved {pgid} {deep}')
       return ('rep-reserved', dt, osd, pgid, deep, '0.000', 't', m.group("epoch"))

     m = finished_ptrn.search(rst)
     if m:
       pgid = m.group("pgid")
       if is_v:
          print(f'   {pgid}')
       return ('scrub-completed', dt, osd, pgid, 'x', '0.000', 't', 'epoch='+m.group("epoch"))

     m = inc_failed_ptrn.search(rst)
     if m:
       if is_v:
          print(f'   inc failure: {m.group("local")} + {m.group("remote")} >= {m.group("max")}')
       return ('inc-failed', dt, osd, 'x', 'x', '0.000', 'f', f'{m.group("local")} / {m.group("remote")} / {m.group("max")}')





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
67565:2023-10-01T04:55:53.943-0500 7f62b5df36c0 10 osd.4 pg_epoch: 31 pg[1.5( v 31'45 (0'0,31'45] local-lis/les=28/29 n=45 ec=28/28 lis/c=28/28 les/c/f=29/29/0 sis=28) [4,2,5,0,1,3] r=0 lpr=28 crt=31'45 lcod 31'44 mlcod 31'44 active+clean+scrubbing+deep [ 1.5:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] scrubber<ReservingReplicas>: reserve_replicas

'''

# # --------------------------------------------------------------------------
# # --------------------------------------------------------------------------

ev_log = [ ('dummy', '2000-09-27T02:16:55.598469', 0, '1.0', 'd', '0.000', 't', '') ]

def test_data_run() -> None:
  i1 = testdt
  for x in i1.split('\n'):
    l1a = log_line_parse(x, True)
    if l1a:
          print(f'   {l1a}')
          ev_log.append(l1a)
    else:
          print(f'   no match')

  print_ev_log(ev_log, 'test')


def run_from_file(fn, is_v) -> None:
  with open(fn, 'r') as f1:
    for x in f1:
      l1a = log_line_parse(x, is_v)
      if l1a:
        #print(f'   {l1a}')
        ev_log.append(l1a)
      #else:
      #  print(f'   no match')


def run_from_dir(dn, verbose) -> None:
  for fn in os.listdir(dn):
    if fn.endswith('.log'):
      run_from_file(os.path.join(dn, fn), verbose)


# # ----- "main"

parser = argparse.ArgumentParser(description='collect scrub durations from OSD logs')
parser.add_argument('-v', '--verbose', action='store_true')
parser.add_argument('-d', '--logs-dir', help='osd.*.log directory', default='/tmp/archive/log')
parser.add_argument('--title', help='output header', default='collected logs')
parser.add_argument('--csv', help='csv output file', default='/tmp/evlog_csv')
#parser.add_argument('-h', '--pool-type', help='pool type (ec or replicated)', default='replicated')
parser.add_argument('--prev', type=argparse.FileType('r'), help='json file created by test script')
parser.add_argument('--test', action='store_true', help='run test data')
#parser.add_argument('-p', '--pgs', help='relevant PGs', nargs='*')
args = parser.parse_args()

if args.test:
  test_data_run()
else:
  print(f'Collecting logs from {args.logs_dir}/*osd.log')
  run_from_dir(args.logs_dir, args.verbose)
  if args.prev:
    with open(args.prev.name, 'r') as prev_log:
      prev_ev_log = json.load(prev_log)
    for x in prev_ev_log:
      ev_log.append(x)
      #ev_log.sort(key=lambda x: datetime.datetime.fromisoformat(x[1]))

#print_ev_log(ev_log, 'mid-work')
ev_log.sort(key=lambda x: datetime.datetime.fromisoformat(x[1]))
#to_add = collect_durations(ev_log)
to_add = collect_total_durations(ev_log, args.verbose)
for x in to_add:
  print(f'\tto_add: {x}')
  ev_log.append(x)
  ev_log.sort(key=lambda x: datetime.datetime.fromisoformat(x[1]))

#if args.verbose:
#  print_ev_log(ev_log, args.title)

evlog_to_csv(ev_log, args.csv)
print(f'csv file: {args.csv}')
