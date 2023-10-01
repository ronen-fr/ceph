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

# the events log:
# list of:
# ( ev-type, time, osd, pg, d/s, duration, ok/failure, extra content )
# ev-type: 
#    - 'scrub-requested', 'deep-scrub-requested'
#    - 'scrub-started', 'deep-scrub-started'
#    - 'scrub-completed', 'deep-scrub-completed'
#    - 'resrv-started', 'resrv-completed', 'resrv-failed', 'local-inc-failed'
#    - 'scrub-time'
ev_log = [ ('dummy', '2020-09-27T02:16:55.598469', 0, '1.0', 's', '0.000', 't', '') ]

def dump_ev_log(fn):
   with open(fn, 'w') as ev_fd:
        json.dump(ev_log, ev_fd)

fn='pgs.json'
trans_cmd_time={}

# returns an up-to-date dictionary of scrub stamps
# (frm is - only for 1st dev step - a filename)
#def upd_stamps(frm, is_v):
def upd_stamps(is_v):
  by_pg = {}
  ceph_dump_p = subprocess.Popen(["ceph", "--format", "json", "pg", "dump", "pgs"], close_fds=True, stdout=subprocess.PIPE)
  # for dev:
  #ceph_dump_p = subprocess.Popen(["cat", frm], close_fds=True, stdout=subprocess.PIPE)
  pgs_full = json.load(ceph_dump_p.stdout)
  pgsts = pgs_full['pg_stats']
  ot, er = ceph_dump_p.communicate()
  #print(pgsts)
  for x in pgsts:
      pgid = x['pgid']
      lstmpS = x['last_scrub_stamp']
      lstmpD = x['last_deep_scrub_stamp']
      lstmp = lstmpS if lstmpS > lstmpD else lstmpD
      if is_v:
        print(f'   stamps: {pgid}: {lstmpS} / {lstmpD} -> {lstmp}')
      #z=( x['pgid'], x['last_scrub_stamp']
      by_pg[pgid] = [ lstmp ]
  return by_pg

def f1():
  by_pg = {}
  by_pg_cnt = {}
  with open(fn, 'r') as pgs_fd:
    pgs_full = json.load(pgs_fd)
    pgsts = pgs_full['pg_stats']
    for x in pgsts:
      pgid = x['pgid']
      lstmp = x['last_scrub_stamp']
      print(pgid, x['last_scrub_stamp']) 
      #z=( x['pgid'], x['last_scrub_stamp']
      by_pg[pgid] = [ lstmp ]
      by_pg_cnt[pgid] = 0

  for k, v in by_pg_cnt.items():
    print(k, v)

  for k, v in by_pg_cnt.items():
    #by_pg[k] = [ by_pg[k], '17' ]
    #by_pg[k] = by_pg[k].append('99')
    print(by_pg[k])
    t1 = by_pg[k]
    t1.append('99')
    print('t1->', t1)
    by_pg[k] = t1
    print(by_pg[k])

  for k, v in by_pg.items():
    print(k, v)


# instruct a PG to scrub
def scrub_pg(p, is_deep):
    scmd = 'deep-scrub' if is_deep else 'scrub'
    pr = subprocess.run(['ceph', 'pg', scmd, p],timeout=3, check=True) 
    trans_cmd_time[p] = datetime.datetime.now().isoformat()
    ev_log.append(('scrub-requested', trans_cmd_time[p], -1, p, 'd' if is_deep else 's', '0.000', 't', ''))


# return list of PGs for which the stamp changed
def updated_pgs(prev_data, is_v):
    ret=[]
    upd = upd_stamps(True)
    for k, v in upd.items():
        prev_stamp = prev_data[k] if k in prev_data else '0'
        if v > prev_stamp:
            if is_v:
              print(f'  for {k} changed from {prev_stamp} to {v}')
            ret.append([k, v])
    return ret


# fetch an update re the scrub stamps. For updated stamps:
# - update the scrubs count for the specific PG
# - if count < max: initiate another scrub
# - count the number of 'complete' PGs
def step_update(num_scrubs, prev, cnts, is_deep, is_v):
  upd = updated_pgs(prev, True)
  print (f'step_update: {len(upd)} updates')
  if len(upd) > 0:
    for kv in upd:
      prev[kv[0]] = kv[1]
      prev_cnt = cnts[kv[0]] if kv[0] in cnts else 0
      upd_cnt = prev_cnt+1
      cnts[kv[0]] = upd_cnt
      print(f' updated count for {kv[0]} to {cnts[kv[0]]}')
      if (upd_cnt < num_scrubs) :
          scrub_pg(kv[0], is_deep)

  # how many scrubs left before completing?
  #compl = sum(v for k, v in cnts.items() if k in name)
  compl = sum(v for k, v in cnts.items())
  return compl

# run until we reach the needed number of scrubs
def steps(max_iter, repeats, num_scrubs, prev, cnts, is_deep, is_v):
  fn = 'test.json'
  while max_iter > 0:
    print(f'\n++++++++++++++++++++++')
    max_iter = max_iter + 1
    fn = fn[:-5]+'_.json'
    already_run = step_update(repeats, prev, cnts, is_deep, is_v)
    print(f'===> {already_run} completed thus far. Runs table:')
    for k, v in cnts.items():
      print(f'\t\t{k} run {v} times')
    if already_run >= num_scrubs:
      print(f' done running {already_run} scrubs');
      break
    for k_pg, v_tm in trans_cmd_time.items():
      print(f' {k_pg} requested at {v_tm}')
    time.sleep(1)


##############################################

parser = argparse.ArgumentParser(description='initiate N rounds of scrubs')
parser.add_argument('-v', '--verbose', action='store_true')
parser.add_argument('--repeat', help='number of scrubs per each PG', type=int, default=3)
parser.add_argument('-t', '--pool-type', help='pool type (ec or replicated)', default='replicated')
parser.add_argument('-o', '--outlog', help='log file', default='ev_log.json')
#parser.add_argument('infile', type=argparse.FileType('r'))
#parser.add_argument('-p', '--pgs', help='relevant PGs', nargs='*')
args = parser.parse_args()

cnt = {}
repeats = args.repeat
bp = upd_stamps(True)
for k, v in bp.items():
    print (k, v)
    cnt[k] = 0
scrubs_needed = repeats * len(bp)
print(f'Total number of scrubs expected: {scrubs_needed}')

#do we sort times correctly?
sbp = dict(sorted(bp.items(), key=lambda item: item[1]))
for k, v in sbp.items():
    print ('sbp ',k, v)

# time?
for k in bp.keys():
    scrub_pg(k, True)

steps(30, repeats, scrubs_needed, bp, cnt, True, args.verbose)
ev_log.append(('test-done', datetime.datetime.now().isoformat(), -1, '0.0', 's', '0.000', 't', ''))
dump_ev_log(args.outlog)




# drafts:

#already_run = step_update(2, bp, cnt, False, 'upd1.json')
#print(f'already run: {already_run}')
#for k, v in cnt.items():
#    print ('u cnts ',k, v)

#already_run = step_update(2, bp, cnt, False, 'upd2.json')
#print(f'already run: {already_run}')
#for k, v in cnt.items():
#    print ('u cnts ',k, v)


#upd1 = updated_pgs(bp, 'upd1.json')
#for kv in upd1:
#    print(f'\tupd1: {kv[0]} : {kv[1]}')

    
# c=b['d'] if 'd' in b else 17
