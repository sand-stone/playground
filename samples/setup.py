import os,subprocess
num=4
dirs=[]
for i in range(0,num):
    dirs.append('/mnt/db/data'+`i`)
for d in dirs:
    os.system("rm -rf "+d)
    os.system("mkdir "+d)
cmds=[]
c=['./bin/kudu-master','--use_hybrid_clock=false','--cfile_do_on_finish=flush','--flush_threshold_mb=1000','-default_num_replicas=1','--logtostderr','-fs_wal_dir=']
c[-1]=c[-1]+dirs[0]
cmds.append(c)
p1=7052
p2=8052
for i in range(1,num):
    t=['./bin/kudu-tserver']
    ps='-rpc_bind_addresses=0.0.0.0:'+`p1`
    ws='-webserver_port='+`p2`
    p1=p1+1
    p2=p2+1
    t.append(ps)
    t.append(ws)
    t.append('--use_hybrid_clock=false')
    t.append('--cfile_do_on_finish=flush')
    t.append('--flush_threshold_mb=1000')
    t.append('--logtostderr')
    t.append('-fs_wal_dir='+dirs[i])
    cmds.append(t)
procs=[]
for c in cmds:
    procs.append(subprocess.Popen(c))
control=raw_input()
if control=='stop':
    for p in procs:
        p.kill()
