import os,subprocess
num=48
waldirs=[]
datadirs=[]
for i in range(0,num):
    waldirs.append('/mnt/db/wal/'+`i`)
    datadirs.append('/mnt/db/data/'+`i`)
for d in waldirs:
    os.system("rm -rf "+d)
    os.system("mkdir -p "+d)
for d in datadirs:
    os.system("rm -rf "+d)
    os.system("mkdir -p "+d)    
cmds=[]
c=['./bin/kudu-master','--use_hybrid_clock=false','--cfile_do_on_finish=flush','--flush_threshold_mb=5000','-rpc_num_service_threads=24','-num_reactor_threads=12', '-default_num_replicas=1','--logtostderr','-fs_wal_dir=']
c[-1]=c[-1]+waldirs[0]
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
    t.append('--flush_threshold_mb=100000')
    t.append('-rpc_num_service_threads=24')
    t.append('-num_reactor_threads=12')
    t.append('--logtostderr')
    t.append('-fs_data_dirs='+datadirs[i])
    t.append('-fs_wal_dir='+waldirs[i])
    cmds.append(t)
procs=[]
for c in cmds:
    procs.append(subprocess.Popen(c))
control=raw_input()
if control=='stop':
    for p in procs:
        p.kill()
