import psutil
import subprocess
import time
import threading
import logging
import os
import awkward
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from ServiceXTimerLogger import time_measure, logger

# Load logger
logger = logging.getLogger('servicex_logger')

# Initialize timer
t = time_measure("benchmark")

# Input files
base_path = '/Volumes/Temp/data/'
f2G  = base_path + 'ttHML_80fb_363489_mc16d.root'
f5G  = base_path + 'ttHML_80fb_364286_mc16d.root'
f23G = base_path + 'ttHML_80fb_364250_mc16a.root'
f56G = base_path + 'ttHML_80fb_364250_mc16d.root'
f72G = base_path + 'ttHML_80fb_364253_mc16d.root'

# Empty file cache
def empty_cache(file_name:str):
    subprocess.run(['vmtouch', '-e', file_name], stdout=subprocess.PIPE)

# Find pid
def find_pid(name:str):
    for p in psutil.process_iter():
        try:
            all_process = p.name()
        except (psutil.AccessDenied, psutil.ZombieProcess):
            pass
        except psutil.NoSuchProcess:
            continue
        if name == 'uproot':
            if "python" in all_process.lower():   
                if len(p.cmdline()) == 2 and 'uproot_test' in p.cmdline()[1]:
                    # print(p.cmdline())             
                    # pids.append(p.pid)
                    print(p.pid)
                    return p.pid
        elif name == 'root':
            if "root" in all_process.lower():
                if len(p.cmdline()) == 5 and 'root_test' in p.cmdline()[4]:
                    # print(p.cmdline())             
                    # pids.append(p.pid)
                    return p.pid
        else:
            raise BaseException(f'Choose uproot or root.')
    

## START Benchmark    

input_file_name = f23G
file_size = round(os.path.getsize(input_file_name)/1e6, 2)
times = []
cpu_usages = []
disk_ios = []
trials = []
input_file_names = []
file_sizes = []

for trial in range(1,31): # Repeat 20 times
# for trial in range(1,3):
    
    print(f'Trial: {trial}')
    trials.append(trial)
    input_file_names.append(input_file_name)
    file_sizes.append(file_size)

    # 1. Empty cache
    if trial%5 == 1:
        empty_cache(input_file_name)

    # 2. Delete previous output file
    if os.path.exists('h.awkd'):
        os.remove('h.awkd')

    # Run uproot_test
    cpu_usage = []
    disk_io = []
    job_is_running = True
    t_start = time.time()

    subprocess.Popen(['python', 'uproot_test.py', input_file_name], stdout=subprocess.DEVNULL)
    p = psutil.Process(find_pid('uproot')) 
    p_job = psutil.Process(p.children()[0].pid) # Get child process
    
    disk_io_last = psutil.disk_io_counters(perdisk=True)['disk2'][2]
    # print(f'Process id: {p_job.pid}')
    clock = 0
    while job_is_running:
    #     # print(p.pid, p.status())    
        # if p_job and clock%10 == 0:
        # if p_job.status() != psutil.STATUS_ZOMBIE and clock%10 == 0:
        if clock%10 == 0:
            disk_io_now = psutil.disk_io_counters(perdisk=True)['disk2'][2]
            disk_io_diff = round((disk_io_now - disk_io_last)/1e6, 2)
            try:
                if psutil.pid_exists(p_job.pid):
                    current_cpu = p_job.cpu_percent()
            except psutil.AccessDenied:
                pass
            # print(f'cpu: {current_cpu}, disk i/o: {disk_io_diff} MB')
            cpu_usage.append(current_cpu)
            disk_io.append(disk_io_diff)
            disk_io_last = disk_io_now
        clock += 1
        time.sleep(0.01)            
        if p_job.status() == psutil.STATUS_ZOMBIE:
            print('Job is done')
            job_is_running = False
        
        
    t_finish = time.time()

    print(f'Time: {round(t_finish - t_start, 2)}')
    times.append(round(t_finish - t_start, 2))
    cpu_usages.append(cpu_usage)
    disk_ios.append(disk_io)

# print(input_file_names, file_sizes, times, trials, cpu_usages, disk_ios)

# output = pd.DataFrame({'filename':input_file_name, 'filesize':file_size, 'time':times, 'trial':trials})
output = pd.DataFrame({'filename':input_file_names, 'filesize':file_sizes, 'time':times, 'trial':trials, 'cpu_usage':cpu_usages, 'disk_io': disk_ios})

arrow = pa.Table.from_pandas(output)
out_file_name = input_file_name.split('/')[4].split('.')[0]
writer = pq.ParquetWriter(f'benchmark_{out_file_name}.parquet', arrow.schema)
writer.write_table(table=arrow)
writer.close()

# output = awkward.Table(filename=[input_file_name.split('/')[4]], filesize=[file_size], time=[round(t_finish - t_start, 2)], trial=[trial], cpu_usage=[cpu_usage], disk_io = [disk_io])
# output = awkward.Table(filename=input_file_names, filesize=file_sizes, time=times, trial=trials, cpu_usage=cpu_usages, disk_io = disk_ios)

# print(output.column)
# # print(outputs[1])

# # awkward.save('test1.awkd', outputs[0])
# # awkward.save('test2.awkd', outputs[1])
# # con_output = awkward.concatenate(outputs)
# out_file_name = input_file_name.split('/')[4]
# awkward.save(f'benchmark_{out_file_name}.awkd', output, mode="w")