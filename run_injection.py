import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from datetime import datetime
import seaborn as sns
from collections import defaultdict
import time
import plotly.graph_objects as go
import random
import shutil
import os
import subprocess
import itertools
import concurrent.futures

def parse_log(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()
        
    # Extract jobs
    jobs = [line for line in lines if not line.startswith(';')]
    return jobs

import random

def reorder_rst_file(input_filename, output_filename):
    with open(input_filename, 'r') as file:
        lines = file.readlines()

    # Parse and sort the lines based on the first element
    sorted_lines = sorted(lines, key=lambda x: int(x.split(';')[0].strip()))

    # Write the sorted lines to a new file
    with open(output_filename, 'w') as file:
        file.writelines(sorted_lines)
        
def filter_jobs(jobs, unix_start_time, start_day, end_day, time_limit, node_limit, system_flag, percentage=None):
    # Convert days to seconds
    start_time_seconds = start_day * 24 * 60 * 60
    end_time_seconds = end_day * 24 * 60 * 60
    
    filtered_jobs = []
    
    for job in jobs:
        parts = job.strip().split()
        submit_time = int(parts[1])
        
        # Check if the job falls within the specified start and end days
        if unix_start_time + start_time_seconds <= submit_time <= unix_start_time + end_time_seconds:
            run_time = int(parts[8])  # Assuming runtime is the fourth element
            requested_nodes = int(parts[4])  # Assuming the node request is the fifth element
            
            # Check other conditions
            if requested_nodes <= node_limit and run_time <= time_limit:
                filtered_jobs.append(job)
    
    if percentage:
        step = int(1 / percentage)
        filtered_jobs = [filtered_jobs[i] for i in range(0, len(filtered_jobs), step)]

    # Modify the second to last value based on the system flag and remove the last value
    for i in range(len(filtered_jobs)):
        parts = filtered_jobs[i].strip().split()
        parts[-2] = str(system_flag)
        filtered_jobs[i] = ' '.join(parts[:-1])  # Removes the last value

    return filtered_jobs


def combine_logs(theta_log_path, cori_log_path, combined_log_path,start,end, runtime, node, p):
    # Parse logs
    theta_jobs = parse_log(theta_log_path)
    cori_jobs = parse_log(cori_log_path)
    
    # Assuming the Unix start time is provided or can be extracted from the logs
    unix_start_time = 1522540800  # Replace with the actual Unix start time
    
    # Filter jobs
    filtered_theta_jobs = filter_jobs(theta_jobs, unix_start_time,start,end, 1000000, 10000, 0)  # For Theta
    filtered_cori_jobs = filter_jobs(cori_jobs, unix_start_time, start,end,runtime,node,1, percentage= None)  
    
    # print(len(filtered_cori_jobs))
    # Combine and sort by submission time
    combined_jobs = sorted(filtered_theta_jobs + filtered_cori_jobs, key=lambda x: int(x.split()[1]))
    
    # Reindex combined logs
    reindexed_combined_jobs = []
    for idx, job in enumerate(combined_jobs, start=1):
        parts = job.split()
        parts[0] = str(idx)  # Replace job ID with new index
        reindexed_combined_jobs.append(' '.join(parts))
    
    # Write to combined log file with headers
    with open(combined_log_path, 'w') as out_file:
        out_file.write("; UnixStartTime: 0\n")
        out_file.write("; MaxNodes: 4360\n")
        out_file.write("; MaxProcs: 4360\n")
        for job in reindexed_combined_jobs:
            out_file.write(job + '\n')

            
def insert_exp(start, end, runtime, node, p):
    start = start
    end = end
    runtime = runtime
    node = node
    p = p
    filename = 'theta_cori.swf'

    rst = 'theta_cori_' + str(runtime) + '_' + str(node) + '_' + str(int(p*100)) + '.rst'
    ult = 'theta_cori_' + str(runtime) + '_' + str(node) + '_' + str(int(p*100)) + '.ult'

    pre = '/Users/zhongzheng/Desktop/data analysis'
    # Combining logs (assuming combine_logs is defined)
    combine_logs(pre + '/swf/theta_2018_4-7.swf', pre + '/swf/cori_2018.swf', pre+'/single_sys/swf/' + filename, start, end, runtime, node, p)

    # File paths
    source_path_theta_cori = pre+'/single_sys/swf/' + filename
    destination_path_theta_cori = '/Users/zhongzheng/Desktop/CQSIM/CQSim/data/InputFiles/' + filename

    # Copy the file to CQSIM directory
    shutil.move(source_path_theta_cori, destination_path_theta_cori)

    # Save the current working directory
    original_dir = os.getcwd()

    # Change to the CQSIM directory and run the script
    cqsim_dir = '/Users/zhongzheng/Desktop/CQSIM/CQSim/src'
    os.chdir(cqsim_dir)

    # Run the subprocess and wait for it to finish
    process = subprocess.run(['python3', 'cqsim.py'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # Check if the subprocess finished successfully
    if process.returncode == 0:
        # print("Subprocess finished successfully.")

        # Change back to the original directory
        os.chdir(original_dir)

        # Move the result files back
        source_directory = '/Users/zhongzheng/Desktop/CQSIM/CQSim/data/Results'
        destination_directory_rst = pre+'/single_sys/rst'
        destination_directory_ult = pre+'/single_sys/ult'

        res_rst = "theta_cori.rst"
        res_ult = "theta_cori.ult"
        # Move the files
        shutil.move(os.path.join(source_directory, res_rst), os.path.join(destination_directory_rst, rst))
        shutil.move(os.path.join(source_directory, res_ult), os.path.join(destination_directory_ult, ult))
        rst_r = pre+'/single_sys/rst/theta_cori_' + str(runtime) + '_' + str(node) + '_' + str(int(p*100)) + '_r.rst'
        reorder_rst_file(pre+"/single_sys/rst/"+rst,rst_r)
        
        # avg_wait = (calculate_avg_waittime(rst_r, start, end,"day",1))["theta"]
                         
        
        # print("Result files moved successfully.")
    else:
        print("Subprocess encountered an error:", process.stderr)
        os.chdir(original_dir)



start = 0
end = 180
percentage = [1]
runtime = [60,120,240,300,540,600,960,1800,2700,3600]

node = [1,4,8,16,64,128,256,512,1024,2048,4096]


for p in percentage:
    for r in runtime:
        for n in node:
            insert_exp(start,end,r,n,p)
os.chdir('/Users/zhongzheng/Desktop/data analysis')