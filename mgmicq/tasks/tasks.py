from celery.task import task
from time import sleep
import paramiko as pk
import os, json,httplib
from urlparse import urlparse
import getpass
from dockertask import docker_task
from subprocess import call,STDOUT
from celery.task.sets import TaskSet

#Example task
@task()
def add(x, y):
    result = x + y
    return result

@task()
def mgmic_16s_classification(forward_read_url, reverse_read_url, basedir="/data/static/",result_dir=None):
    task_id = str(mgmic_16s_classification.request.id)
    if not result_dir:
        resultDir = os.path.join(basedir, 'mgmic_tasks/', task_id)
        os.makedirs(resultDir)
        #os.chdir(resultDir)
        #Check if Urls exist
        if not check_url_exist(forward_read_url):
            raise Exception("Please Check URL %s" % forward_read_url)
        if not check_url_exist(reverse_read_url):
            raise Exception("Please Check URL %s" % reverse_read_url)
            foward_read = os.path.join(resultDir,forward_read_url.split('/')[-1])
            reverse_read = os.path.join(resultDir,reverse_read_url.split('/')[-1])
        logfile= open(resultDir + "/logfile.txt","w")
        call(['wget','-O',foward_read,forward_read_url],stdout=logfile)
        call(['wget','-O',reverse_read,reverse_read_url],stdout=logfile)
        logfile.close()
    else:
        resultDir= os.path.join(result_dir,"16s_classification")
        os.makedirs(resultDir)
        reverse_read= reverse_read_url
        foward_read = forward_read_url

    s16_database = "/data/DATABASES/16S/SSURef_111_candidate_db.udb"
    docker_opts = "-v /opt/local/scripts/:/scripts -v /data:/data -v /opt:/opt"
    docker_cmd = "/scripts/bin/classify_metagenome_by_16S_step1.pl %s %s %s %s" % (foward_read,reverse_read,s16_database,resultDir)
    try:
        result = docker_task(docker_name="mgmic/bioinformatics",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
        #return "http://%s/mgmic_tasks/%s" % (result['host'],result['task_id'])
        docker_opts = "-i -t -v /opt:/opt -v /data:/data"
        docker_cmd = "/opt/local/scripts/bin/classify_metagenome_by_16S_step2.pl %s %s %s" % (foward_read,reverse_read,resultDir)
        #result = docker_task(docker_name="bwawrik/qiime",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
        result = docker_task(docker_name="qiime_env",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
        return "http://%s/mgmic_tasks/%s" % (result['host'],result['task_id'])
    except:
        raise
@task()
def mgmic_assembly_ray(forward_read_url, reverse_read_url, basedir="/data/static/",result_dir=None):
    task_id = str(mgmic_assembly_ray.request.id)
    if not result_dir:
        resultDir = os.path.join(basedir, 'mgmic_tasks/', task_id)
        os.makedirs(resultDir)
        #os.chdir(resultDir)
        #Check if Urls exist
        if not check_url_exist(forward_read_url):
            raise Exception("Please Check URL %s" % forward_read_url)
        if not check_url_exist(reverse_read_url):
            raise Exception("Please Check URL %s" % reverse_read_url)
        foward_read = os.path.join(resultDir,forward_read_url.split('/')[-1])
        reverse_read = os.path.join(resultDir,reverse_read_url.split('/')[-1])
        logfile= open(resultDir + "/logfile.txt","w")
        call(['wget','-O',foward_read,forward_read_url],stdout=logfile)
        call(['wget','-O',reverse_read,reverse_read_url],stdout=logfile)
        logfile.close()
    else:
        resultDir = os.path.join(result_dir,"assemble_ray")
        os.makedirs(resultDir)
        reverse_read = reverse_read_url
        foward_read = forward_read_url

    docker_opts = "-v /opt/local/scripts/:/scripts -v /data:/data"
    docker_cmd = "/scripts/bin/Illumina_MySeq_Assemble_Ray31.pl %s %s %s" % (forward_read,reverse_read,resultDir)
    try:
        result = docker_task(docker_name="mgmic/bioinformatics",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
        return "http://%s/mgmic_tasks/%s" % (result['host'],result['task_id'])
    except:
        raise


@task()
def mgmic_qc_workflow(forward_read_url, reverse_read_url, basedir="/data/static/"):
    """
        Task: mgmic_qc_workflow
        args: [forward_read_url, reverse_read_url]
        returns: resutl url
    """
    task_id = str(mgmic_qc_workflow.request.id)
    resultDir = os.path.join(basedir, 'mgmic_tasks/', task_id)
    os.makedirs(resultDir)

    foward_read = os.path.join(resultDir,forward_read_url.split('/')[-1])
    reverse_read = os.path.join(resultDir,reverse_read_url.split('/')[-1])
    logfile= open(resultDir + "/logfile.txt","w")
    #check if local file
    if os.path.isfile(forward_read_url):
        os.rename(forward_read_url, os.path.join(resultDir,forward_read_url.split('/')[-1]))
    else:
        #Check if Urls exist
        if not check_url_exist(forward_read_url):
            raise Exception("Please Check URL %s" % forward_read_url)
        call(['wget','-O',foward_read,forward_read_url],stdout=logfile)
    if os.path.isfile(reverse_read_url):
        os.rename(reverse_read_url,os.path.join(resultDir,reverse_read_url.split('/')[-1]))
    else:
        if not check_url_exist(reverse_read_url):
            raise Exception("Please Check URL %s" % reverse_read_url)
        call(['wget','-O',reverse_read,reverse_read_url],stdout=logfile)
    logfile.close()
    docker_opts = "-v /opt/local/scripts/:/scripts -v /data/static:/data/static"
    docker_cmd = "/scripts/bin/Illumina_MySeq_Trim %s %s %s" % (foward_read,reverse_read,resultDir)
    try:
        result = docker_task(docker_name="mgmic/bioinformatics",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
        fqc="%s/%s" % (resultDir,"F.QCed.fastq")
        rqc="%s/%s" % (resultDir,"R.QCed.fastq")
        job = TaskSet(tasks=[ mgmic_assembly_ray.subtask(args=(fqc,rqc),kwargs={'result_dir':resultDir}),
            mgmic_16s_classification.subtask(args=(fqc,rqc),kwargs={'result_dir':resultDir})
        ])
        result_set = job.apply_async()
        #data = result_set.join()
        return "http://%s/mgmic_tasks/%s" % (result['host'],result['task_id'])
    except:
        raise

def check_url_exist(url):
    p = urlparse(url)
    c = httplib.HTTPConnection(p.netloc)
    c.request("HEAD", p.path)
    return c.getresponse().status < 400
"""
@task()
def qc_docker_workflow(forward_read_filename, reverse_read_filename, basedir="/data/static/",docker_worker=os.environ['docker_worker']):
    ""
        Deprecated version of docker_task
        Task: mgmic_qc_workflow
        args: [forward_read_filename, reverse_read_filename] This is local paths
        returns: resutl url
    ""
    resultDir = os.path.join(basedir, 'mgmic_tasks/', str(qc_docker_workflow.request.id))
    os.makedirs(resultDir)
    ssh = pk.SSHClient()
    ssh.set_missing_host_key_policy(pk.AutoAddPolicy())
    #keys setup to login as pass environmental username to docker
    ssh.connect(docker_worker,22,os.environ['username'])
    cmd ='docker run -d -v /opt/local/scripts/:/scripts -v /data/static:/data/static bwawrik/bioinformatics /scripts/bin/Illumina_MySeq_Trim %s %s %s'
    cmd = cmd % (forward_read_filename,reverse_read_filename,resultDir)
    stdin, stdout, stderr = ssh.exec_command(cmd)
    std_out = stdout.read()
    std_err = stderr.read()
    if std_err == '':
        dock_id = std_out.strip(' \n')
        #Example poll_docker return
        #{u'Pid': 0, u'OOMKilled': True, u'Paused': False, u'Running': False,
        # u'FinishedAt': u'2015-03-13T19:11:31.66940599Z', u'Restarting': False,
        # u'Error': u'', u'StartedAt': u'2015-03-13T19:08:56.879774955Z', u'ExitCode': 0}
        while True:
            poll = docker_state(dock_id,ssh)
            if poll['Running']:
                sleep(10)
            else:
                if poll['ExitCode']==0:
                    return "http://%s/mgmic_tasks/%s" % (docker_worker,str(qc_docker_workflow.request.id))
                else:
                    raise Exception(poll['Error'])
    else:
        raise Exception(std_err)

def docker_state(docker_id,ssh):
    cmd = 'docker inspect %s' % (docker_id)
    stdin, stdout, stderr = ssh.exec_command(cmd)
    std_out = stdout.read()
    std_err = stderr.read()
    if std_err == '':
        data = json.loads(std_out)
        return data[0]['State']
    else:
        raise Exception(std_err)
"""
