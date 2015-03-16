from celery.task import task
from time import sleep
import paramiko as pk
import os, json

#Example task
@task()
def add(x, y):
    result = x + y
    return result


@task()
def qc_docker_workflow(forward_read_filename, reverse_read_filename, basedir="/data/static/",docker_worker=os.environ['docker_worker']):
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
            poll = poll_docker(dock_id,ssh)
            if poll['Running']:
                sleep(10)
            else:
                if poll['ExitCode']==0:
                    return "http://%s/mgmic_tasks/%s" % (docker_worker,str(qc_docker_workflow.request.id))
                else:
                    raise Exception(poll['Error'])
    else:
        raise Exception(std_err)
        
def poll_docker(docker_id,ssh):
    cmd = 'docker inspect %s' % (docker_id)
    stdin, stdout, stderr = ssh.exec_command(cmd)
    std_out = stdout.read()
    std_err = stderr.read()
    if std_err == '':
        data = json.loads(std_out)
        return data[0]['State']
    else:
        raise Exception(std_err)
    
