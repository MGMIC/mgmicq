from celery.task import task
from time import sleep
import paramiko as pk
import os, json,httplib
from urlparse import urlparse
import getpass
from dockertask import docker_task
from subprocess import call,STDOUT
from celery.task.sets import TaskSet
from celery.result import TaskSetResult
from celery import current_app
from config import workflow_config
#from celery.task import subtask
import requests
#Default MGMIC config
basedir="/data/static/"
#docker config settings
try:
    docker_config=workflow_config[os.getenv('docker_worker')]
except:
    docker_config=workflow_config["default"]
#Example task
@task()
def add(x, y):
    result = x + y
    return result

@task()
def mgmic_future_script_1(assembly,predicted_proteins,predicted_genes,forwardreads,reversereads,docker_name="mgmic/bioinformatics"):
    """
        Input Arguments
            Assembly file local path or URL
                #arg[0] = "Contigs.fasta"" from the assembly_ray folder
            Predicted Proteins - Local path or URL
                #arg[1] = "prodigal.orfs.faa" from the assembly_ray folder
            Predicted Genes
                #arg[2] = "prodigal.orfs.fna" from the assembly_ray folder
            Forward Reads - Local file path or URL
                #arg[3] = "F.QCed.fastq"
            Reverse Reads - Local file path or URL
                #arg[4] = "R.QCed.fastq"
            Docker Name (optional, default: 'mgmic/bioinformatics') - Name of the docker container that will run script  
    """
    task_id = str(mgmic_future_script_1.request.id)
    script = "MGMIC_future_script_1.pl"
    return mgmic_future_script(task_id,script,assembly,predicted_proteins,predicted_genes,forwardreads,reversereads,docker_name)

@task()
def mgmic_future_script_2(assembly,predicted_proteins,predicted_genes,forwardreads,reversereads,docker_name="mgmic/bioinformatics"):
    """
        Input Arguments
            Assembly file local path or URL
                #arg[0] = "Contigs.fasta"" from the assembly_ray folder
            Predicted Proteins - Local path or URL
                #arg[1] = "prodigal.orfs.faa" from the assembly_ray folder
            Predicted Genes
                #arg[2] = "prodigal.orfs.fna" from the assembly_ray folder
            Forward Reads - Local file path or URL
                #arg[3] = "F.QCed.fastq"
            Reverse Reads - Local file path or URL
                #arg[4] = "R.QCed.fastq"
            Docker Name (optional, default: 'mgmic/bioinformatics') - Name of the docker container that will run script
    """
    task_id = str(mgmic_future_script_2.request.id)
    script = "MGMIC_future_script_2.pl"
    return mgmic_future_script(task_id,script,assembly,predicted_proteins,predicted_genes,forwardreads,reversereads,docker_name)

@task()
def mgmic_future_script_3(assembly,predicted_proteins,predicted_genes,forwardreads,reversereads,docker_name="mgmic/bioinformatics"):
    """
        Input Arguments
            Assembly file local path or URL
                #arg[0] = "Contigs.fasta"" from the assembly_ray folder
            Predicted Proteins - Local path or URL
                #arg[1] = "prodigal.orfs.faa" from the assembly_ray folder
            Predicted Genes
                #arg[2] = "prodigal.orfs.fna" from the assembly_ray folder
            Forward Reads - Local file path or URL
                #arg[3] = "F.QCed.fastq"
            Reverse Reads - Local file path or URL
                #arg[4] = "R.QCed.fastq"
            Docker Name (optional, default: 'mgmic/bioinformatics') - Name of the docker container that will run script
    """
    task_id = str(mgmic_future_script_3.request.id)
    script = "MGMIC_future_script_3.pl"
    return mgmic_future_script(task_id,script,assembly,predicted_proteins,predicted_genes,forwardreads,reversereads,docker_name)

@task()
def mgmic_future_script_4(assembly,predicted_proteins,predicted_genes,forwardreads,reversereads,docker_name="mgmic/bioinformatics"):
    """
        Input Arguments
            Assembly file local path or URL
                #arg[0] = "Contigs.fasta"" from the assembly_ray folder
            Predicted Proteins - Local path or URL
                #arg[1] = "prodigal.orfs.faa" from the assembly_ray folder
            Predicted Genes
                #arg[2] = "prodigal.orfs.fna" from the assembly_ray folder
            Forward Reads - Local file path or URL
                #arg[3] = "F.QCed.fastq"
            Reverse Reads - Local file path or URL
                #arg[4] = "R.QCed.fastq"
            Docker Name (optional, default: 'mgmic/bioinformatics') - Name of the docker container that will run script
    """
    task_id = str(mgmic_future_script_4.request.id)
    script = "MGMIC_future_script_4.pl"
    return mgmic_future_script(task_id,script,assembly,predicted_proteins,predicted_genes,forwardreads,reversereads,docker_name)

@task()
def mgmic_future_script_5(assembly,predicted_proteins,predicted_genes,forwardreads,reversereads,docker_name="mgmic/bioinformatics"):
    """
        Input Arguments
            Assembly file local path or URL
                #arg[0] = "Contigs.fasta"" from the assembly_ray folder
            Predicted Proteins - Local path or URL
                #arg[1] = "prodigal.orfs.faa" from the assembly_ray folder
            Predicted Genes
                #arg[2] = "prodigal.orfs.fna" from the assembly_ray folder
            Forward Reads - Local file path or URL
                #arg[3] = "F.QCed.fastq"
            Reverse Reads - Local file path or URL
                #arg[4] = "R.QCed.fastq"
            Docker Name (optional, default: 'mgmic/bioinformatics') - Name of the docker container that will run script
    """
    task_id = str(mgmic_future_script_5.request.id)
    script = "MGMIC_future_script_5.pl"
    return mgmic_future_script(task_id,script,assembly,predicted_proteins,predicted_genes,forwardreads,reversereads,docker_name)

def mgmic_future_script(task_id,script,assembly,predicted_proteins,predicted_genes,forwardreads,reversereads,docker_name):
    resultDir = os.path.join(basedir, 'mgmic_tasks/', task_id)
    os.makedirs(resultDir)
    logfile= open(resultDir + "/logfile.txt","w")
    assembly_file = task_file_setup(assembly,resultDir,logfile)
    predicted_proteins_file= task_file_setup(predicted_proteins,resultDir,logfile)
    predicted_genes_file = task_file_setup(predicted_genes,resultDir,logfile)
    forwardreads_file = task_file_setup(forwardreads,resultDir,logfile)
    reversereads_file = task_file_setup(reversereads,resultDir,logfile)
    docker_opts = "-v %s:/data -v %s:/opt/local/scripts" % (docker_config["data_dir"],docker_config["script_dir"])
    docker_cmd = "/opt/local/scripts/bin/%s %s %s %s %s %s %s"
    docker_cmd= docker_cmd % (script,assembly_file,predicted_proteins_file,predicted_genes_file,forwardreads_file,reversereads_file,resultDir)
    try:
        result = docker_task(docker_name=docker_name,docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
        return "http://%s/mgmic_tasks/%s" % (result['host'],result['task_id'])
    except:
        raise

@task()
def amplicon_workflow(forward_read_url, reverse_read_url,mapfile,runflags=None):
    task_id = str(amplicon_workflow.request.id)
    resultDir = os.path.join(basedir, 'mgmic_tasks/', task_id)
    os.makedirs(resultDir)
    logfile= open(resultDir + "/logfile.txt","w")
    foward_read = task_file_setup(forward_read_url,resultDir,logfile)
    reverse_read = task_file_setup(reverse_read_url,resultDir,logfile)
    map_read = task_file_setup(mapfile ,resultDir,logfile) 
    logfile.close()
    print "************************* ", runflags, " **********************************"
    try:
        #Step 1 Bioinformatics docker contatiner
        docker_opts = "-v %s:/data -v %s:/opt/local/scripts" % (docker_config["data_dir"],docker_config["script_dir"])
        docker_cmd = "/opt/local/scripts/bin/Illumina_MySeq_16SAmplicon_analysis_part1.pl %s %s %s" % (foward_read,reverse_read,resultDir)
        if runflags:
            docker_cmd = "%s '%s'" % (docker_cmd,runflags)
        result = docker_task(docker_name="mgmic/bioinformatics",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
        #Step 2 qiime docker container
        docker_opts = "-i -t -v %s:/opt/local/scripts -v %s:/data" % (docker_config["script_dir"],docker_config["data_dir"])
        docker_cmd = "/opt/local/scripts/bin/Illumina_MySeq_16SAmplicon_analysis_part2.pl %s %s %s" % (foward_read,map_read.split('/')[-1],resultDir)
        if runflags:
            docker_cmd = "%s '%s'" % (docker_cmd,runflags)
        result = docker_task(docker_name="qiime_env",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
        #return result http directory
        return "http://%s/mgmic_tasks/%s" % (result['host'],result['task_id'])
    except:
        raise

@task()
def mgmic_functional_gene(forward_read_url, reverse_read_url, database,result_dir=None,parent_id=None,runflags=None,workflow=None):
    task_id = str(mgmic_functional_gene.request.id)
    #Get local database file
    default_dbs = "http://mgmic.oscer.ou.edu/api/catalog/data/data_portal/gene_database/.json?query=%s"
    query ="{'spec':{'name':'%s'}}" % database
    data = requests.get(default_dbs % query).json()
    data = data['results'][0]
    db_local_file = data['local_file']
    #Check Input and setup gene search docker
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
        resultDir= os.path.join(result_dir,"functional_gene",database.split('.')[0])
        os.makedirs(resultDir)
        reverse_read= reverse_read_url
        foward_read = forward_read_url
    #Setup Docker container
    docker_opts = "-v /opt/local/scripts/:/scripts -v /data:/data -v /opt:/opt"
    docker_cmd = "/scripts/bin/Illumina_MySeq_Quantify_Funtional_Gene.pl %s %s %s %s" % (foward_read,reverse_read,db_local_file,resultDir)
    try:
        result = docker_task(docker_name="mgmic/bioinformatics",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
        if parent_id:
            return "http://%s/mgmic_tasks/%s/%s/%s" % (result['host'],parent_id,"functional_gene",database.split('.')[0])
        else:
            return "http://%s/mgmic_tasks/%s" % (result['host'],result['task_id'])
    except:
        raise


@task()
def mgmic_16s_classification(forward_read_url, reverse_read_url, result_dir=None,parent_id=None,runflags=None,workflow=None):
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
        if parent_id:
            return "http://%s/mgmic_tasks/%s/%s" % (result['host'],parent_id,"16s_classification")
        else:
            return "http://%s/mgmic_tasks/%s" % (result['host'],result['task_id'])
    except:
        raise
@task()
def mgmic_assembly_ray(forward_read_url, reverse_read_url, result_dir=None,parent_id=None,runflags=None,workflow=None):
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
        forward_read = forward_read_url

    docker_opts = "-v /opt/local/scripts/:/scripts -v /data:/data"
    docker_cmd = "/scripts/bin/Illumina_MySeq_Assemble_Ray31.pl %s %s %s" % (forward_read,reverse_read,resultDir)
    try:
        result = docker_task(docker_name="mgmic/bioinformatics",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
        if parent_id:
            return "http://%s/mgmic_tasks/%s" % (result['host'],parent_id,"assemble_ray")
        else:
            return "http://%s/mgmic_tasks/%s" % (result['host'],result['task_id'])
    except:
        raise


@task()
def mgmic_qc_workflow(forward_read_url, reverse_read_url,functional_gene=None,runflags=None,workflow={"qc":"on","s16":"on","assemble":"on"},callback=None):
    """
        Task: mgmic_qc_workflow
        args: [forward_read_url, reverse_read_url]
        returns: resutl url
    """
    #Setup Result Directory
    task_id = str(mgmic_qc_workflow.request.id)
    resultDir = os.path.join(basedir, 'mgmic_tasks/', task_id)
    os.makedirs(resultDir)

    #foward_read = os.path.join(resultDir,forward_read_url.split('/')[-1])
    #reverse_read = os.path.join(resultDir,reverse_read_url.split('/')[-1])
    logfile= open(resultDir + "/logfile.txt","w")
    #setup local files from url or local filename
    foward_read = task_file_setup(forward_read_url,resultDir,logfile)
    reverse_read = task_file_setup(reverse_read_url,resultDir,logfile)
    #check if local file
    #if os.path.isfile(forward_read_url):
    #    os.rename(forward_read_url, os.path.join(resultDir,forward_read_url.split('/')[-1]))
    #else:
        #Check if Urls exist
   #     if not check_url_exist(forward_read_url):
   #         raise Exception("Please Check URL %s" % forward_read_url)
   #     call(['wget','-O',foward_read,forward_read_url],stdout=logfile)
   # if os.path.isfile(reverse_read_url):
   #     os.rename(reverse_read_url,os.path.join(resultDir,reverse_read_url.split('/')[-1]))
   # else:
   #     if not check_url_exist(reverse_read_url):
   #         raise Exception("Please Check URL %s" % reverse_read_url)
   #     call(['wget','-O',reverse_read,reverse_read_url],stdout=logfile)
    logfile.close()
    #docker_opts = "-v /opt/local/scripts/:/scripts -v /data/static:/data/static"
    #docker_cmd = "/scripts/bin/Illumina_MySeq_Trim %s %s %s" % (foward_read,reverse_read,resultDir)
    try:
        #Step 1 Bioinformatics docker contatiner
        docker_opts = "-v %s:/data -v %s:/scripts" % (docker_config["data_dir"],docker_config["script_dir"])
        docker_cmd = "/scripts/bin/Illumina_MySeq_Trim %s %s %s %s" % (foward_read,reverse_read,resultDir,workflow.get("qc","on"))
        if runflags:
            docker_cmd = "%s '%s'" % (docker_cmd,runflags)
        result = docker_task(docker_name="mgmic/bioinformatics",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
        #step 2 Setup subtasks
        fqc="%s/%s" % (resultDir,"F.QCed.fastq")
        rqc="%s/%s" % (resultDir,"R.QCed.fastq")
        tasks= [mgmic_assembly_ray.subtask(args=(fqc,rqc),kwargs={'result_dir':resultDir,'runflags':runflags,'on_off':workflow.get("assemble","on")}),
                mgmic_16s_classification.subtask(args=(fqc,rqc),kwargs={'result_dir':resultDir,'runflags':runflags,'on_off':workflow.get("s16","on")}),]
        if functional_gene:
            #print type(functional_gene)
            for name in functional_gene:
                tasks.append(mgmic_functional_gene.subtask(args=("%s/%s" % (resultDir,"F.QCed.fasta"),"%s/%s" % (resultDir,"R.QCed.fasta"), name),
                            kwargs={'result_dir':resultDir,'parent_id':task_id,'runflags':runflags}))
        job = TaskSet(tasks=tasks)
        result_set = job.apply_async()
        callback = generate_report.subtask(args=(foward_read,reverse_read,task_id,result_set.taskset_id,result_set.subtasks,"callback"),kwargs={'max_retries':2880}).apply_async()
        #generate_report.subtask(args=(result_set)).apply_async()
        temp=[]
        for result in result_set.subtasks:
            temp.append(result.serializable())
        #report= callback.apply_async()
        print dir(callback)
        return {"result_url":"http://%s/mgmic_tasks/%s" % (result['host'],result['task_id']),"subtasks":temp,"report":"result"}
        #result_set.taskset_id
        #result_set.subtasks
        #if callback is not None:
        #    subtask(
        #data = result_set.join()
        #return "http://%s/mgmic_tasks/%s" % (result['host'],result['task_id'])
    except:
        raise

@task()
def generate_report(fread,rread,task_id,setid, subtasks, callback, interval=60, max_retries=None):
    result = TaskSetResult(setid, subtasks)
    if result.ready():
        docker_opts = "-v /data:/data"
        docker_cmd = "make_report -f %s -r %s -t %s" % (fread,rread,task_id)
        try:
            result = docker_task(docker_name="mgmic/report",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
            return "http://%s/mgmic_tasks/%s/report.html" % (result['host'],result['task_id'])
        except:
            raise
        #return subtask(callback).delay(result.join())
    generate_report.retry(countdown=interval, max_retries=max_retries)

def gunzip(filename,logfile):
    if filename[-3:]==".gz":
        call(['gunzip',filename],stdout=logfile)
        return filename[:-3]
    return filename

def task_file_setup(filename,resultDir,logfile):
    #check if filename is local file
    if os.path.isfile(filename):
        return_file = os.path.join(resultDir,filename.split('/')[-1])
        os.rename(filename, return_file)
        return gunzip(return_file,logfile)
    else:
        #Check if Urls exist
        if not check_url_exist(filename):
            raise Exception("Please Check URL or Local File Path(local files must be in /data directory) %s" % filename)
        return_file = os.path.join(resultDir,filename.split('/')[-1])
        logfile.write('This is a test\n')
        print return_file, filename
        call(['wget','-O',return_file ,filename],stdout=logfile,stderr=logfile)
        return gunzip(return_file,logfile)

def check_url_exist(url):
    p = urlparse(url)
    c = httplib.HTTPConnection(p.netloc)
    c.request("HEAD", p.path)
    return c.getresponse().status < 400
