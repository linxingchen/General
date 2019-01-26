#!/home/linking/.pyenv/versions/3.5.1/bin/python
import os
import argparse
import time

# ggkbase.prep.py
# Version 0.1
# 01.17.19
# by linxingchen

# localtime = time.asctime(time.localtime(time.time()))

parser = argparse.ArgumentParser(description="This script is used to perform all metagenomic steps before submitted "
                                             "to ggKbase, please make sure the reads files for assembly are in the "
                                             "current path.")
requiredNamed = parser.add_argument_group('required named arguments')
requiredNamed.add_argument("-b", "--basename", type=str, help="read file(s) base name", required=True)
requiredNamed.add_argument("-u", "--username", type=str, help="your username at biotite", required=True)
parser.add_argument("-t", "--threads", type=int, default=6, help="number of threads for assembly (default = 6)")
parser.add_argument("-p", "--processes", type=int, default=6, help="number of threads for bowtie2 (default = 6)")
parser.add_argument("-m", "--metaspades", action='store_true', default=False,
                    help="do metaspades assembly (default = True, if provided)")
parser.add_argument("-i", "--idba", action='store_true', default=False,
                    help="do idba_ud assembly (default = True, if provided)")
parser.add_argument("-q", "--quality", type=str, help="run process_reads_bbmap.rb")
parser.add_argument("-e", "--email", type=str, help="email address to send information when job is finished")
args = parser.parse_args()

name = args.basename

if args.quality:
    os.system('process_reads_bbmap.rb --basename={0} -c'.format(name))


# Check cluster highmem queue
def highmem_is_free():
    os.system('qstat -u "**" > {0}.0.txt'.format(name))
    high = open('{0}.0.txt'.format(name), 'r')
    high_jobs = []
    for line in high.readlines():
        if line.startswith('job-ID') is False and line.startswith('-') is False:
            line = line.strip().split()
            if line[7] == 'highmem@hydra.berkeley.edu':
                high_jobs.append(line[7])
    return len(high_jobs) == 0


# Check cluster normal queue
def queue_is_busy(a):
    os.system('qstat -u "**" > {0}.1.txt'.format(name))
    qstat = open('{0}.1.txt'.format(name), 'r')
    total_qw_jobs = []
    for line in qstat.readlines():
        if line.startswith('job-ID') is False and line.startswith('-') is False:
            line = line.strip().split()
            if line[4] == 'wq':
                total_qw_jobs.append(line[4])
    return len(total_qw_jobs) >= a


# Run metaspades assembly
threads = args.threads
on_cluster = []
pwd = os.getcwd()

if args.metaspades:
    if highmem_is_free():
        my_job = open("{0}".format(name), 'w')
        print("/home/linking/softwares/SPAdes-3.11.0-Linux/bin/./metaspades.py --pe1-1 "
              "{0}/{1}_trim_clean.PE.1.fastq.gz "
              "--pe1-2 {0}/{1}_trim_clean.PE.2.fastq.gz -o {0}/{1}.metaspades -t 60 -k 21,33,55,77,99,127".
              format(pwd, name), flush=True, file=my_job)
        os.system("qsub -V -q highmem -pe smp 60 {0}".format(name))
        on_cluster.append('yes')
        print("the assembly of {0} is on highmem".format(name), flush=True)
    elif not highmem_is_free() and not queue_is_busy(100):
        my_job = open("{0}".format(name), 'w')
        print("/home/linking/softwares/SPAdes-3.11.0-Linux/bin/./metaspades.py --pe1-1 "
              "{0}/{1}_trim_clean.PE.1.fastq.gz "
              "--pe1-2 {0}/{1}_trim_clean.PE.2.fastq.gz -o {0}/{1}.metaspades -t 48 -k 21,33,55,77,99,127".
              format(pwd, name), flush=True, file=my_job)
        os.system("qsub -V {0}".format(name))
        on_cluster.append('yes')
        print("the assembly of {0} is on cluster".format(name), flush=True)
    else:
        os.system("/home/linking/softwares/SPAdes-3.11.0-Linux/bin/metaspades.py --pe1-1 {0}_trim_clean.PE.1.fastq.gz "
                  "--pe1-2 {0}_trim_clean.PE.2.fastq.gz -o {0}.metaspades -t {1} -k 21,33,55,77,99,127".
                  format(name, threads))
        print("the assembly of {0} is on biotite".format(name), flush=True)

if args.idba:
    if highmem_is_free():
        my_job = open("{0}".format(name), 'w')
        print("idba_ud --pre_correction -r {0}/{1}_trim_clean.PE.fa -o {0}/{1}.idba --mink 20 --maxk 140 --step 20 "
              "--num_threads 60".format(pwd, name), flush=True, file=my_job)
        os.system("qsub -V -q highmem -pe smp 60 {0}".format(name))
        on_cluster.append('yes')
        print("the assembly of {0} is on highmem".format(name), flush=True)
    elif not highmem_is_free() and not queue_is_busy(100):
        my_job = open("{0}".format(name), 'w')
        print("idba_ud --pre_correction -r {0}/{1}_trim_clean.PE.fa -o {0}/{1}.idba --mink 20 --maxk 140 --step 20 "
              "--num_threads 48".format(pwd, name), flush=True, file=my_job)
        os.system("qsub -V {0}".format(name))
        on_cluster.append('yes')
        print("the assembly of {0} is on cluster".format(name), flush=True)
    else:
        os.system("idba_ud --pre_correction -r {0}_trim_clean.PE.fa -o {0}.idba --mink 20 --maxk 140 --step 20 "
                  "--num_threads {1}".format(name, threads))
        print("the assembly of {0} is on biotite".format(name), flush=True)


# Get the assembly job id when running at cluster
def get_assembly_job_id():
    os.system('qstat -u "**" > {0}.2.txt'.format(name))
    qstat = open('{0}.2.txt'.format(name), 'r')
    job_id = []
    for line in qstat.readlines():
        if line.startswith('job-ID') is False and line.startswith('-') is False:
            line = line.strip().split()
            if line[2] in "{0}".format(name) and line[3] == "{0}".format(args.username):
                job_id.append(line[0])
    return job_id[-1]  # try to get the most recently submitted one, job_id[0] may have problem


assembly_job_id = get_assembly_job_id()
print(assembly_job_id, flush=True)
error_file_name = "{0}".format(name) + '.e' + assembly_job_id
print(error_file_name, flush=True)


# Check if assembly is finished
# if non finished, check each minute
def assembly_is_finished():
    os.system('qstat -u "**" > {0}.3.txt'.format(name))
    qstat = open('{0}.3.txt'.format(name), 'r')
    job = []
    for line in qstat.readlines():
        if line.startswith('job-ID') is False and line.startswith('-') is False:
            line = line.strip().split()
            if line[2] in "{0}".format(name) and line[3] == "{0}".format(args.username):
                job.append(line[2])
    return len(job) == 0  # if you submit metaspades and idba assembly for a same sample at the same time,
    # this step will wait for both finishing.


if len(on_cluster) == 1:
    while not assembly_is_finished():
        time.sleep(60)


# Check if assembly finished without error happening
def assembly_without_error(file):
    size = os.path.getsize("/home/{0}/{1}".format(args.username, file))
    return size == 0


if assembly_without_error(error_file_name):
    if args.email:
        os.system("echo ggkbase preparation for {0} has finished assembly | mail -s ggkbase_prep_for_{0} {1}".
                  format(name, args.email))
else:
    if args.email:
        os.system("echo ggkbase preparation for {0} has problem with assembly, please see the {2} file in your "
                  "home directory | mail -s ggkbase_prep_for_{0} {1}".format(name, args.email, error_file_name))
    exit()


os.system('rm {0}.0.txt {0}.1.txt {0}.2.txt {0}.3.txt'.format(name))

# Delete unnecessary output files and modify scaffolds headers
if args.metaspades:
    os.chdir("{0}.metaspades".format(name))
    os.system("rm -r assembly* before* *paths corrected dataset.info first* input* K* misc* tmp")
    os.system("sed 's/NODE/{0}_scaffold/g' scaffolds.fasta >temp.fasta".format(name))
    os.system("sed 's/_length/\t/g' temp.fasta >temp1.fasta")
    os.system("cut -f1 temp1.fasta >{0}_scaffolds.fasta".format(name))
    os.system("rm temp.fasta temp1.fasta")

if args.idba:
    os.chdir("{0}.idba".format(name))
    os.system("rm kmer contig-* align-* graph-* local-* begin end")
    os.system("sed 's/scaffold/{0}_scaffold/g' scaffold.fa > {0}_scaffolds.fasta".format(name))

# Read mapping for coverage calculation
processes = args.processes
os.mkdir('bt2')
os.system("bowtie2-build {0}_scaffolds.fasta bt2/{0}_scaffolds.fasta".format(name))
os.system("bowtie2 -p {0} -X 2000 -x bt2/{1}_scaffolds.fasta -1 {2}/{1}_trim_clean.PE.1.fastq.gz "
          "-2 {2}/{1}_trim_clean.PE.2.fastq.gz 2> {1}_scaffolds_mapped.log | shrinksam -v > {1}_scaffolds_mapped.sam".
          format(processes, name, pwd))
os.system("add_read_count.rb {0}_scaffolds_mapped.sam {0}_scaffolds.fasta 150 > {0}_scaffolds.fasta.counted".
          format(name))
os.system("mv {0}_scaffolds.fasta.counted {0}_scaffolds.fasta".format(name))

# Scaffold summary information
os.system("contig_stats.pl -i {0}_scaffolds.fasta".format(name))

# Gene and Small RNA Prediction
os.system("pullseq -i {0}_scaffolds.fasta -m 1000 > {0}_scaffolds_min1000.fasta".format(name))
os.system("prodigal -i {0}_scaffolds_min1000.fasta -o {0}_scaffolds_min1000.fasta.genes "
          "-a {0}_scaffolds_min1000.fasta.genes.faa -d {0}_scaffolds_min1000.fasta.genes.fna -m -p meta".format(name))
os.system("16s.sh {0}_scaffolds_min1000.fasta >{0}_scaffolds_min1000.fasta.16s".format(name))
os.system("trnascan_pusher.rb -i {0}_scaffolds_min1000.fasta > /dev/null 2>&1".format(name))


# check if cluster is busy before submitting annotation
# if with long job list (>= 300 jobs), check each 30 minutes
while queue_is_busy(300):
    time.sleep(60)


# Annotation
os.system("cluster_usearch_wrev.rb -i {0}_scaffolds_min1000.fasta.genes.faa -k -d kegg".format(name))
os.system("cluster_usearch_wrev.rb -i {0}_scaffolds_min1000.fasta.genes.faa -k -d uni".format(name))
os.system("cluster_usearch_wrev.rb -i {0}_scaffolds_min1000.fasta.genes.faa -k -d uniprot".format(name))


# check if annotation is finished
# if not finished, check every minutes
def anno_is_finished():
    os.system('ls {0}*b6 > {0}.5.txt'.format(name))
    os.system("wc -l {0}.5.txt > {0}.6.txt".format(name))
    number = []
    anno = open('{0}.6.txt'.format(name), 'r')
    for line in anno.readlines():
        line = line.strip().split()
        number.append(line[0])
    return number[0] == '48'


while not anno_is_finished():
    time.sleep(60)

# Post-annotation
os.system("clean.rb {0}_scaffolds_min1000".format(name))
os.system("annolookup.py {0}_scaffolds_min1000.fasta.genes.faa-vs-kegg.b6.gz kegg > "
          "{0}_scaffolds_min1000.fasta.genes.faa-vs-kegg.b6+".format(name))
os.system("annolookup.py {0}_scaffolds_min1000.fasta.genes.faa-vs-uni.b6.gz kegg > "
          "{0}_scaffolds_min1000.fasta.genes.faa-vs-uni.b6+".format(name))
os.system("annolookup.py {0}_scaffolds_min1000.fasta.genes.faa-vs-uniprot.b6.gz kegg > "
          "{0}_scaffolds_min1000.fasta.genes.faa-vs-uniprot.b6+".format(name))

os.system('rm {0}.1.txt {0}.5.txt {0}.6.txt'.format(name))


# Send an email when this is done
if args.email:
    os.system("echo ggkbase preparation for {0} has been finished | mail -s ggkbase_prep_for_{0} {1}".
              format(name, args.email))
