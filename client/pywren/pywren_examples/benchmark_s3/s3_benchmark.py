import time
import boto3 
import uuid
import numpy as np
import time
import pywren
import subprocess
import logging
import sys
import boto3
import hashlib
import cPickle as pickle
import uuid
import click
# this is in general a bad idea, but oh well. 
import sys
sys.path.append("../")
import exampleutils

@click.group()
def cli():
    pass


def write(bucket_name, mb_per_file, number, key_prefix, 
          region):

    def run_command(key_name):
        bytes_n = mb_per_file * 1024**2
        d = exampleutils.RandomDataGenerator(bytes_n)

        client = boto3.client('s3', region)
        t1 = time.time()
        client.put_object(Bucket=bucket_name, 
                          Key = key_name,
                          Body=d)
        t2 = time.time()


        mb_rate = bytes_n/(t2-t1)/1e6
        return t1, t2, mb_rate

    wrenexec = pywren.default_executor(shard_runtime=True)

    # create list of random keys
    keynames = [ key_prefix + str(uuid.uuid4().get_hex().upper()) for _ in range(number)]
    futures = wrenexec.map(run_command, keynames)

    results = [f.result() for f in futures]
    run_statuses = [f.run_status for f in futures]
    invoke_statuses = [f.invoke_status for f in futures]



    res = {'results' : results, 
           'run_statuses' : run_statuses, 
           'bucket_name' : bucket_name, 
           'keynames' : keynames, 
           'invoke_statuses' : invoke_statuses}
    return res


def read(bucket_name, number, 
         keylist_raw, read_times, region):
    
    blocksize = 1024*1024

    def run_command(key):
        client = boto3.client('s3', region)

        m = hashlib.md5()
        bytes_read = 0

        t1 = time.time()
        for i in range(read_times):
            obj = client.get_object(Bucket=bucket_name, Key=key)

            fileobj = obj['Body']

            buf = fileobj.read(blocksize)
            while len(buf) > 0:
                bytes_read += len(buf)
                m.update(buf)
                buf = fileobj.read(blocksize)
        t2 = time.time()

        a = m.hexdigest()
        mb_rate = bytes_read/(t2-t1)/1e6
        return t1, t2, mb_rate, bytes_read, a

    wrenexec = pywren.default_executor(shard_runtime=True)
    if number == 0:
        keylist = keylist_raw
    else:
        keylist = [keylist_raw[i % len(keylist_raw)]  for i in range(number)]

    futures = wrenexec.map(run_command, keylist)

    results = [f.result() for f in futures]
    run_statuses = [f.run_status for f in futures]
    invoke_statuses = [f.invoke_status for f in futures]
    res = {'results' : results, 
           'run_statuses' : run_statuses, 
           'invoke_statuses' : invoke_statuses}
    return res


@cli.command('write')
@click.option('--bucket_name', help='bucket to save files in')
@click.option('--mb_per_file', help='MB of each object in S3', type=int)
@click.option('--number', help='number of files', type=int)
@click.option('--key_prefix', default='', help='S3 key prefix')
@click.option('--outfile', default='s3_benchmark.write.output.pickle', 
              help='filename to save results in')
@click.option('--region', default='us-west-2', help="AWS Region")
def write_command(bucket_name, mb_per_file, number, key_prefix, region, outfile):
    res = write(bucket_name, mb_per_file, number, key_prefix, region)
    pickle.dump(res, open(outfile, 'wb'))

@cli.command('read')
@click.option('--key_file', default=None, help="filename generated by write command, which contains the keys to read")
@click.option('--number', help='number of objects to read, 0 for all', type=int, default=0)
@click.option('--outfile', default='s3_benchmark.read.output.pickle', 
              help='filename to save results in')
@click.option('--read_times', default=1, help="number of times to read each s3 key")
@click.option('--region', default='us-west-2', help="AWS Region")
def read_command(key_file, number, outfile, 
                 read_times, region):

    d = pickle.load(open(key_file, 'rb'))
    bucket_name = d['bucket_name']
    keynames = d['keynames']
    res = read(bucket_name, number, keynames, 
               read_times, region)
    pickle.dump(res, open(outfile, 'wb'))

if __name__ == '__main__':
    cli()
