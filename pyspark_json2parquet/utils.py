from urllib.parse import urlparse
from functools import lru_cache
from datetime import datetime
import distutils.dir_util
import sys
import os
import json
from pyspark_json2parquet import sparkapp
from pyspark_json2parquet import s3handler

'''
   . important location variables that drive the job are returned as a dict
   . location full paths 
   . local paths are always under /tmp/
   . cache for the length of the spark job, acts like a singleton
'''


@lru_cache(maxsize=1)
def get_loc_properties(source_path, env, date=None):
    # figure out the full paths
    now = datetime.now().astimezone()
    clock = now
    if date:
        now = (datetime.strptime(date, '%Y-%m-%d %H')).astimezone()

    day = str(now.day).zfill(2) + '/'

    day_dest = 'day=' + day

    hour = str(now.hour).zfill(2) + '/'

    hour_dest = 'hour=' + hour

    month_src = str(now.month).zfill(2) + '/'

    month_dest = 'month=' + month_src

    year_src = str(now.year).zfill(4) + '/'

    year_dest = 'year=' + year_src

    pid = os.getpid()

    if env == 'prod':
        destination_path = sparkapp.get_conf()['destination']
    elif env == 'qa':
        destination_path = sparkapp.get_conf()['destination_qa']
    elif env == 'dev':
        destination_path = sparkapp.get_conf()['destination_dev']
    else:
        # should never reach here
        sys.exit(1)

    src_url_p = urlparse(source_path)
    dst_url_p = urlparse(destination_path)
    src_path = source_path
    src_prefix = ''
    dst_path = dst_url_p.netloc + dst_url_p.path[1:]
    s3_dst_bucket = ''
    dst_prefix = ''
    state_prefix = ''
    forward_slash = ''
    if src_url_p.scheme == 's3':
        src_path = src_url_p.netloc + src_url_p.path
        src_prefix = src_url_p.path[1:] + year_src + month_src + day + hour
        dst_prefix = src_path + year_dest + month_dest + day + hour
        s3_dst_bucket = dst_url_p.netloc
        state_prefix = 'processing/state/' + src_path
        forward_slash = '/'

    src = '/tmp' + forward_slash + src_path + year_src + month_src + day + hour

    dest = '/tmp/' + dst_path + forward_slash + src_path + year_dest + month_dest + day_dest + hour_dest

    state = '/tmp/' + dst_path + '/processing/state' + forward_slash + src_path

    state_file = 'state_file_' + str(now.year).zfill(4) + '_' + str(now.month).zfill(2) + '_' \
                 + str(now.day).zfill(2) + '_' + str(now.hour).zfill(2) + '.json'

    temp = '/tmp/tmp' + forward_slash + src_path + str(pid) + '/'

    s3temp = '/tmp/tmp' + forward_slash + src_path + 's3/' + str(pid) + '/'

    # make directories as needed
    distutils.dir_util.mkpath(src)
    distutils.dir_util.mkpath(dest)
    distutils.dir_util.mkpath(state)
    distutils.dir_util.mkpath(s3temp)

    return {
        'clock': str(clock),
        'date': str(now),
        'source_scheme': src_url_p.scheme,
        's3_source_bucket': src_url_p.netloc,
        's3_source_prefix': src_prefix,
        'local_source_path': src,
        's3_destination_bucket': s3_dst_bucket,
        's3_destination_prefix': dst_prefix,
        'local_destination_path': dest,
        's3_state_prefix': state_prefix,
        'state_filename': state_file,
        'local_state_path': state,
        'local_temp_path': temp,
        'local_source_path_from_s3': s3temp
    }


'''
    . always returns the state in a locked state if the state does not exist
    . returns the state from file as is if the state file exists
'''


def get_state(source_path, env, date=None):
    loc = get_loc_properties(source_path, env, date)
    log = sparkapp.get_pyspark_logger()

    s = dict(
        has_lock=False,
        bytes_processed=None,
        total_bytes=None,
        start_time=loc['clock'],
        end_time=None
    )
    s['loc'] = loc
    s3state = None

    if loc['source_scheme'] == 's3':

        try:
            s3state = s3handler.S3Handler(bucket=loc['s3_destination_bucket'],
                                          logger=log,
                                          prefix=loc['s3_state_prefix'])

            stemp = json.loads(s3state.get_object(key=(loc['s3_state_prefix'] + loc['state_filename'])))
            if stemp['has_lock']:
                s = stemp

            s['loc'] = loc


        except s3handler.S3NoSuchKey:
            # in that case, write to it
            try:
                resp = s3state.put_object(key=(loc['s3_state_prefix'] + loc['state_filename']),
                                          body=json.dumps(s)
                                          )

            except Exception:
                log.error('Unexpected error on write to {} {}'
                          .format(loc['s3_state_prefix'] + loc['state_filename']
                                  , sys.exc_info())
                          )


        except Exception:
            log.error('Unexpected Error getting {} Object {} with error {}'
                      .format(loc['s3_destination_bucket'],
                              (loc['s3_state_prefix'] + loc['state_filename']),
                              sys.exc_info())
                      )


    else:
        # read state from local file
        try:
            with open(loc['local_state_path'] + loc['state_filename'], mode='r') as f:
                stemp = json.load(f)
                if stemp['has_lock']:
                    s = stemp

                s['loc'] = loc
                f.close()

        except FileNotFoundError:
            # in that case , write to it
            try:
                with open(loc['local_state_path'] + loc['state_filename'], mode='w') as f:
                    if f.writable():
                        json.dump(s, f)
                        f.close()
            except Exception:
                log.error('Unexpected error on write to {} {}'
                          .format(loc['local_state_path'] + loc['state_filename']
                                  , sys.exc_info())
                          )
        except Exception:
            log.error('Unexpected error on read from {} {}'
                      .format(loc['local_state_path'] + loc['state_filename']
                              , sys.exc_info())
                      )
    return s, s3state


'''
    . state object s -- write -- will throw exception straight to called 
    . and thereby shutdown spark job 
'''


def set_state(s):
    log = sparkapp.get_pyspark_logger()

    if s['loc']['source_scheme'] == 's3':

        s3state = s3handler.S3Handler(bucket=s['loc']['s3_destination_bucket'],
                                      logger=log,
                                      prefix=s['loc']['s3_state_prefix'])

        try:
            resp = s3state.put_object(key=(s['loc']['s3_state_prefix'] + s['loc']['state_filename']),
                                      body=json.dumps(s)
                                      )

        except Exception:
            log.error('Unexpected error on write to {} {}'
                      .format(s['loc']['s3_state_prefix'] + s['loc']['state_filename']
                              , sys.exc_info())
                      )
            raise Exception

    else:
        try:
            with open(s['loc']['local_state_path'] + s['loc']['state_filename'], mode='w') as f:
                if f.writable():
                    json.dump(s, f)
                    f.close()
        except:
            log.error('Unexpected error on write to {} {}'
                      .format(s['loc']['local_state_path'] + s['loc']['state_filename']
                              , sys.exc_info())
                      )
            raise Exception


'''
    . get the size of all files to process
    . state object s 
    . logger object
    . locatiion -- 'source' or 'destination'
    . return size and list while we are at it
'''


def get_size_list(s, log, location='source'):
    byte_size = 0
    flist = []
    f = {}
    s3loc = None

    # if state is locked and ready
    if s['has_lock']:

        if s['loc']['source_scheme'] == 's3':

            if location == 'source':

                s3loc = s3handler.S3Handler(bucket=s['loc']['s3_source_bucket'],
                                            logger=log,
                                            prefix=s['loc']['s3_source_prefix'])
            elif location == 'destination':

                s3loc = s3handler.S3Handler(bucket=s['loc']['s3_destination_bucket'],
                                            logger=log,
                                            prefix=s['loc']['s3_destination_prefix'])

            if s3loc is not None:
                objects = s3loc.list_bucket_prefix('')
                if objects is not None:
                    for k in objects:
                        f['file'] = k['Key']
                        f['bytes'] = k['Size']
                        flist.append(f)
                        byte_size += f['bytes']
                        f = {}

        elif s['loc']['source_scheme'] == '':

            if location == 'source':

                for file in os.listdir(s['loc']['local_source_path']):
                    f['file'] = s['loc']['local_source_path'] + os.fsdecode(file)
                    f['bytes'] = os.path.getsize(f['file'])
                    flist.append(f)
                    byte_size += f['bytes']
                    f = {}

            elif location == 'destination':

                for file in os.listdir(s['loc']['local_destination_path']):
                    f['file'] = s['loc']['local_destination_path'] + os.fsdecode(file)
                    f['bytes'] = os.path.getsize(f['file'])
                    flist.append(f)
                    byte_size += f['bytes']
                    f = {}

    return byte_size, flist


'''
    Only delete from the destination bucket
    recoverable if accidentally deleted from destination bucket
    k -> key
    f -> local binary file object
'''


def put_s3dest_file(s, log, k, f):
    if s['has_lock'] and s['loc']['source_scheme'] == 's3':

        s3loc = s3handler.S3Handler(bucket=s['loc']['s3_destination_bucket'],
                                    logger=log,
                                    prefix=s['loc']['s3_destination_prefix'])

        try:

            resp = s3loc.put_object(key=k, body=f)

        except Exception:
            log.error('Unexpected error on s3 put to {} {}'
                      .format(s['loc']['s3_destination_bucket'] + '/' + k
                              , sys.exc_info())
                      )
            raise Exception


'''
    Only get from the source bucket
    k -> key
    return -> contents or None
'''


def get_s3source_file(s, log, k):
    if s['has_lock'] and s['loc']['source_scheme'] == 's3':

        s3loc = s3handler.S3Handler(bucket=s['loc']['s3_source_bucket'],
                                    logger=log,
                                    prefix=s['loc']['s3_source_prefix'])

        try:

            resp = s3loc.get_object(key=k)

            return resp

        except Exception:
            log.error('Unexpected error on s3 get_object from {} {}'
                      .format(s['loc']['s3_source_bucket'] + '/' + k
                              , sys.exc_info())
                      )
            raise Exception

    else:

        return None


'''
    . s --> state
    . log --> log object
    . bt --> file list from destination before transaction
    . at --> file list from destination after transaction

    Commit --> means remove files from bt 
'''


def commit(s, log, bt, at):
    bt_list = [f['file'] for f in bt]
    at_list = [f['file'] for f in at]
    new_files = list(set(at_list) - set(bt_list))

    # make sure that the state is in the locked state and
    # that there are new files
    if s['has_lock'] and len(new_files) > 0:

        for f in bt_list:

            if s['loc']['source_scheme'] == 's3':

                s3loc = s3handler.S3Handler(bucket=s['loc']['s3_destination_bucket'],
                                            logger=log,
                                            prefix=s['loc']['s3_destination_prefix'])

                try:
                    resp = s3loc.delete_object(key=f)

                except Exception:
                    log.error('Unexpected error on s3 delete to {} {}'
                              .format(s['loc']['s3_destination_prefix'] + f
                                      , sys.exc_info())
                              )
                    raise Exception

            elif s['loc']['source_scheme'] == '':

                os.remove(f)

        log.info('Commit successful')


'''
    . s --> state
    . log --> log object
    . bt --> file list from destination before transaction
    . at --> file list from destination after transaction
    Rollback --> means remove new files only and keep old files from bt --> list(set(at) - set(bt)) 
'''


def rollback(s, log, bt, at):
    bt_list = [f['file'] for f in bt]
    at_list = [f['file'] for f in at]
    new_files = list(set(at_list) - set(bt_list))

    # make sure that the state is in the locked state and
    # that there are new files
    if s['has_lock'] and len(new_files) > 0:

        for f in new_files:

            if s['loc']['source_scheme'] == 's3':

                s3loc = s3handler.S3Handler(bucket=s['loc']['s3_destination_bucket'],
                                            logger=log,
                                            prefix=s['loc']['s3_destination_prefix'])

                try:
                    resp = s3loc.delete_object(key=f)

                except Exception:
                    log.error('Unexpected error on s3 delete to {} {}'
                              .format(s['loc']['s3_destination_prefix'] + f
                                      , sys.exc_info())
                              )
                    raise Exception

            elif s['loc']['source_scheme'] == '':

                os.remove(f)

        log.info('Rollback successful')

