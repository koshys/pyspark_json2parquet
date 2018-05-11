from pyspark_json2parquet import sparkapp
from pyspark_json2parquet import utils
from datetime import datetime
import distutils.dir_util
import glob, shutil
import sys
import os


def run_job(source_path, env='qa', date=None):
    """
    #    Get all the basic things in order
    """

    conf = sparkapp.get_conf()

    sql_context = sparkapp.get_sql_context()

    log = sparkapp.get_pyspark_logger()

    prop, s3state = utils.get_state(source_path, env, date)

    s3source = None
    s3dest = None
    source_files = []

    """
    #    get the prop and make sure its not in a locked state
    #    if the process running is hung, kill the pypark and rm the state_file from s3 XOR local
    #    for the prop['loc']['date']

    """

    if not prop:
        log.error('Job Properties not retrieved from utils.get_state(), env mishandled. Please let support know')
        sys.exit(1)
    if prop['has_lock']:
        log.error('Another process is running for {} at {}, verify that process is running and wait for it to complete'
                  .format(source_path, prop['loc']['clock']))
        sys.exit(1)
    else:
        prop['has_lock'] = True
    utils.set_state(prop)

    if prop['loc']['source_scheme'] == 's3' and not s3state:
        log.error('s3 state obj retrieved from utils.get_state(), env mishandled. Please let support know')
        sys.exit(1)

    """
    #    Get the file lists and associated byte size
    #    how many bytes are we talking about and full path list
    #    _bt --> before transaction
    #    _at --> after transaction
    """

    prop['total_bytes'], source_files = utils.get_size_list(prop, log, 'source')
    utils.set_state(prop)
    _, destination_files_bt = utils.get_size_list(prop, log, 'destination')
    destination_files_at = []

    """
    #   load the files into memory based on the conf['mem_cap_gb'] limit set
    #
    """

    df_mem_gb = 0.0
    prop['bytes_processed'] = 0
    sdfl = []
    sdf = None
    for f in source_files:

        if df_mem_gb >= conf['mem_cap_gb']:

            # add the list to sdf
            if len(sdfl) > 0:
                sdf = sdfl[0]
                for df in sdfl[1:]:
                    sdf = sdf.union(df)

            # generate parquet snappy
            sdf.repartition(2).write.parquet(prop['loc']['local_temp_path'], compression='snappy')

            # check if parquet files are successfully generated
            if not os.path.isfile(prop['loc']['local_temp_path'] + '_SUCCESS'):
                log.error('parquet file processing was not successful in ' + prop['loc']['local_temp_path'])

                # rollback changes
                _, destination_files_at = utils.get_size_list(prop, log, 'destination')
                utils.rollback(prop, log, destination_files_bt, destination_files_at)

                # update the state of the prop and unlock
                prop['has_lock'] = False
                utils.set_state(prop)
                sys.exit(1)

            # store it into the destination
            for temp_file in glob.glob(prop['loc']['local_temp_path'] + '*.snappy.parquet'):

                log.info('[snappy.parquet file] ' + temp_file)

                if prop['loc']['source_scheme'] == 's3':

                    # write to s3 destination from temp
                    try:
                        with open(temp_file, mode='rb') as sp:

                            tmp_content = sp.read()

                            utils.put_s3dest_file(prop, log,
                                                  prop['loc']['s3_destination_prefix'] +
                                                  temp_file[len(prop['loc']['local_temp_path']):]
                                                  , tmp_content)
                            sp.close()

                            tmp_content = None

                    except Exception:
                        # rollback changes
                        log.error('Rolling back ', sys.exc_info())
                        _, destination_files_at = utils.get_size_list(prop, log, 'destination')
                        utils.rollback(prop, log, destination_files_bt, destination_files_at)

                        # update the state of the prop and unlock
                        prop['has_lock'] = False
                        utils.set_state(prop)
                        sys.exit(1)

                elif prop['loc']['source_scheme'] == '':

                    shutil.copy(temp_file, prop['loc']['local_destination_path'])

            # update the state for the prop['bytes_processed']
            prop['bytes_processed'] += df_mem_gb * (1024 ** 3)
            utils.set_state(prop)

            # delete temp dir for parquet files distutils.dir_util.remove_tree('/tmp/test/test/hello/1234')
            distutils.dir_util.remove_tree(prop['loc']['local_temp_path'])

            # reset control
            df_mem_gb = 0.0
            sdfl = []
            sdf = None

        # add to sdf list
        df_mem_gb += f['bytes'] / (1024 ** 3)

        if prop['loc']['source_scheme'] == 's3':

            s3obj = utils.get_s3source_file(prop, log, f['file'])

            s3obj = s3obj.decode('utf-8').replace('}{', '}\n{')

            tmp_filename = prop['loc']['local_source_path_from_s3'] + (f['file'])[len(prop['loc']['s3_source_prefix']):]

            tmp_file = open(tmp_filename, 'w')
            tmp_file.write(s3obj)
            tmp_file.close()
            sdfl.append(sql_context.read.json(tmp_filename))
            tmp_filename = None
            tmp_file = None
            s3obj = None

        elif prop['loc']['source_scheme'] == '':

            sdfl.append(sql_context.read.json(f['file']))

        log.info('[FILES] ' + f['file'])

    # after loop add the list to sdf if capacity not reached
    if len(sdfl) > 0:
        sdf = sdfl[0]
        for df in sdfl[1:]:
            sdf = sdf.union(df)

        # generate parquet snappy
        sdf.repartition(2).write.parquet(prop['loc']['local_temp_path'], compression='snappy')

        # check if parquet files are successfully generated
        if not os.path.isfile(prop['loc']['local_temp_path'] + '_SUCCESS'):
            log.error('parquet file processing was not successful in ' + prop['loc']['local_temp_path'])

            # rollback changes
            _, destination_files_at = utils.get_size_list(prop, log, 'destination')
            utils.rollback(prop, log, destination_files_bt, destination_files_at)

            # update the state of the prop and unlock
            prop['has_lock'] = False
            utils.set_state(prop)
            sys.exit(1)

        # store it into the destination
        for temp_file in glob.glob(prop['loc']['local_temp_path'] + '*.snappy.parquet'):

            log.info('[snappy.parquet file] ' + temp_file)

            if prop['loc']['source_scheme'] == 's3':

                # write to s3 destination from temp
                try:
                    with open(temp_file, mode='rb') as sp:

                        tmp_content = sp.read()

                        utils.put_s3dest_file(prop, log,
                                              prop['loc']['s3_destination_prefix'] +
                                              temp_file[len(prop['loc']['local_temp_path']):]
                                              , tmp_content)
                        sp.close()

                        tmp_content = None

                except Exception:
                    # rollback changes
                    log.error('Rolling back ', sys.exc_info())
                    _, destination_files_at = utils.get_size_list(prop, log, 'destination')
                    utils.rollback(prop, log, destination_files_bt, destination_files_at)

                    # update the state of the prop and unlock
                    prop['has_lock'] = False
                    utils.set_state(prop)
                    sys.exit(1)

            elif prop['loc']['source_scheme'] == '':

                shutil.copy(temp_file, prop['loc']['local_destination_path'])

        # commit
        _, destination_files_at = utils.get_size_list(prop, log, 'destination')
        utils.commit(prop, log, destination_files_bt, destination_files_at)

        # update the state for the prop['bytes_processed']
        prop['bytes_processed'] += df_mem_gb * (1024 ** 3)
        utils.set_state(prop)

        # delete temp dir for parquet files distutils.dir_util.remove_tree('/tmp/test/test/hello/1234')
        distutils.dir_util.remove_tree(prop['loc']['local_temp_path'])
        sdfl = []
        sdf = None

    # update the state of the prop and unlock
    prop['has_lock'] = False
    prop['end_time'] = str(datetime.now().astimezone())
    utils.set_state(prop)

    log.info('[job_properties] ' + str(prop))
    log.info('[Files] ' + str(source_files))

    # pdf = sql_context.read.parquet(prop['loc']['local_destination_path'])

    # pdf.show()

    log.info('Total Memory ' + str(df_mem_gb))

