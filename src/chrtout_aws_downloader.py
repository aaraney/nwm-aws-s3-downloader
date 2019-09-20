#!/usr/local/bin/python3

"""
Requires h5netcdf, s3fs

"""
try:
    import h5netcdf
    import s3fs
    import netCDF4
    import xarray as xr
except ModuleNotFoundError as e:
    print("Missing library requests.\nPlease install it using the command `pip3 install requests`.\n")
    raise ModuleNotFoundError from e


def aws_s3_file_list(product, year, month='*', day='*', hour='*',
                     bucket='noaa-nwm-retro-v2.0-pds/full_physics', s3fs_object=None):
    """
    This function returns a list of file paths stored in a bucket on aws s3.

    For a description of the nwm-archive please see this site: https://docs.opendata.aws/nwm-archive/readme.html

    product: one of the following strings - CHRTOUT, LAKEOUT, LDASOUT, RTOUT
    year: string yyyy format (i.e., 2019, 19*, 201*)
    month: string mm format (i.e., 01) default *
    day: string dd format (i.e., 02) default *
    hour: string 24 hh format (i.e., 23) default *

    Other aws s3 nwm buckets:
        - noaa-nwm-retro-v2.0-pds/long_range
        - nwm-archive

    Ex:
        chrt_list = aws_s3_file_list('CHRTOUT', '2017', month='02', day='01')
        print(chrt_list)
        > ['noaa-nwm-retro-v2.0-pds/full_physics/2017/201702010000.CHRTOUT_DOMAIN1.comp',
           'noaa-nwm-retro-v2.0-pds/full_physics/2017/201702010100.CHRTOUT_DOMAIN1.comp',..
           'noaa-nwm-retro-v2.0-pds/full_physics/2017/201702012300.CHRTOUT_DOMAIN1.comp']
    """
    if s3fs_object:
        s3 = s3fs_object
    else:
        s3 = s3fs.S3FileSystem(anon=True, default_fill_cache=False)

    # Globstar search term: e.g., nwm-archive/2018/01*2300.CHRTOUT*
    s3_glob_search = '{0}/{1}/{1}{2}{3}{4}00.{5}*'.format(bucket,year, month, day, hour, product)

    return s3.glob(s3_glob_search)

def aws_s3_file_to_xarray(s3_store_location, s3fs_object=None):
    """
    This function takes an netcdf file aws store location
    (i.e., 'noaa-nwm-retro-v2.0-pds/full_physics/2017/201702010000.CHRTOUT_DOMAIN1.comp')
    and returns an xarray object stored in memory.

    Ex:
        for f in s3_store_location_list:
            df = aws_s3_file_to_xarray(f)
            # do something to the data
            # store the data etc.
    """

    if s3fs_object:
        s3 = s3fs_object
    else:
        s3 = s3fs.S3FileSystem(anon=True, default_fill_cache=False)

    # Retrieve file from s3
    file = s3.open(s3_store_location)

    # Read into netcdf object from memory. Use name from retrieved file as netcdf name
    netcdf_object = netCDF4.Dataset(file.info()['name'].split('/')[-1], memory=file.read())

    store = xr.backends.NetCDF4DataStore(netcdf_object)

    return xr.open_dataset(store)


if __name__ == '__main__':
    aws_s3_url = 'http://noaa-nwm-retro-v2.0-pds.s3.amazonaws.com/'

    s3 = s3fs.S3FileSystem(anon=True, default_fill_cache=False)

    file_list = aws_s3_file_list('LDASOUT', '1995', month='02', day='01')

    for file in file_list:
        url_prefix = '/'.join(file.split('/')[1:])
        print('{}{}'.format(aws_s3_url, url_prefix))

# s3 = boto3.client('s3')
# nwm-archive
# s3.download_file('nwm-archive','2003/200305011200.CHRTOUT_DOMAIN1.comp','/Users/aaraney/Desktop/chrtout.nc')
# print(s3.list_buckets())

# url = 'https://nwm-archive.s3.amazonaws.com/2003/200305011200.CHRTOUT_DOMAIN1.comp'
# call_back = nwm_archive_downloader('CHRTOUT', '2018', '01', '01', '01')

# s = time.time()
# import multiprocessing as mp
# pool = mp.Pool(mp.cpu_count())
#
# l = pool.map(aws_s3_file_to_xarray, chrt_list)
# pool.close()
# e = time.time()
# print(e-s)
# x = aws_s3_file_to_xarray(chrt_list[0], s3fs_object=s3)

# s3 = s3fs.S3FileSystem(anon=True, default_fill_cache=False)
# netCDF4.Dataset('tasmin_day_BCSD_rcp45_r1i1p1_CESM1-BGC_2073', memory=res.content)
# x = netCDF4.Dataset(f.info()['name'].split('/')[-1], memory=f.read())
# name = f.info()['name'].split('/')[-1]
# store = xr.backends.NetCDF4DataStore(x)
# ds = xr.open_dataset(store)
# import xarray as xr
# x = xr.open_dataset(call_back)