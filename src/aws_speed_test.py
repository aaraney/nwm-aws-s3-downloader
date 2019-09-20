#!/usr/local/bin/python3
from os.path import join
import sys
sys.path.append('.')
from chrtout_aws_downloader import *

# store_name = sys.argv[2]
# clipping_file = sys.argv[1]
# route_link = '/Users/austinraney/Box Sync/si/nwm/domains/sipsey_wilderness_cuahsi_subset/DOMAIN/1.2Route_Link.nc'
# parent_dir = '/Users/austinraney/Box Sync/si/nwm/domains/sipsey_wilderness_cuahsi_subset/FORCING'

aws_s3_list = aws_s3_file_list('CHRTOUT', '2017', month='03', day='01')
print(aws_s3_list)


# for i,f in enumerate(aws_s3_list):
#     df = aws_s3_file_to_xarray(f)
#     fn = '{}.nc'.format(aws_s3_list[i].split('/')[-1])
#
#     subset_df = subset_chrtout(df, route_link)
#     subset_df.to_netcdf(join(parent_dir, fn))

