import os
from nighttimelights_pipeline.utils import fetch_resource_size
local = '/tmp/SVDNB_npp_d20240125.rade9d.tif.local'
#local = '/work/tmp/ntl/SVDNB_npp_d20240125.rade9d.tif'
#latest = '/work/tmp/ntl/SVDNB_npp_d20240125.rade9d_latest.tif'
remote = 'https://eogdata.mines.edu/nighttime_light/nightly/rade9d/SVDNB_npp_d20240125.rade9d.tif'


print (os.path.getsize(local),  fetch_resource_size(remote))