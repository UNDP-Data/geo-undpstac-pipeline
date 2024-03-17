#!/bin/bash

lonmin=-180
lonmax=180
latmin=-65
latmax=75
tilesize=5

#currentdate=$(date +%Y%m%d)
#rooturl="https://eogdata.mines.edu/nighttime_light/nightly/rade9d_sunfiltered/"
#
#filename="SVDNB_npp_d"$currentdate".rade9d_sunfiltered.tif"
#url=$rooturl$filename
#echo "downloading file from "$url
#
## if tiles directory does not exist, create it
#if [ ! -d "./tiles" ]; then
#    mkdir tiles
#fi
## Checking if the file exists remotely
#http_status=$(curl -s -o /dev/null -w "%{http_code}" $url)
#
#if [ $http_status -eq 200 ]; then
#    echo "Downloading $filename..."
#    # Downloading the file using curl
#    curl -O $url
#else
#    echo "File $filename not found remotely."
#    exit 1
#fi

for x in   $(seq $lonmin $tilesize $(($lonmax-$tilesize)) )
do
    for y in $(seq $latmin $tilesize $(($latmax-$tilesize)))
    do
        x1=$(($x+$tilesize))
        y1=$(($y+$tilesize))

        r=$((($x-$lonmin)/$tilesize))
        c=$((($y-$latmin)/$tilesize))
        echo $x, $x1, $y, $y1, $r, $c
    done
done
#-130, -125, 25, 30, 10, 18
cmd="gdalwarp -te_srs EPSG:4326 -s_srs EPSG:4326 -t_srs EPSG:3857 -of COG -r near -ovr NONE -wo NUM_THREADS=ALL_CPUS -co BLOCKSIZE=256 -co OVERVIEWS=IGNORE_EXISTING -co COMPRESS=ZSTD -co PREDICTOR=YES -co OVERVIEW_RESAMPLING=NEAREST -co BIGTIFF=YES -multi -overwrite $filename ./tiles/SVDNB_npp_d$currentdate.rade9d_r.tif "
## Uncomment the next line to actually execute the command
#eval $cmd


# upload to azure blob where we arrange by date

## retile
#retile_cmd="gdal_retile.py -v -r bilinear -ps 256 256 -co TILED=YES -co COMPRESS=ZSTD -co BLOCKXSIZE=256 -co BLOCKYSIZE=256 -targetDir ./tiles ./tiles/SVDNB_npp_d$currentdate.rade9d_r""_c$c.tif"
#echo $retile_cmd
#eval $retile_cmd
#        break
#    done
#    break
# done
