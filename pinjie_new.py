# -*- coding: utf-8 -*-
"""
Created on Sun Nov  3 16:48:46 2019

@author: Song
"""

import datetime
import arcpy
from arcpy import env
from arcpy.sa import *

def create_assist_date(datestart = None,dateend = None):
	
	if datestart is None:
		datestart = '2021-01-01'
	if dateend is None:
		dateend = datetime.datetime.now().strftime('%Y-%m-%d')

	
	datestart=datetime.datetime.strptime(datestart,'%Y-%m-%d')
	dateend=datetime.datetime.strptime(dateend,'%Y-%m-%d')
	date_list = []
	date_list.append(datestart.strftime('%Y-%m-%d'))
	while datestart<dateend:
		
	    datestart+=datetime.timedelta(days=+1)
	    
	    date_list.append(datestart.strftime('%Y-%m-%d'))
	return date_list

env.workspace = r"F:\Data\US_Hurricane\MaskFree\MaskFree_2020\\" # 去云后的sdr geotiff所在文件夹
arcpy.env.extent = r"F:\Data\US_Hurricane\extent\USAMainland.shp"
arcpy.env.snapRaster = r"F:\Data\US_Hurricane\Snap_Raster\snapraster.tif"
outPath=r"F:\Data\US_Hurricane\Composite\Composite_2020\\" # 按天拼接后sdr geotiff输出文件夹
  
rasters = arcpy.ListRasters("*","TIF")
for i,ra in enumerate(rasters):
    rasters[i] = env.workspace + '\\' + ra

date = create_assist_date('2020-12-01','2020-12-31')

for i,d in enumerate(date):
    date[i] = d.replace('-','')

for day in date:
    match = 'd' + day + '_t'
    index = []
    for s in rasters:
        if match in s:
            index.append(s)	
    ras_list = ";".join(index)
	#print(ras_list)
    if ras_list == '':
        print day,'is nodata.'
        continue
    else:
        arcpy.MosaicToNewRaster_management(input_rasters=ras_list, output_location=outPath,
                                           raster_dataset_name_with_extension="mosaic_d" + day + '.tif',
                                           pixel_type='32_BIT_FLOAT',number_of_bands='1', mosaic_method='MEAN',
                                           mosaic_colormap_mode='FIRST')
        print day,'is done.'
print('All done.')