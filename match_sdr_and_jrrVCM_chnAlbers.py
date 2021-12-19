# -*- coding: utf-8 -*-#
#-------------------------------------------------------------------------------
# Name:         match_sdr_and_jrrVCM.py
# Description:  match sdr and jrrVCM, generate the mosaic vcm raster of corresponding sdr
# Author:       'yc Hong'
# Date:         2020/11/12
#-------------------------------------------------------------------------------
import os
import re
import datetime
import gc
import arcpy

# 根据路径获取文件名（不包括尾缀） get filename without suffix
def get_filename_without_suffix(inputPath):
    pattern = re.compile(r'([^<>/\\\|:""\*\?]+)\.\w+$')
    data = pattern.findall(inputPath)
    if data:
        return data[0]

# 遍历文件路径，获取文件列表 Search all tif file, including subfolders
def search_tif_file(inDir):
    tif_fileList = []
    for root, dirs, files in os.walk(inDir, topdown=False):
        for name in files:
            if name.endswith(".tif"):
                tif_fileList.append(os.path.join(root, name))

    return tif_fileList

# get time range from file name sting in yyyymmddHHMMSS.S format
def getTimeRangefromVcmStr(fileNameStr):
    fileNameStr = fileNameStr.split("_")
    startTimeStr = fileNameStr[3][1:]
    endTimeStr = fileNameStr[4][1:]

    startTime = datetime.datetime.strptime(startTimeStr[:-1], "%Y%m%d%H%M%S")
    startTime_offset = datetime.timedelta(seconds=int(startTimeStr[-1]) * 0.1)
    startTime = startTime + startTime_offset # add seconds
    endTime = datetime.datetime.strptime(endTimeStr[:-1], "%Y%m%d%H%M%S")
    endTime_offset = datetime.timedelta(seconds=int(endTimeStr[-1]) * 0.1)
    endTime = endTime + endTime_offset

    del fileNameStr, startTimeStr, endTimeStr, startTime_offset, endTime_offset
    gc.collect()

    return (startTime, endTime)

def getTimeRangefromSdrStr(fileNameStr):
    fileNameStr = fileNameStr.split("_")
    fileDateStr = fileNameStr[2][1:]
    startTimeStr = fileNameStr[3][1:]
    endTimeStr = fileNameStr[4][1:]

    # fileDate = datetime.datetime.strptime(fileDateStr, "%Y%m%d")
    startTime = datetime.datetime.strptime(fileDateStr + startTimeStr[:-1], "%Y%m%d%H%M%S")
    startTime_offset = datetime.timedelta(seconds=int(startTimeStr[-1]) * 0.1)
    startTime = startTime + startTime_offset  # add seconds
    endTime = datetime.datetime.strptime(fileDateStr + endTimeStr[:-1], "%Y%m%d%H%M%S")
    endTime_offset = datetime.timedelta(seconds=int(endTimeStr[-1]) * 0.1)
    endTime = endTime + endTime_offset

    del fileNameStr, startTimeStr, endTimeStr, startTime_offset, endTime_offset, fileDateStr
    gc.collect()

    return (startTime, endTime)

# judge the inTime in the range of specific time
def inTimeRange(inTime, timeRange):
    """
    :param inTime: datetime in yyyymmddHHMMSS.S format
    :param timeRange: tuple of datetime in yyyymmddHHMMSS.S format
    :return:
    """
    if timeRange[0] <= inTime <= timeRange[1]:
        return True
    else:
        return False

def match_sdr_vcm(inSDR_Dir, inVCM_Dir, outMosaicVcm_Dir):
    sdrTif_list = search_tif_file(inSDR_Dir)
    vcmTif_list = search_tif_file(inVCM_Dir)

    # iter sdr tif
    for sdrTif_path in sdrTif_list:
        sdrTif_Name = get_filename_without_suffix(sdrTif_path)
        sdr_TimeRange = getTimeRangefromSdrStr(sdrTif_Name)
        tmp_vcmList = ""

        for vcmTif_path in vcmTif_list:
            vcmTif_Name = get_filename_without_suffix(vcmTif_path)
            vcm_TimeRange = getTimeRangefromVcmStr(vcmTif_Name)

            if inTimeRange(vcm_TimeRange[0], sdr_TimeRange) & inTimeRange(vcm_TimeRange[1], sdr_TimeRange):
                tmp_vcmList = tmp_vcmList + vcmTif_path + ";"

        outMosaicVcmName = sdrTif_Name + "_CM.tif"
        arcpy.MosaicToNewRaster_management(input_rasters=tmp_vcmList, output_location=outMosaicVcm_Dir,
                                           raster_dataset_name_with_extension=outMosaicVcmName, pixel_type="8_BIT_UNSIGNED",
                                           number_of_bands=1, mosaic_method="MINIMUM", mosaic_colormap_mode="FIRST") # cellsize=750,
        print (sdrTif_Name + " processed.")
    print ("All done.")

if __name__ == "__main__":
    inSdrDir = r"F:\Data\US_Hurricane\SDR\SDR_2020_Geotiff" # sdr geotiff所在文件夹
    inVcmDir = r"F:\Data\US_Hurricane\VCM\VCM_2020_Geotiff" # vcm geotiff所在文件
    outMosaicDir = r"F:\Data\US_Hurricane\Match\VCM_2020_mosaic" # 拼接的vcm所在文件

    arcpy.env.workspace = inVcmDir
    arcpy.env.extent = r"F:\Data\US_Hurricane\extent\USAMainland.shp" # 设定处理范围
    arcpy.env.snapRaster = r"F:\Data\US_Hurricane\Snap_Raster\snapraster.tif" # 控制像元偏移
    # arcpy.env.cellSize = 750

    match_sdr_vcm (inSdrDir, inVcmDir, outMosaicDir)