# -*- coding: utf-8 -*-#
#-------------------------------------------------------------------------------
# Name:         use_jrrVCM_maskSDR.py
# Description:
# Author:       'yc Hong'
# Date:         2020/11/12
#-------------------------------------------------------------------------------
import os
import re
import gc
import arcpy

# Check out the ArcGIS Spatial Analyst extension license
arcpy.CheckOutExtension("Spatial")

# get filename without suffix from absolute path
def get_filename_without_suffix(inputPath):
    pattern = re.compile(r'([^<>/\\\|:""\*\?]+)\.\w+$')
    data = pattern.findall(inputPath)
    if data:
        return data[0]

# Search all tif file, including subfolders
def search_tif_file(inDir):
    tif_fileList = []
    for root, dirs, files in os.walk(inDir, topdown=False):
        for name in files:
            if name.endswith(".tif"):
                tif_fileList.append(os.path.join(root, name))

    return tif_fileList

def maskCloud(inSDRTif_Dir, inVCMTif_Dir, outDir):
    sdrTif_List = search_tif_file(inSDRTif_Dir)
    vcmTif_List = search_tif_file(inVCMTif_Dir)

    for sdrTif_path in sdrTif_List:
        sdrName = get_filename_without_suffix(sdrTif_path)
        sdrNames = sdrName.split("_")
        sdrIdentityName = sdrNames[2] + "_" + sdrNames[3] + "_" + sdrNames[4] + "_" + sdrNames[5]
        for vcmTif_path in vcmTif_List:
            if sdrIdentityName in vcmTif_path:
                vcmRaster = arcpy.Raster(vcmTif_path)
                outSetNull = arcpy.sa.SetNull(vcmRaster != 0, 1) # outSetNull = arcpy.sa.SetNull(vcmRaster != 0, vcmRaster)
                # outPath = outDir + "\\" + sdrName + "_CM_Setnull.tif"
                # outSetNull.save(outPath)
                sdrRaster = arcpy.Raster(sdrTif_path)
                outExtractByMask = sdrRaster * outSetNull
                # outExtractByMask = arcpy.sa.ExtractByMask(sdrRaster, outSetNull)
                del outSetNull, sdrRaster, vcmRaster
                gc.collect()

                outPath = outDir + "\\" + sdrName + "_rCM.tif"
                outExtractByMask.save(outPath)
                print sdrName + " processed."
                del sdrName, sdrNames, sdrIdentityName
                gc.collect()
                break

if __name__ == "__main__":
    inSDRTif_Dir = r"F:\Data\US_Hurricane\SDR\SDR_2020_Geotiff" # sdr geotiff所在文件夹
    inVCMTif_Dir = r"F:\Data\US_Hurricane\Match\VCM_2020_mosaic" # 拼接的vcm所在文件
    outDir = r"F:\Data\US_Hurricane\MaskFree\MaskFree_2020" # 去云后的sdr geotiff输出文件夹

    arcpy.env.workspace = outDir
    arcpy.env.extent = r"F:\Data\US_Hurricane\extent\USAMainland.shp"  # 设定处理范围
    arcpy.env.snapRaster = r"F:\Data\US_Hurricane\Snap_Raster\snapraster.tif" # 控制像元偏移
    arcpy.env.cellSize = 750

    maskCloud(inSDRTif_Dir, inVCMTif_Dir, outDir)
    print("end")
