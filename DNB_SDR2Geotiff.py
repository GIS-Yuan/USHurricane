import h5py
import numpy as np
from pyresample import geometry
import os
import xarray as xr
import dask.array as da
from satpy import Scene
from satpy.utils import debug_on
import argparse
from pyresample import get_area_def
# import matplotlib.pyplot as plt
# from pylab import *
from pyresample import create_area_def
from PIL import Image
import datetime
import warnings
import os
import gc

warnings.filterwarnings("ignore")
# 读取SDR文件中的Radiance、QF1_VIIRSDNBSDR以及SDR_GEO中的Longitude_TC、Latitude_TC、QF2_VIIRSSDRGEO、
# SolarZenithAngle、QF1_SCAN_VIIRSSDRGEO、LunarZenithAngle
def read_h5(sdr_data_path, SDR_names, SDR_GEO_names):
    with h5py.File(sdr_data_path, 'r') as sdr_file:
        GROUP_DNB_SDR = dict()
        GROUP_DNB_SDR_GEO = dict()

        if len(SDR_names) != 0:
            for SDR_name in SDR_names:
                temp_subdataset = sdr_file.get(SDR_name)
                if temp_subdataset is None:
                    print("The subdataset:%s don't exist." % (SDR_name))
                    continue
                GROUP_DNB_SDR[SDR_name] = temp_subdataset[()]
                del temp_subdataset

        if len(SDR_GEO_names) != 0:
            for SDR_GEO_name in SDR_GEO_names:
                temp_subdataset = sdr_file.get(SDR_GEO_name)
                if temp_subdataset is None:
                    print("The subdataset:%s don't exist." % (SDR_GEO_name))
                    continue
                GROUP_DNB_SDR_GEO[SDR_GEO_name] = temp_subdataset[()] # temp_subdataset.value
                del temp_subdataset

    return GROUP_DNB_SDR, GROUP_DNB_SDR_GEO

def batch_pro(sdr_input_dir, SDR_out_dir):
    file_list = os.listdir(sdr_input_dir)
    h5_file_list = []
    # 防止出现非h5文件，所以对读出来的文件过滤一下
    for temp_file in file_list:
        if temp_file.endswith('.h5'):
            h5_file_list.append(sdr_input_dir + "\\" + temp_file)

    # 用于在h5文件中提取相应数据的关键字
    SDR_names = ["/All_Data/VIIRS-DNB-SDR_All/Radiance", "/All_Data/VIIRS-DNB-SDR_All/QF1_VIIRSDNBSDR"]
    SDR_GEO_names = ["/All_Data/VIIRS-DNB-GEO_All/Longitude_TC", "/All_Data/VIIRS-DNB-GEO_All/Latitude_TC",
                     "/All_Data/VIIRS-DNB-GEO_All/QF2_VIIRSSDRGEO", "/All_Data/VIIRS-DNB-GEO_All/SolarZenithAngle",
                     '/All_Data/VIIRS-DNB-GEO_All/QF1_SCAN_VIIRSSDRGEO', '/All_Data/VIIRS-DNB-GEO_All/LunarZenithAngle']

    for h5_file in h5_file_list:
        # sdr_radiance_filter(h5_file, SDR_names, SDR_GEO_names, SDR_out_dir)
        GROUP_DNB_SDR, GROUP_DNB_SDR_GEO = read_h5(h5_file, SDR_names, SDR_GEO_names)
        sdr_output_name = os.path.basename(h5_file)
        sdr_output_name = sdr_output_name.split('.')[0]

if __name__ == "__main__":
    input_sdr_dir = r"F:\Data\US_Hurricane\SDR\SDR_2020" # sdr的存储文件夹
    output_sdr_dir = r"F:\Data\US_Hurricane\SDR\SDR_2020_Geotiff" # 输出文件夹；输出是剔除了边缘噪声、阳光、月光等影响的Radiance数据，格式为geotiff
    batch_pro(input_sdr_dir, output_sdr_dir)

    print("Complete")