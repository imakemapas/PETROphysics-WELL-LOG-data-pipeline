# -*- coding: utf-8 -*-
"""
Created on Wed Apr  9 20:54:05 2025

@author: Theresa Rocco Pereira Barbosa
"""

from dlisio import dlis
import lasio
import pandas as pd
pd.set_option('display.max_row', 50)

f, *tail = dlis.load("C:/Users/Theresa/OneDrive/PESQUISA/LabMeg_Exxon/PMP_BC_WELL_LOGGING/original_data/dlis_data/1TP__0003__SC_1TP__0003__SC.dlis")
f.describe()

origin, *origin_tail = f.origins
origin.describe()

for frame in f.frames:
    
    for channel in frame.channels:
        if channel.name == frame.index:
            depth_units = channel.units
    
    print(f'Frame Name: \t\t {frame.name}')
    print(f'Index Type: \t\t {frame.index_type}')
    print(f'Depth Interval: \t {frame.index_min} - {frame.index_max} {depth_units}')
    print(f'Depth Spacing: \t\t {frame.spacing} {depth_units}')
    print(f'Direction: \t\t {frame.direction}')
    print(f'Num of Channels: \t {len(frame.channels)}')
    print(f'Channel Names: \t\t {str(frame.channels)}')
    print('\n\n')

las_file = lasio.LASFile()

well_name  = origin.well_name
field_name = origin.field_name
operator   = origin.company

las_file.well['WELL'] = lasio.HeaderItem('WELL', value = well_name)
las_file.well['FLD'] = lasio.HeaderItem('FLD', value = field_name)
las_file.well['COMP'] = lasio.HeaderItem('COMP', value = operator)

columns_to_extract = ['INDEX1423', 'DUMM']

frame = f.frames[0]

for channel in frame.channels:
    # If the channel name is in the list of channels to extract
    if channel.name in columns_to_extract:
        curves = channel.curves()

        # If the channel name is 'INDEX...', convert to 'DEPT' 
        if channel.name == 'INDEX1423':
            channel_name = 'DEPT'
            description = 'DEPTH'
            # If the units are 0.1 in then convert to metres
            if channel.units == '0.1 in':
                curves = curves * 0.00254 
                unit = 'm'
            else:
                unit = channel.units
        else:
            description = channel.long_name
            channel_name = channel.name
            unit = channel.units
        
        # Add the data to the LAS file
        las_file.append_curve(
            channel_name,
            curves,
            unit=unit,
            descr=description
        )

las_file.curves

las_file.write('C:/Users/Theresa/OneDrive/PESQUISA/LabMeg_Exxon/PMP_BC_WELL_LOGGING/DLIS_to_LAS_stage1/1TP_3_SC.las')
