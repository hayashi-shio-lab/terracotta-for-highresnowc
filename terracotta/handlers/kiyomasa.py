"""handlers/kiyomasa.py

Handle /kiyomasa API endpoint.
"""

from typing import Sequence, Mapping, Union, Tuple, Optional, TypeVar, cast
from typing.io import BinaryIO
import traceback

import collections
from io import BytesIO
import math
from operator import and_

import numpy as np
from PIL import Image

from terracotta import get_settings, get_driver, image, xyz
from terracotta.profile import trace
from terracotta import exceptions

Number = TypeVar('Number', int, float)
RGBA = Tuple[Number, Number, Number, Number]


def get_png_stream(image):
    if image.dtype == 'uint8':
        mode = 'L'
    elif image.dtype == 'uint16':
        mode = 'LA'
    assert mode is not None, f'{image.dtype} is not supported.'
    img = Image.fromarray(image, mode=mode)
    sio = BytesIO()
    settings = get_settings()
    img.save(sio, 'png', compress_level=settings.PNG_COMPRESS_LEVEL)
    sio.seek(0)
    return sio


def get_tile_data_from_multi_cogs(keys: Union[Sequence[str], Mapping[str, str]],
                                  tile_xyz: Tuple[int, int, int] = None,
                                  tile_size: Tuple[int, int] = None):
    settings = get_settings()
    if tile_size is None:
        tile_size = settings.DEFAULT_TILE_SIZE
    driver = get_driver(settings.DRIVER_PATH, provider=settings.DRIVER_PROVIDER)
    tile_x, tile_y, tile_z = tile_xyz
    tile_data = np.ma.array(np.zeros(tile_size), mask=True)
    section_x_idx = driver.key_names.index('section_x')
    section_y_idx = driver.key_names.index('section_y')
    sections_x = keys[section_x_idx].split(',')
    sections_y = keys[section_y_idx].split(',')
    is_xyz_outside = True
    with driver.connect():
        futures = {}
        for x in sections_x:
            keys[section_x_idx] = x
            for y in sections_y:
                keys[section_y_idx] = y
                try:
                    metadata = driver.get_metadata(keys)
                except exceptions.DatasetNotFoundError:
                    continue
                wgs_bounds = metadata['bounds']
                if not xyz.tile_exists(wgs_bounds, tile_x, tile_y, tile_z):
                    continue
                futures[(x, y)] = xyz.get_tile_data(
                    driver, keys, tile_xyz, tile_size=tile_size,
                    asynchronous=True,
                )

        # print ('futures', futures)
        num_collected_tiles = 0
        for x in sections_x:
            for y in sections_y:
                f = futures.get((x,y))
                if f is None:
                    continue
                out = f.result()
                exc = f.exception()
                if exc != None:
                   raise exc
                tile_data.data[~out.mask] = out.data[~out.mask]
                tile_data.mask[~out.mask] = out.mask[~out.mask]
                num_collected_tiles += 1
        # if num_collected_tiles == 0:
        #    print (f'Tile {tile_z}/{tile_x}/{tile_y} is outside image bounds')
    return tile_data


@trace('pri60lv_handler')
def Pri60lv(keys: Union[Sequence[str], Mapping[str, str]],
            tile_xyz: Tuple[int, int, int] = None,
            tile_size: Tuple[int, int] = None) -> BinaryIO:
    """Return pri60lv image as PNG"""

    tile = get_tile_data_from_multi_cogs(keys, tile_xyz, tile_size)

    '''
      PNG(uint8) :    COG(uint16)   (precipitation      mm/h)
           1未満 :     0 -     1未満 (              0.01 mm/h未満)
     1 -  10未満 :     1 -    10未満 (0.01 mm/h -   0.10 mm/h未満) Unit = 0.01mm/h
    10 -  59未満 :    10 -   500未満 (0.10 mm/h -   5.00 mm/h未満) unit =  0.1mm/h
    59 - 254未満 :   500 - 20000未満 (5.00 mm/h - 200.00 mm/h未満) unit =    1mm/h
   254           : 20000 - 65535未満 ( 200 mm/h - 655.35 mm/h未満)
   255(uint8.max): 65535(uint16.max) (outside the data area)
    '''
    #out = np.where((   0 <= tile) & (tile <     1), 0                                 , tile)
    #out = np.where((   1 <=  out) & ( out <    10), out * 1                           ,  out)
    out = np.where((   10 <= tile) & (tile <   500), np.floor((tile -  10) *  0.1) + 10, tile)
    out = np.where((  500 <=  out) & ( out < 20000), np.floor(( out - 500) * 0.01) + 59,  out)
    out = np.where( 20000 <=  out                  , 254                               ,  out)
    nodata_value = np.iinfo(np.uint8).max
    out[tile.mask] = nodata_value
    out = out.astype(np.uint8)

    return get_png_stream(out)


@trace('pphw10_handler')
def Pphw10(keys: Union[Sequence[str], Mapping[str, str]],
            tile_xyz: Tuple[int, int, int] = None, *,
            tile_size: Tuple[int, int] = None) -> BinaryIO:
    """Return pphw10 image as PNG"""

    tile = get_tile_data_from_multi_cogs(keys, tile_xyz, tile_size)

    #nodata_value = np.iinfo(np.uint8).max
    #tile[tile.mask] = nodata_value
    out = tile.astype(np.uint8)

    return get_png_stream(out)


@trace('plts10_handler')
def Plts10(keys: Union[Sequence[str], Mapping[str, str]],
            tile_xyz: Tuple[int, int, int] = None, *,
            tile_size: Tuple[int, int] = None) -> BinaryIO:
    """Return plts10 image as PNG"""

    tile = get_tile_data_from_multi_cogs(keys, tile_xyz, tile_size)

    #nodata_value = np.iinfo(np.uint8).max
    #tile[tile.mask] = nodata_value
    out = tile.astype(np.uint8)

    return get_png_stream(out)


@trace('cwm_height_handler')
def CWM_height(keys: Union[Sequence[str], Mapping[str, str]],
            tile_xyz: Tuple[int, int, int] = None, *,
            tile_size: Tuple[int, int] = None) -> BinaryIO:
    """Return cwm_height image as PNG"""

    tile = get_tile_data_from_multi_cogs(keys, tile_xyz, tile_size)
    out = tile.astype(np.uint8)

    return get_png_stream(out)


@trace('cwm_period_handler')
def CWM_period(keys: Union[Sequence[str], Mapping[str, str]],
            tile_xyz: Tuple[int, int, int] = None, *,
            tile_size: Tuple[int, int] = None) -> BinaryIO:
    """Return cwm_period image as PNG"""

    tile = get_tile_data_from_multi_cogs(keys, tile_xyz, tile_size)
    out = tile.astype(np.uint8)

    return get_png_stream(out)


@trace('cwm_direction_handler')
def CWM_direction(keys: Union[Sequence[str], Mapping[str, str]],
            tile_xyz: Tuple[int, int, int] = None, *,
            tile_size: Tuple[int, int] = None) -> BinaryIO:
    """Return cwm_direction image as PNG"""

    tile = get_tile_data_from_multi_cogs(keys, tile_xyz, tile_size)
    tile[tile.mask] = np.iinfo(np.uint8).max    # nodata
    out = tile.astype(np.uint8)     # ZERO means north direction.

    return get_png_stream(out)


@trace('gwm_height_handler')
def GWM_height(keys: Union[Sequence[str], Mapping[str, str]],
            tile_xyz: Tuple[int, int, int] = None, *,
            tile_size: Tuple[int, int] = None) -> BinaryIO:
    """Return gwm_height image as PNG"""
    return CWM_height(keys, tile_xyz=tile_xyz, tile_size=tile_size)


@trace('gwm_period_handler')
def GWM_period(keys: Union[Sequence[str], Mapping[str, str]],
            tile_xyz: Tuple[int, int, int] = None, *,
            tile_size: Tuple[int, int] = None) -> BinaryIO:
    """Return gwm_period image as PNG"""
    return CWM_period(keys, tile_xyz=tile_xyz, tile_size=tile_size)


@trace('gwm_direction_handler')
def GWM_direction(keys: Union[Sequence[str], Mapping[str, str]],
            tile_xyz: Tuple[int, int, int] = None, *,
            tile_size: Tuple[int, int] = None) -> BinaryIO:
    """Return gwm_direction image as PNG"""
    return CWM_direction(keys, tile_xyz=tile_xyz, tile_size=tile_size)


@trace('msm_temp_handler')
def MSM_temp(keys: Union[Sequence[str], Mapping[str, str]],
            tile_xyz: Tuple[int, int, int] = None, *,
            tile_size: Tuple[int, int] = None) -> BinaryIO:
    """Return msm_temp image as PNG"""

    tile = get_tile_data_from_multi_cogs(keys, tile_xyz, tile_size)
    tile[tile.mask] = np.iinfo(np.uint16).max    # nodata
    out = tile.astype(np.uint16)

    return get_png_stream(out)


@trace('msm_rh_handler')
def MSM_rh(keys: Union[Sequence[str], Mapping[str, str]],
            tile_xyz: Tuple[int, int, int] = None, *,
            tile_size: Tuple[int, int] = None) -> BinaryIO:
    """Return msm_rh image as PNG"""

    tile = get_tile_data_from_multi_cogs(keys, tile_xyz, tile_size)
    tile[tile.mask] = np.iinfo(np.uint16).max    # nodata
    out = tile.astype(np.uint16)

    return get_png_stream(out)


@trace('hdw_temp_handler')
def HDW_temp(keys: Union[Sequence[str], Mapping[str, str]],
            tile_xyz: Tuple[int, int, int] = None, *,
            tile_size: Tuple[int, int] = None) -> BinaryIO:
    """Return hdw_temp image as PNG"""

    tile = get_tile_data_from_multi_cogs(keys, tile_xyz, tile_size)
    tile[tile.mask] = np.iinfo(np.uint16).max    # nodata
    out = tile.astype(np.uint16)

    return get_png_stream(out)


@trace('hdw_rh_handler')
def HDW_rh(keys: Union[Sequence[str], Mapping[str, str]],
            tile_xyz: Tuple[int, int, int] = None, *,
            tile_size: Tuple[int, int] = None) -> BinaryIO:
    """Return hdw_rh image as PNG"""

    tile = get_tile_data_from_multi_cogs(keys, tile_xyz, tile_size)
    tile[tile.mask] = np.iinfo(np.uint16).max    # nodata
    out = tile.astype(np.uint16)

    return get_png_stream(out)


@trace('hdw_precip_handler')
def HDW_precip(keys: Union[Sequence[str], Mapping[str, str]],
            tile_xyz: Tuple[int, int, int] = None, *,
            tile_size: Tuple[int, int] = None) -> BinaryIO:
    """Return hdw_precip image as PNG"""

    tile = get_tile_data_from_multi_cogs(keys, tile_xyz, tile_size)
    tile[tile.mask] = np.iinfo(np.uint16).max    # nodata
    out = tile.astype(np.uint16)

    return get_png_stream(out)


@trace('hdw_wind_speed_handler')
def HDW_wind_speed(keys: Union[Sequence[str], Mapping[str, str]],
            tile_xyz: Tuple[int, int, int] = None, *,
            tile_size: Tuple[int, int] = None) -> BinaryIO:
    """Return hdw_wind_speed image as PNG"""

    tile = get_tile_data_from_multi_cogs(keys, tile_xyz, tile_size)
    tile[tile.mask] = np.iinfo(np.uint16).max    # nodata
    out = tile.astype(np.uint16)

    return get_png_stream(out)


@trace('hdw_wind_dir_handler')
def HDW_wind_dir(keys: Union[Sequence[str], Mapping[str, str]],
            tile_xyz: Tuple[int, int, int] = None, *,
            tile_size: Tuple[int, int] = None) -> BinaryIO:
    """Return hdw_wind_dir image as PNG"""

    tile = get_tile_data_from_multi_cogs(keys, tile_xyz, tile_size)
    tile[tile.mask] = np.iinfo(np.uint8).max    # nodata
    out = tile.astype(np.uint8)

    return get_png_stream(out)


def int16_to_uint16(keys: Union[Sequence[str], Mapping[str, str]],
                        tile_xyz: Tuple[int, int, int],
                        tile_size: Tuple[int, int]) -> BinaryIO:
    tile = get_tile_data_from_multi_cogs(keys, tile_xyz, tile_size)
    tile = tile + np.iinfo(np.int16).max
    tile = np.clip(tile, 0, np.iinfo(np.uint16).max)
    tile[tile.mask] = np.iinfo(np.uint16).max    # nodata
    return tile.astype(np.uint16)
    

@trace('msm_u_component_of_wind_handler')
def MSM_u_component_of_wind(keys: Union[Sequence[str], Mapping[str, str]],
                        tile_xyz: Tuple[int, int, int] = None, *,
                        tile_size: Tuple[int, int] = None) -> BinaryIO:
    """Return msm_u_component_of_wind image as PNG"""
    return get_png_stream(int16_to_uint16(keys, tile_xyz, tile_size))


@trace('msm_v_component_of_wind_handler')
def MSM_v_component_of_wind(keys: Union[Sequence[str], Mapping[str, str]],
                        tile_xyz: Tuple[int, int, int] = None, *,
                        tile_size: Tuple[int, int] = None) -> BinaryIO:
    """Return msm_v_component_of_wind image as PNG"""
    return get_png_stream(int16_to_uint16(keys, tile_xyz, tile_size))
