"""handlers/kiyomasa.py

Handle /kiyomasa API endpoint.
"""

from typing import Sequence, Mapping, Union, Tuple, Optional, TypeVar, cast
from typing.io import BinaryIO

import collections
import math
import numpy as np
from PIL import Image
from io import BytesIO
from operator import and_

from terracotta import get_settings, get_driver, image, xyz
from terracotta.profile import trace
from terracotta import exceptions

Number = TypeVar('Number', int, float)
RGBA = Tuple[Number, Number, Number, Number]


def get_png_stream(uint8_value):
    img = Image.fromarray(uint8_value, mode='L')
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
        for x in sections_x:
            keys[section_x_idx] = x
            for y in sections_y:
                keys[section_y_idx] = y
                try:
                    metadata = driver.get_metadata(keys)
                    wgs_bounds = metadata['bounds']
                    if not xyz.tile_exists(wgs_bounds, tile_x, tile_y, tile_z):
                        continue
                    partial_data = xyz.get_tile_data(
                                    driver, keys, tile_xyz,
                                    tile_size=tile_size
                    )
                    tile_data.data[~partial_data.mask] = partial_data.data[~partial_data.mask]
                    tile_data.mask[~partial_data.mask] = partial_data.mask[~partial_data.mask]
                    is_xyz_outside = False
                except:
                    continue
    if is_xyz_outside:
        raise exceptions.TileOutOfBoundsError(
            f'Tile {tile_z}/{tile_x}/{tile_y} is outside image bounds'
        )
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
    #nodata_value = np.iinfo(np.uint8).max
    #out[tile.mask] = nodata_value
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

    #nodata_value = np.iinfo(np.uint8).max
    #tile[tile.mask] = nodata_value
    out = tile.astype(np.uint8)

    return get_png_stream(out)


@trace('cwm_direction_handler')
def CWM_direction(keys: Union[Sequence[str], Mapping[str, str]],
            tile_xyz: Tuple[int, int, int] = None, *,
            tile_size: Tuple[int, int] = None) -> BinaryIO:
    """Return cwm_direction image as PNG"""

    tile = get_tile_data_from_multi_cogs(keys, tile_xyz, tile_size)

    #nodata_value = np.iinfo(np.uint8).max
    #tile[tile.mask] = nodata_value
    out = tile.astype(np.uint8)

    return get_png_stream(out)
