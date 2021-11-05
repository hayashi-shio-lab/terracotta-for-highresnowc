"""handlers/highresnowc.py

Handle /highresnowc API endpoint.
"""

from typing import Sequence, Mapping, Union, Tuple, Optional, TypeVar, cast
from typing.io import BinaryIO

import collections
import math
import numpy as np
from PIL import Image
from io import BytesIO

from terracotta import get_settings, get_driver, image, xyz
from terracotta.profile import trace

Number = TypeVar('Number', int, float)
RGBA = Tuple[Number, Number, Number, Number]


@trace('highresnowc_handler')
def highresnowc(keys: Union[Sequence[str], Mapping[str, str]],
               tile_xyz: Tuple[int, int, int] = None, *,
               colormap: Union[str, Mapping[Number, RGBA], None] = None,
               stretch_range: Tuple[Number, Number] = None,
               tile_size: Tuple[int, int] = None) -> BinaryIO:
    """Return highresnowc image as PNG"""

    cmap_or_palette: Union[str, Sequence[RGBA], None]

    preserve_values = isinstance(colormap, collections.Mapping)

    settings = get_settings()
    if tile_size is None:
        tile_size = settings.DEFAULT_TILE_SIZE

    driver = get_driver(settings.DRIVER_PATH, provider=settings.DRIVER_PROVIDER)

    with driver.connect():
        metadata = driver.get_metadata(keys)
        tile_data = xyz.get_tile_data(
            driver, keys, tile_xyz,
            tile_size=tile_size, preserve_values=preserve_values
        )

    '''
        uint8  :    uint16   (precipitation      mm/h)
            0 :   0 -     0 (0.01 mm/h 未満         )
        1 -   9 :   1 -     9 (0.01 mm/h -   0.09 mm/h) Unit = 0.01mm/h (*  1)
    10 -  59 :  10 -   500 (0.10 mm/h -   4.99 mm/h) unit =  0.1mm/h (* 10)
    60 - 253 : 600 - 19900 (5.00 mm/h - 199.00 mm/h) unit =    1mm/h (*100)
    '''
    nodata_value = np.iinfo(np.uint16).max
    out = np.where(tile_data == nodata_value, 0, tile_data)
    #out = np.where((   0 <= out) & (out <       1), 0, out)
    #out = np.where((   1 <= out) & (out <      10), out * 1, out)
    out = np.where((   10 <= out) & (out <     500), np.floor((out -  10) *  0.1) + 10, out)
    out = np.where((  500 <= out) & (out <=  19900), np.floor((out - 500) * 0.01) + 59, out)
    out = np.where(19901 <= out, 253, out)
    out = out.astype(np.uint8)

    settings = get_settings()
    compress_level = settings.PNG_COMPRESS_LEVEL
    mode = 'L'
    img = Image.fromarray(out, mode=mode)

    sio = BytesIO()
    img.save(sio, 'png', compress_level=compress_level)
    sio.seek(0)
    return sio
