"""server/kiyomasa.py

Flask route to handle /kiyomasa calls.
"""

from typing import Any, Mapping, Dict, Tuple
import json

from marshmallow import (Schema, fields, validate, validates_schema,
                         pre_load, ValidationError, EXCLUDE)
from flask import request, send_file, Response

from terracotta.server.flask_api import TILE_API
from terracotta.cmaps import AVAILABLE_CMAPS

import terracotta.handlers.kiyomasa as handlers


class KiyomasaQuerySchema(Schema):
    keys = fields.String(required=True, description='Keys identifying dataset, in order')
    tile_z = fields.Int(required=True, description='Requested zoom level')
    tile_y = fields.Int(required=True, description='y coordinate')
    tile_x = fields.Int(required=True, description='x coordinate')


class KiyomasaOptionSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    tile_size = fields.List(
        fields.Integer(), validate=validate.Length(equal=2), example='[256,256]',
        description='Pixel dimensions of the returned PNG image as JSON list.'
    )


@TILE_API.route('/kiyomasa/<path:keys>/<int:tile_z>/<int:tile_x>/<int:tile_y>.png',
                methods=['GET'])
def get_kiyomasa(tile_z: int, tile_y: int, tile_x: int, keys: str) -> Response:
    """Return kiyomasa PNG image of requested tile
    ---
    get:
        summary: /kiyomasa (tile)
        description: Return single-band PNG image of requested XYZ tile
        parameters:
            - in: path
              schema: KiyomasaQuerySchema
            - in: query
              schema: KiyomasaOptionSchema
        responses:
            200:
                description:
                    PNG image of requested tile
            400:
                description:
                    Invalid query parameters
            404:
                description:
                    No dataset found for given key combination
    """
    tile_xyz = (tile_x, tile_y, tile_z)
    return _get_kiyomasa_image(keys, tile_xyz)


class KiyomasaPreviewSchema(Schema):
    keys = fields.String(required=True, description='Keys identifying dataset, in order')


@TILE_API.route('/kiyomasa/<path:keys>/preview.png', methods=['GET'])
def get_kiyomasa_preview(keys: str) -> Response:
    """Return kiyomasa PNG preview image of requested dataset
    ---
    get:
        summary: /kiyomasa (preview)
        description: Return single-band PNG preview image of requested dataset
        parameters:
            - in: path
              schema: KiyomasaPreviewSchema
            - in: query
              schema: KiyomasaOptionSchema
        responses:
            200:
                description:
                    PNG image of requested tile
            400:
                description:
                    Invalid query parameters
            404:
                description:
                    No dataset found for given key combination
    """
    return _get_kiyomasa_image(keys)


def _get_kiyomasa_image(keys: str, tile_xyz: Tuple[int, int, int] = None) -> Response:
    parsed_keys = [key for key in keys.split('/') if key]

    option_schema = KiyomasaOptionSchema()
    options = option_schema.load(request.args)

    data_kind = parsed_keys[0]
    try:
        handler = getattr(handlers, data_kind)
        assert handler in (handlers.Pri60lv,
                           handlers.Pphw10,
                           handlers.Plts10,
                           handlers.CWM_height,
                           handlers.CWM_period,
                           handlers.CWM_direction)
        image = handler(parsed_keys, tile_xyz=tile_xyz, **options)
        return send_file(image, mimetype='image/png')
    except:
        return ('', 204)
