"""server/weather.py

Flask route to handle /weather calls.
"""

from typing import Any, Mapping, Dict, Tuple
import json

from marshmallow import (Schema, fields, validate, validates_schema,
                         pre_load, ValidationError, EXCLUDE)
from flask import request, send_file, Response

from terracotta.server.flask_api import TILE_API
from terracotta.cmaps import AVAILABLE_CMAPS

import terracotta.handlers.weather as handlers


class WeatherQuerySchema(Schema):
    keys = fields.String(required=True, description='Keys identifying dataset, in order')
    tile_z = fields.Int(required=True, description='Requested zoom level')
    tile_y = fields.Int(required=True, description='y coordinate')
    tile_x = fields.Int(required=True, description='x coordinate')


class WeatherOptionSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    tile_size = fields.List(
        fields.Integer(), validate=validate.Length(equal=2), example='[256,256]',
        description='Pixel dimensions of the returned PNG image as JSON list.'
    )


@TILE_API.route('/weather/<path:keys>/<int:tile_z>/<int:tile_x>/<int:tile_y>.png',
                methods=['GET'])
def get_weather(tile_z: int, tile_y: int, tile_x: int, keys: str) -> Response:
    """Return weather PNG image of requested tile
    ---
    get:
        summary: /weather (tile)
        description: Return single-band PNG image of requested XYZ tile
        parameters:
            - in: path
              schema: WeatherQuerySchema
            - in: query
              schema: WeatherOptionSchema
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
    return _get_weather_image(keys, tile_xyz)


class WeatherPreviewSchema(Schema):
    keys = fields.String(required=True, description='Keys identifying dataset, in order')


@TILE_API.route('/weather/<path:keys>/preview.png', methods=['GET'])
def get_weather_preview(keys: str) -> Response:
    """Return weather PNG preview image of requested dataset
    ---
    get:
        summary: /weather (preview)
        description: Return single-band PNG preview image of requested dataset
        parameters:
            - in: path
              schema: WeatherPreviewSchema
            - in: query
              schema: WeatherOptionSchema
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
    return _get_weather_image(keys)


def _get_weather_image(keys: str, tile_xyz: Tuple[int, int, int] = None) -> Response:
    parsed_keys = [key for key in keys.split('/') if key]

    option_schema = WeatherOptionSchema()
    options = option_schema.load(request.args)

    data_kind = parsed_keys[0]
    data_handler = None
    if data_kind == 'Pri60lv':
        data_handler = handlers.pri60lv
    elif data_kind == 'Pphw10':
        data_handler = handlers.pphw10
    elif data_kind == 'Plts10':
        data_handler = handlers.plts10
    
    if data_handler is not None: 
        image = data_handler(parsed_keys, tile_xyz=tile_xyz, **options)

    return send_file(image, mimetype='image/png')
