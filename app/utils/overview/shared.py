"""Shared overview constants."""

from decouple import config as env

USD_TO_IDR_RATE = env("USD_TO_IDR_RATE", default=16000.0, cast=float)
