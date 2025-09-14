"""MeTTa NL Corpus - A labeling pipeline for Dutch language corpus."""

from importlib import metadata

try:
    __version__ = metadata.version("metta-nl-corpus")
except metadata.PackageNotFoundError:
    __version__ = "0.0.0"
