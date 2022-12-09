#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_lightspeed import SourceLightspeed

if __name__ == "__main__":
    source = SourceLightspeed()
    launch(source, sys.argv[1:])
