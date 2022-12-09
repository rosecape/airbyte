#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_lightspeed_resto import SourceLightspeedResto

if __name__ == "__main__":
    source = SourceLightspeedResto()
    launch(source, sys.argv[1:])
