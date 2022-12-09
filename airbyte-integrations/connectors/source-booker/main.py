#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_booker import SourceBooker

if __name__ == "__main__":
    source = SourceBooker()
    launch(source, sys.argv[1:])
