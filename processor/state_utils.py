import os

import pandas as pd
import logging as log

from typing import List, Any, Dict

from general_utils import clean_string_for_ddl_naming
from processor_state import State, StateDataColumnDefinition, StateConfigLM, StateConfig, \
    implicit_count_with_force_count

from datetime import datetime as dt

logging = log.getLogger(__name__)
