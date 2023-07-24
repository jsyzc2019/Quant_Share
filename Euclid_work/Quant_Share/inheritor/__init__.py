
from .chronos import *
from .consts import *
from .utils import *

Config.stock_list = list(map(Formatter.stock, Config.stock_list))