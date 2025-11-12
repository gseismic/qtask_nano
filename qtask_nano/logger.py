# import sys
from loguru import logger as _logger

logger = _logger.bind(name='qtask_nano')
logger.add("qtask_nano.log")
# logger.add(sys.stdout, level="INFO")
    
__all__ = ['logger']
