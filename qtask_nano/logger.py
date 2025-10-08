# import sys
from loguru import logger as _logger

logger = _logger.bind(name='qtask_nano')
logger.add("qtask_nano.log")
# logger.add(sys.stdout, level="INFO")
    
# 导出 logger 对象供其他模块使用
__all__ = ['logger']
