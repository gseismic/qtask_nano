from loguru import logger

logger.add("qtask_nano.log")

def get_logger(name: str):
    return logger.bind(name=name)
    
# 导出 logger 对象供其他模块使用
__all__ = ['logger', 'get_logger']