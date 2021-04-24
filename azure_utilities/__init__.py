import logging
import os
import shutil
ROOT_DIR = os.path.abspath(os.curdir)

# custom logger
logger = logging.getLogger(__name__)

# Create handlers
shutil.rmtree(f"{ROOT_DIR}/logs/azure_logs", ignore_errors=True)
os.makedirs(f"{ROOT_DIR}/logs/azure_logs/")
debug_handler = logging.FileHandler(f'{ROOT_DIR}/logs/azure_logs/azure_debug.log')
error_handler = logging.FileHandler(f'{ROOT_DIR}/logs/azure_logs/azure_error.log')
debug_handler.setLevel(logging.DEBUG)
error_handler.setLevel(logging.ERROR)


# Create formatters and add it to handlers
handler_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
error_handler.setFormatter(handler_format)
debug_handler.setFormatter(handler_format)

# Add handlers to the logger
logger.addHandler(error_handler)
logger.addHandler(debug_handler)

