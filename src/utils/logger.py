"""
Structured Enterprise Logging Utility for Formula 1 Lakehouse Pipeline.
Provides standardized JSON/console logging with timestamps, pipeline stages, and severity levels.
"""

import logging
import sys
import os

def get_logger(name: str = "F1Lakehouse") -> logging.Logger:
    """
    Creates and returns a configured logger instance.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Console handler with standardized format
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        
    return logger
