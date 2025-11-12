import os
import sys


def _setup_environment():
    """Setup required environment variables and paths"""
    if getattr(sys, 'frozen', False):
        # Running as compiled exe
        base_path = sys._MEIPASS
    else:
        # Running as script
        base_path = os.path.dirname(os.path.abspath(__file__))
    
    # Set environment variables
    os.environ['PLAYWRIGHT_BROWSERS_PATH'] = os.path.join(base_path, 'ms-playwright')

_setup_environment()


