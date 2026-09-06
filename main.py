import os
import sys

# Ensure submodules can be imported cleanly
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from layer3_ml_forecaster.server import app
