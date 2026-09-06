import os
import sys

# Add project root to sys.path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from src.train import train_model, MODEL_PATH

if __name__ == "__main__":
    local_model_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "xgb_eta_model.joblib")
    train_model(save_path=local_model_path)
    # Also save to root if different
    if os.path.abspath(local_model_path) != os.path.abspath(MODEL_PATH):
        import shutil
        shutil.copy(local_model_path, MODEL_PATH)
