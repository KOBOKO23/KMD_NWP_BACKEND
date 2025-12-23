import os
from datetime import datetime
from wrf_data.utils.ssh_fetcher import fetch_wrf_data

# ===============================
# Test Config (Use environment variables for passwords)
# ===============================
TEST_CONFIG = {
    "WRF_JUMP_HOST": os.environ.get("WRF_JUMP_HOST", "196.202.217.197"),
    "WRF_JUMP_PORT": int(os.environ.get("WRF_JUMP_PORT", 10002)),
    "WRF_JUMP_USERNAME": os.environ.get("WRF_JUMP_USERNAME", "nwp1"),
    "WRF_JUMP_PASSWORD": os.environ.get("WRF_JUMP_PASSWORD"),  # MUST be set in env

    "WRF_TARGET_HOST": os.environ.get("WRF_TARGET_HOST", "192.168.1.201"),
    "WRF_TARGET_PORT": int(os.environ.get("WRF_TARGET_PORT", 22)),
    "WRF_TARGET_USERNAME": os.environ.get("WRF_TARGET_USERNAME", "nwp"),
    "WRF_TARGET_PASSWORD": os.environ.get("WRF_TARGET_PASSWORD"),  # MUST be set in env

    "WRF_REMOTE_GRIB_PATH": os.environ.get("WRF_REMOTE_GRIB_PATH", "/home/nwp/DA/SEVERE"),
    "LOCAL_DATA_PATH": os.environ.get("LOCAL_DATA_PATH", "./data/raw"),
    "FORECAST_HOURS": 2,  # For test, keep small
}

def test_fetch_wrf_files():
    """
    Test fetching WRF files from the target server via jump host.
    """
    print("=== Fetching d01 files ===")
    try:
        d01_files = fetch_wrf_data(datetime(2025, 12, 22, 0, 0), 'kenya', TEST_CONFIG)
        print(f"Downloaded {len(d01_files)} files:")
        for f in d01_files:
            print(f" - {f}")
    except Exception as e:
        print("❌ Test failed:", e)

if __name__ == "__main__":
    test_fetch_wrf_files()
