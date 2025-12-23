# inspect_grib.py
from wrf_data.utils.grib_processor import GRIBProcessor

# Use one of the fetched files
grib_file = './data/raw/20251222/WRFPRS_d01.00'

with GRIBProcessor(grib_file) as processor:
    messages = processor.list_available_messages()

# Print all messages neatly
for msg in messages:
    print(f"Index: {msg['index']}, Name: {msg['name']}, ShortName: {msg['shortName']}, "
          f"Level: {msg['level']}, TypeOfLevel: {msg['typeOfLevel']}, Units: {msg['units']}")
