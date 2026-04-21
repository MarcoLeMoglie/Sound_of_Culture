import pandas as pd
from pandas.io.stata import StataReader

dta_path = "/Users/marcolemoglie_1_2/Library/CloudStorage/Dropbox/Sound_of_Culture/data/phase_01_dataset_construction/processed/country_artists/Sound_of_Culture_Country_Restricted_Final_v6.dta"

try:
    with StataReader(dta_path) as reader:
        labels = reader.variable_labels()
        
    print(f"Total columns matched with labels: {len(labels)}")
    print("\nSample Labels:")
    for i, (col, label) in enumerate(list(labels.items())[:15]):
        print(f" - {col}: {label}")
        
    # Check specific critical ones
    critical = ["complexity", "release_year", "name_primary", "energy"]
    print("\nCritical Fields Check:")
    for c in critical:
        print(f" - {c}: {labels.get(c, 'MISSING')}")

except Exception as e:
    print(f"Error checking labels: {e}")
