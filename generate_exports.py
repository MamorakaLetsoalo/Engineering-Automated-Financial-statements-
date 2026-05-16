import os
import sys
sys.path.insert(0, os.getcwd())
from streamlit_app.app import _generate_synthetic

hist, fcast, scenarios, monthly = _generate_synthetic()
export_dir = os.path.join(os.getcwd(), "exports")
os.makedirs(export_dir, exist_ok=True)
for name, df in [
    ("historical_annual", hist),
    ("annual_forecast", fcast),
    ("scenarios", scenarios),
    ("monthly_forecast", monthly),
]:
    path = os.path.join(export_dir, f"{name}.csv")
    df.to_csv(path, index=False)
    print("wrote", path, "rows=", len(df))
