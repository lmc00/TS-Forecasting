# imports and useful variables
import sys
import os
import numpy as np
import pandas as pd
import dask as dd
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller, kpss, acf, grangercausalitytests
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf, month_plot, quarter_plot
from scipy import signal
import matplotlib.pyplot as plt
import seaborn as sns

from statsmodels.tsa.stattools import adfuller
import statsmodels.api as sm
from pylab import rcParams

from datetime import timedelta

# from statsmodels.tsa.forecasting.stl import STLForecast
import numpy as np
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX
import pickle

%matplotlib inline
pd.options.display.float_format = "{:.2f}".format
sns.set_style("whitegrid")
plt.rc("xtick", labelsize=15)
plt.rc("ytick", labelsize=15)

# Useful directory variables
src_path = os.getcwd()
root_path = os.path.dirname(src_path)
data_path = root_path + "/datasets"
visualization_path = root_path + "/data_visualization"
consumption_train = pd.read_parquet(
    data_path + "/05_model_input/" + "consumption_train"
)
pressure_train = pd.read_parquet(data_path + "/05_model_input/" + "pressure_train")
print(os.listdir(data_path + "/05_model_input"))
consumption_train.head(2)
# consumption_train.shape
# pressure_train.shape
# consumption_train[consumption_train.duplicated()]
# pressure_train[pressure_train.duplicated()]

print(pressure_train.n_working_compressors.value_counts())

consumption_train_pressure = consumption_train[
    ["time", "max_pressure_Bars", "n_working_compressors"]
].copy()
consumption_train_pressure.set_index("time", inplace=True)
consumption_train_pressure = consumption_train_pressure.resample("H").max().copy()
consumption_train_pressure.reset_index(inplace=True)
consumption_train = consumption_train[
    [
        "time",
        "total_average_power_consumption_kW_nodes",
        "total_average_power_consumption_kW_chillers",
        "Temperature_In_Degrees",
        "Temperature_Out_Degrees",
        "Temperature_Ambient_Degrees",
        "Temperature_Evaporator_Degrees",
    ]
].copy()

consumption_train.set_index("time", inplace=True)
consumption_train = consumption_train.resample("H").mean().copy()
consumption_train.reset_index(inplace=True)
consumption_train = pd.merge(consumption_train, consumption_train_pressure, on="time")

pressure_train_pressure = pressure_train[
    ["time", "max_pressure_Bars", "n_working_compressors"]
].copy()
pressure_train_pressure.set_index("time", inplace=True)
pressure_train_pressure = pressure_train_pressure.resample("H").max().copy()
pressure_train_pressure.reset_index(inplace=True)
pressure_train = pressure_train[
    [
        "time",
        "total_average_power_consumption_kW_nodes",
        "total_average_power_consumption_kW_chillers",
        "Temperature_In_Degrees",
        "Temperature_Out_Degrees",
        "Temperature_Ambient_Degrees",
        "Temperature_Evaporator_Degrees",
    ]
].copy()
pressure_train.set_index("time", inplace=True)
pressure_train = pressure_train.resample("H").mean().copy()
pressure_train.reset_index(inplace=True)
pressure_train = pd.merge(pressure_train, pressure_train_pressure, on="time")

pressure_train.to_parquet(data_path + "/05_model_input/" + "pressure_train_1hour")
consumption_train.to_parquet(data_path + "/05_model_input/" + "consumption_train_1hour")
