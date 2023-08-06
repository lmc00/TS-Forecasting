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

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName('Training Sarimax') \
        .getOrCreate()
    sc = spark.sparkContext
    # Useful directory variables
    src_path = os.getcwd()
    root_path = os.path.dirname(src_path)
    data_path = root_path + "/datasets"
    visualization_path = root_path + "/data_visualization"
    consumption_train = pd.read_parquet(data_path+"/05_model_input/"+"consumption_train")
    pressure_train = pd.read_parquet(data_path+"/05_model_input/"+"pressure_train")
    #consumption_train[:-48*7]#Cantidad para entrenar

    sarimax_model = SARIMAX(consumption_train[:-48*7].total_average_power_consumption_kW_chillers, order = (48, 0, 3), trend="c").fit()
    with open(data_path+"/06_models/sarimax_consumption_48_0_3.pickle", "wb") as f: #For some reason, models in statsmodels.tsa.statespace have pending the implementation of .load()
        pickle.dump(sarimax_model, f)
    spark.stop()