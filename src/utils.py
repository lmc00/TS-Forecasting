import numpy as np
import pandas as pd
from sklearn.metrics import mean_squared_error


def smape(
    y_true: pd.core.series.Series, y_pred: pd.core.series.Series
) -> float:
    """Given the ground truth time series set and the forecasted one, it computes and returns the
    Symmetric mean absolute percentage error.
    Multiplied by 100 to get iot in the percentage form. In Spanish Error porcentual de la media absoluta simétrica
    Implementation based on https://towardsdatascience.com/choosing-the-correct-error-metric-mape-vs-smape-5328dec53fac
    https://typethepipe.com/post/symmetric-mape-is-not-symmetric/
    Args:
        y_true (pd.core.series.Series): Ground Truth array (n_predictions,)containing the Time Series test set
        y_pred (np.arraypd.core.series.Series): Predictions array (n_predictions,) containing the forecasted values

    Returns:
        float: calculated SMAPE
    """
    return 100 * np.mean(
        2 * np.abs(y_pred - y_true) / (np.abs(y_pred) + np.abs(y_true))
    )


def rmse(y_true, y_pred) -> float:
    """Returns Root Meean Squared Error RMSE

    Args:
        y_true (pd.core.series.Series): Ground Truth array (n_predictions,)containing the Time Series test set
        y_pred (np.arraypd.core.series.Series): Predictions array (n_predictions,) containing the forecasted values


    Returns:
        float: the RMSE
    """
    return np.sqrt(mean_squared_error(y_true, y_pred))
