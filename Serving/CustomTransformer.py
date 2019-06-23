import numpy as np
import pandas as pd

def datetime_transform(x):
    for c in x.columns.values:
        x.loc[:, c] = pd.to_datetime(x[c])
        x.loc[:, c] = x[c].dt.round('H').dt.hour
        x.loc[:, c+'_sin'] = np.sin(x[c]*(2.*np.pi/24))
        x.loc[:, c+'_cos'] = np.cos(x[c]*(2.*np.pi/24))
        x.drop([c], axis=1, inplace=True)
    return x.values

def log_transform(x, shift=1):
    return np.log(x + shift)