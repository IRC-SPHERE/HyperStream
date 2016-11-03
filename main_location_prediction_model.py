# The MIT License (MIT)
# Copyright (c) 2014-2017 University of Bristol
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
# OR OTHER DEALINGS IN THE SOFTWARE.

from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.model_selection import PredefinedSplit
import pandas as pd

data = [
]

feature_columns = []
label_column = ''
fold_column = ''

# Generate a data frame
df = pd.DataFrame([datum['x'].merge(datum['y']) for datum in data])

# Fill empty values, and drop any rows for which the label/fold index is NaN
df[feature_columns].fillna(-120)
df.isnull().any(axis=1)
df.dropna(subset=[label_column, fold_column], inplace=True)

# Use pre-defined folds for model selection
folds = PredefinedSplit(df[fold_column])
for train_inds, test_inds in folds.split():
    train_x = df[feature_columns].iloc[train_inds]
    test_x = df[feature_columns].iloc[test_inds]
    train_y = df[label_column].iloc[train_inds]
    test_y = df[label_column].iloc[test_inds]
    
    clf = LinearDiscriminantAnalysis()
    clf.fit(train_x.data, train_y.data)
