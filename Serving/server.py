from flask import Flask, request, jsonify
from werkzeug.exceptions import BadRequest
import sklearn
from gensim.models import Word2Vec
import numpy as np
from pandas.io.json import json_normalize
import copy
import pandas as pd

cat_feature_size = 5
categories = ['ip','app','device','os','channel']


def load_model(filename):
    return sklearn.externals.joblib.load(filename)

def convert_to_cat(df):
    data = copy.deepcopy(df)
    data.reset_index(drop=True, inplace=True)
    for c in list(data.columns.values):
        data[c] = data[c].astype('category')
        data[c].cat.categories = ["%s %s" % (c,g) for g in data[c].cat.categories]
    return data

def transform(X, model, vocab, feature_size, categories):
    X_cat = X[categories]
    all_cat_vec = np.zeros((X_cat.shape[0], X_cat.shape[1] * feature_size), dtype="float64")

    X_cat = convert_to_cat(X_cat)
    for index, row in X_cat.iterrows():
        for catIndex, catItem in enumerate(row):
            if catItem in vocab:
                startIndex = catIndex * feature_size
                endIndex = (catIndex + 1) * feature_size
                all_cat_vec[index, startIndex: endIndex] = model[catItem]


    cat_feature_df = pd.DataFrame(data=all_cat_vec,  # values
                                  columns=["cat_vec_%s" % i for i in range(25)])
    X.drop(columns=categories, inplace=True)
    X = pd.concat([cat_feature_df, X], axis=1)
    return X

app = Flask(__name__)
classifier_model = load_model("./Model/TalkingDataModel.model")
cat2vec_model = Word2Vec.load("./Model/cat2vec.model")
vocab = set(cat2vec_model.wv.index2word)

@app.errorhandler(BadRequest)
def handle_bad_request(e):
    return jsonify({"Request Fail": str(e)}), 400


@app.route('/talkingdata-api', methods=['POST'])
def predict():
    try:
        data = request.get_json(force=True)
        data = data["attrPredictRequest"]
        df = json_normalize(data)
        features = transform(df, cat2vec_model, vocab, cat_feature_size, categories)
        prediction = classifier_model.predict(features)
        result = {"attrPredictResponse": prediction.tolist()}
    except (KeyError, ValueError) as e:
        raise BadRequest(str(e))

    return jsonify(result)


if __name__ == '__main__':
    app.run(port=5000, debug=True)