# Mobile Apps Online Ads Attribution Prediction
This project intends to show end to end machine learning model development workflow by solving the challenging online ads attribution prediction problem. 

Dataset: Kaggle “TalkingData AdTracking Fraud Detection” Competition Dataset 

ML Problem Framing: Binary Classification, the problem is further framed as online prediction problem. Framing this problem as online prediction problem increases the level of challenge in feature engineering because we should be only allowed to generate the features related to culmulative sum group by keys based on sliding windows rather than the sum group by keys of whole dataset (refer to the notebook and big query code for more details). In the original Kaggle competition this problem can be framed as batch prediction. In my opinion, online prediction make more sense on how this application can help the business to solve the ads attribution prediction problem in real time.         

The Challenges: 
- Large data size (more than 180 million records) 
- Highly Imbalance dataset 
- Data is in raw log format; creative feature engineering is required to generate useful features 

Development Tasks Performed / Achievements Highlights: 
- Followed data science/machine learning model development workflow – perform exploratory data analysis, feature engineering, model development and training, hyperparameter tuning, validation and evaluation 
- Generated new features (various moving cumulative sums group by keys) with BigQuery  
- Categorical features encoding with Cat2Vec – an idea inspired by Word2Vec encoding in Natural Language Processing 
- Assembled all pre-processing and classification steps in Scikit-Learn Pipeline 
- Hyperparameter tuning with automated Bayesian approach using Hyperopt package 
- Implemented XGBoost model in Spark ML using Scala for production scale distributed training and hyperparameter tuning
- Developed Rest API using Flask for serving

Main Files/Folders:
- talkingdata-eda-and-feature-engineering.ipynb - EDA and feature engineering
- talkingdata-model-development.ipynb - model development and local training / evaluation
- feature-engineering.sql - implemented features generation using Big Query
- SparkML - Model implementation in Spark ML codes using Scala for distributed training and hyperparameter tuning
- Serving - Rest API implemented in Flask to serve the model prediction 
