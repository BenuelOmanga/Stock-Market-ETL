import streamlit as st
import pickle
import numpy as np

with open("ML_Model/ridge_model.pkl", "rb") as file:
    model = pickle.load(file)

st.title("Stock Price Predictor (Ridge Regression)")
st.markdown("Enter feature values to predict the stock's **closing price**.")

features = [
    "Open", "High", "Low", "Volume",
    "% Change", "SMA_30", "SMA_200",
    "RSI", "Volume_MA_20", "Volatility_30"
]

inputs = []
for feature in features:
    value = st.number_input(f"{feature}", format="%.4f")
    inputs.append(value)

if st.button("Predict"):
    input_array = np.array(inputs).reshape(1, -1)
    prediction = model.predict(input_array)[0]
    st.success(f"Predicted Closing Price: **${prediction:.2f}**")
