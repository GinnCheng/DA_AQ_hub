from flask import Flask, request, render_template
from data_fetcher import qld_amns_data_fetcher
import pandas as pd  # Assuming your class fetches data into a DataFrame

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def home():
    data = None
    if request.method == 'POST':
        location = request.form['location']
        fetcher = qld_amns_data_fetcher()
        data = fetcher.fetch_data(location)
    return render_template('index.html', data=data)

if __name__ == '__main__':
    app.run(debug=True)
