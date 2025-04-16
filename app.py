from flask import Flask, render_template, request, jsonify
from ingestion import ingest_clickhouse_to_flatfile, ingest_flatfile_to_clickhouse

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')  # Serve the HTML frontend

@app.route('/ingest', methods=['POST'])
def ingest_data():
    data = request.get_json()  # Get data from the frontend
    source = data['source']
    target = data['target']
    
    # Simulate data ingestion process based on the user's selection
    if source == 'ClickHouse' and target == 'FlatFile':
        # Simulate ClickHouse to FlatFile ingestion
        ingest_clickhouse_to_flatfile(data['columns'], 'output.csv')  # Add necessary params
        return jsonify({'message': 'Data ingested from ClickHouse to FlatFile.'})
    
    elif source == 'FlatFile' and target == 'ClickHouse':
        # Simulate FlatFile to ClickHouse ingestion
        ingest_flatfile_to_clickhouse('input.csv', 'target_table')  # Add necessary params
        return jsonify({'message': 'Data ingested from FlatFile to ClickHouse.'})
    
    else:
        return jsonify({'message': 'Invalid source/target selection.'})

if __name__ == '__main__':
    app.run(debug=True)
