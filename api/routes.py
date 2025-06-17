from flask import Blueprint, jsonify, request
from datetime import datetime
from services.cassandra_service import cassandra_service

api = Blueprint('api', __name__)

@api.route('/assets', methods=['GET'])
def get_assets():
    query = "SELECT id, name FROM asset"
    rows = cassandra_service.execute_query(query)
    return jsonify([{"id": row.id, "name": row.name} for row in rows])

@api.route('/assets', methods=['POST'])
def add_asset():
    """Adaugă un nou asset"""
    data = request.json
    query = """
    INSERT INTO asset (id, name, system_time)
    VALUES (%s, %s, %s)
    """
    params = (data['id'], data['name'], datetime.now())
    cassandra_service.execute_query(query, params)
    return jsonify({"status": "success"}), 201

@api.route('/data/<asset_id>', methods=['GET'])
def get_asset_data(asset_id):
    """Returnează datele pentru un asset"""
    query = """
    SELECT * FROM time_series_data
    WHERE asset_id = %s LIMIT 100
    """
    rows = cassandra_service.execute_query(query, (asset_id,))
    return jsonify([dict(row) for row in rows])