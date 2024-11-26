import os

from flask import Flask, request, jsonify, Response

from scraper_xm_api import Scraper

app = Flask(__name__)


@app.errorhandler(ValueError)
def handle_value_error(error):
    return jsonify({
        "message": str(error)
    }), 403

@app.errorhandler(Exception)
def handle_generic_error(error):
    return jsonify({
        "message": f"Unexpected error: {str(error)}"
    }), 500

@app.route("/health", methods=["GET"])
def health():
    data = {
            "message":"UP"
    }
    return jsonify(data), 200

@app.route("/xm-data", methods=["POST"])
def active_list():
    content = request.get_json(silent=False)
    if ({'date','bucket'} <= set(content)):
        try:   
            date = content['date']
            bucket = content['bucket']

            xm = Scraper(date)
            result = xm.extract_data(bucket)

            return jsonify({
                            "message": f"Successfully extracted all data for date: {date}",
                            "data": result
                        }), 200

        except ValueError as e:
            raise e
        except Exception as e:
            raise e
    else:
        return jsonify({"message": "Missing parameters in request"}), 400


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))