from flask import Flask,request,jsonify
from dotenv import load_dotenv
import os
import pymupdf
import requests
from PIL import Image
from io import BytesIO
from flask_pymongo import PyMongo
from azure.storage.blob import BlobServiceClient
load_dotenv()


app=Flask(__name__)

app.config["MONGO_URI"]=os.getenv("MONGO_URI")
mongo=PyMongo(app)

AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
CONTAINER_NAME = "pdf-processing"
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)

@app.route('/')
def home():
    return "yes"

@app.route('/readfile',methods=['POST'])
def readfile():
    try:
        pdf_url = request.json.get('pdf_url')
        uuid = request.json.get('uuid')
        r = requests.get(pdf_url)
    
    # Check if the request was successful
        if r.status_code != 200:
            return jsonify({"error": f"Failed to fetch PDF from the URL, status code {r.status_code}"}), 400
        
        # Check if the content is a valid PDF
        if 'application/pdf' not in r.headers.get('Content-Type', ''):
            return jsonify({"error": "The fetched content is not a valid PDF file."}), 400

        pdf=mongo.db.pdfProcessed.find_one({"uuid": uuid})
        if pdf!=None:
            return jsonify({"error":"uuid already exists"}),400
        
        data = r.content
        doc = pymupdf.Document(stream=data)
        image_urls = []
    #print(type(doc))
        for page_number, page in enumerate(doc):
            pix = page.get_pixmap()
            img_stream=BytesIO()
            img_stream_2=BytesIO(pix.tobytes())
            #print("png size: ",len(img_stream_2.getvalue())/1024,"kb")

            img = Image.open(img_stream_2)
            img = img.convert("RGB")
            img.save(img_stream, "JPEG", optimize=True, quality=80)


            blob_name = f"{uuid}_page_{page_number + 1}.jpg"
            blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
            img_stream.seek(0)
            blob_client.upload_blob(img_stream, overwrite=True)

            # Get the image URL and add to the list
            image_url = blob_client.url
            image_urls.append(image_url)

            # print("jpeg size: ",len(img_stream.getvalue())/1024,"kb")
        mongo.db.pdfProcessed.insert_one({"uuid": uuid, "image_urls": image_urls})
        return jsonify({"message": "File processed successfully."})
    except Exception as e:
        return jsonify({"err_msg":str(e)}),500

if __name__=='__main__':
    app.run(debug=True)