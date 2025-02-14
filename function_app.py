import os
import json
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.data.tables import TableServiceClient, TableEntity
from PIL import Image
import io
import logging
import azure.functions as func

connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
table_service_client = TableServiceClient.from_connection_string(connection_string)

src_container_name = 'src'
dest_container_name = 'dest'
table_name = 'image-logs'


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Fonction déclenchée par un appel HTTP pour couper une image en 2 parties et les stocker.
    """

    req_body = req.get_json()
    blob_name = req_body.get('blob_name')

    if not blob_name:
        return func.HttpResponse(
            "Bad request. 'blob_name' is required.",
            status_code=400
        )

    blob_client = blob_service_client.get_blob_client(container=src_container_name, blob=blob_name)
    
    try:
        blob_data = blob_client.download_blob()
        image_data = io.BytesIO(blob_data.readall())
        image = Image.open(image_data)

        width, height = image.size
        half_height = height // 2
        
        top_image = image.crop((0, 0, width, half_height))
        bottom_image = image.crop((0, half_height, width, height))

        top_output_image_data = io.BytesIO()
        top_image.save(top_output_image_data, format="PNG")
        top_output_image_data.seek(0)

        bottom_output_image_data = io.BytesIO()
        bottom_image.save(bottom_output_image_data, format="PNG")
        bottom_output_image_data.seek(0)

        top_blob_name = f"top_{blob_name}"
        bottom_blob_name = f"bottom_{blob_name}"

        top_blob_client = blob_service_client.get_blob_client(container=dest_container_name, blob=top_blob_name)
        top_blob_client.upload_blob(top_output_image_data, overwrite=True)

        bottom_blob_client = blob_service_client.get_blob_client(container=dest_container_name, blob=bottom_blob_name)
        bottom_blob_client.upload_blob(bottom_output_image_data, overwrite=True)

        table_client = table_service_client.get_table_client(table_name)
        entity = TableEntity(partition_key="imagePartition", row_key=blob_name)
        entity['processed'] = True
        entity['top_blob_url'] = f"https://{blob_service_client.account_name}.blob.core.windows.net/{dest_container_name}/{top_blob_name}"
        entity['bottom_blob_url'] = f"https://{blob_service_client.account_name}.blob.core.windows.net/{dest_container_name}/{bottom_blob_name}"

        table_client.create_entity(entity=entity)

        return func.HttpResponse(
            json.dumps({"message": "Image cut successfully!", "top_blob_url": entity['top_blob_url'], "bottom_blob_url": entity['bottom_blob_url']}),
            mimetype="application/json",
            status_code=200
        )
    
    except Exception as e:
        logging.error(f"Error processing the image: {str(e)}")
        return func.HttpResponse(
            f"Error processing the image: {str(e)}",
            status_code=500
        )
