import os
import json
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.data.tables import TableServiceClient, TableEntity
from PIL import Image
import io
import logging
import azure.functions as func

# Configuration des clients Azure
connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
table_service_client = TableServiceClient.from_connection_string(connection_string)

# Nom des conteneurs et de la table
src_container_name = 'src'
dest_container_name = 'dest'
table_name = 'image-logs'


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Fonction déclenchée par un appel HTTP pour redimensionner une image et la stocker.
    """

    # Récupération des paramètres du corps de la requête
    req_body = req.get_json()
    blob_name = req_body.get('blob_name')
    resize_width = req_body.get('width')
    resize_height = req_body.get('height')

    if not blob_name or not resize_width or not resize_height:
        return func.HttpResponse(
            "Bad request. 'blob_name', 'width' and 'height' are required.",
            status_code=400
        )

    # Récupérer l'image depuis le Blob
    blob_client = blob_service_client.get_blob_client(container=src_container_name, blob=blob_name)
    
    try:
        blob_data = blob_client.download_blob()
        image_data = io.BytesIO(blob_data.readall())
        image = Image.open(image_data)

        # Redimensionner l'image
        image = image.resize((resize_width, resize_height))

        # Enregistrer l'image redimensionnée dans un fichier temporaire
        output_image_data = io.BytesIO()
        image.save(output_image_data, format="PNG")
        output_image_data.seek(0)

        # Stocker l'image redimensionnée dans le conteneur 'dest'
        dest_blob_name = f"resized_{resize_width}x{resize_height}_{blob_name}"
        dest_blob_client = blob_service_client.get_blob_client(container=dest_container_name, blob=dest_blob_name)
        dest_blob_client.upload_blob(output_image_data, overwrite=True)

        # Ajouter une entrée dans la table 'image-logs'
        table_client = table_service_client.get_table_client(table_name)
        entity = TableEntity(partition_key="imagePartition", row_key=dest_blob_name)
        entity['processed'] = True
        entity['resize_width'] = resize_width
        entity['resize_height'] = resize_height
        entity['blob_url'] = f"https://{blob_service_client.account_name}.blob.core.windows.net/{dest_container_name}/{dest_blob_name}"

        table_client.create_entity(entity=entity)

        # Retourner une réponse HTTP avec l'URL du blob redimensionné
        return func.HttpResponse(
            json.dumps({"message": "Image resized successfully!", "blob_url": entity['blob_url']}),
            mimetype="application/json",
            status_code=200
        )
    
    except Exception as e:
        logging.error(f"Error processing the image: {str(e)}")
        return func.HttpResponse(
            f"Error processing the image: {str(e)}",
            status_code=500
        )
