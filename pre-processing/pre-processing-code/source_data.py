import os
import boto3
import urllib.request
import json
import gzip

def source_dataset(new_filename, s3_bucket, new_s3_key):

    source_dataset_url = 'https://data.nextstrain.org/ncov'

    file_paths = [
        '_global.json',
        '_north-america.json',
        '_europe.json',
        '_asia.json',
        '_oceania.json',
        '_africa.json'
    ]

    for file_path in file_paths:

        urllib.request.urlretrieve(
            source_dataset_url + file_path, '/tmp/' + new_filename + file_path + '.gz')

        with gzip.open('/tmp/' + new_filename + file_path + '.gz', 'rb') as g, open('/tmp/' + new_filename + file_path, 'w', encoding='utf-8') as f:
            str_data = g.read().decode()
            dict_data = json.loads(str_data)
            f.write(json.dumps(dict_data))

    asset_list = []

    # Creates S3 connection
    s3 = boto3.client('s3')

    # Looping through filenames, uploading to S3
    for filename in os.listdir('/tmp'):
        
        if '.gz' not in filename:
            s3.upload_file('/tmp/' + filename, s3_bucket,
                           new_s3_key + filename)

            asset_list.append(
                {'Bucket': s3_bucket, 'Key': new_s3_key + filename})
    
    return asset_list