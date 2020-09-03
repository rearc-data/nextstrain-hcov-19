import os
import boto3
import json
import gzip
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
from multiprocessing.dummy import Pool
from s3_md5_compare import md5_compare

def data_to_s3(endpoint):

	# throws error occured if there was a problem accessing data
	# otherwise downloads and uploads to s3

	source_dataset_url = 'https://data.nextstrain.org/ncov'

	try:
		response = urlopen(source_dataset_url + endpoint)

	except HTTPError as e:
		raise Exception('HTTPError: ', e.code, endpoint)

	except URLError as e:
		raise Exception('URLError: ', e.reason, endpoint)

	else:
		data_set_name = os.environ['DATA_SET_NAME']
		filename = data_set_name + endpoint
		new_s3_key = data_set_name + '/dataset/' + filename
		file_location = '/tmp/' + filename

		with open(file_location + '.gz', 'wb') as f:
			f.write(response.read())

		with gzip.open(file_location + '.gz', 'rb') as g, open(file_location, 'w', encoding='utf-8') as f:
			str_data = g.read().decode()
			dict_data = json.loads(str_data)
			f.write(json.dumps(dict_data))
		
		os.remove(file_location + '.gz')

		# variables/resources used to upload to s3
		s3_bucket = os.environ['S3_BUCKET']
		s3 = boto3.client('s3')

		has_changes = md5_compare(s3, s3_bucket, new_s3_key, file_location)
		if has_changes:
			s3.upload_file(file_location, s3_bucket, new_s3_key)
			print('Uploaded: ' + filename)

		# deletes to preserve limited space in aws lamdba
		os.remove(file_location)

		# dicts to be used to add assets to the dataset revision
		asset_source = {'Bucket': s3_bucket, 'Key': new_s3_key}
		return {'has_changes': has_changes, 'asset_source': asset_source}

def source_dataset():

	# list of enpoints to be used to access data included with product
	endpoints = [
		'_global.json',
		'_north-america.json',
		'_europe.json',
		'_asia.json',
		'_oceania.json',
		'_africa.json'
	]
	asset_list = []

	# multithreading speed up accessing data, making lambda run quicker
	with (Pool(6)) as p:
		s3_uploads = p.map(data_to_s3, endpoints)

	# If any of the data has changed, we need to republish the adx product
	count_updated_data = sum(
		upload['has_changes'] == True for upload in s3_uploads)
	if count_updated_data > 0:
		asset_list = list(
			map(lambda upload: upload['asset_source'], s3_uploads))
		if len(asset_list) == 0:
			raise Exception('Something went wrong when uploading files to s3')
	# asset_list is returned to be used in lamdba_handler function
	return asset_list
